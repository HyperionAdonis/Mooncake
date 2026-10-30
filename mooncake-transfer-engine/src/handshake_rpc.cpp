// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "handshake_rpc.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <limits>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <json/json.h>
#include <ylt/coro_io/coro_io.hpp>
#include <ylt/coro_io/server_acceptor.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/coro_rpc/impl/protocol/coro_rpc_protocol.hpp>
#include <ylt/easylog.hpp>

#include <cstdio>
#include <future>

#include "common.h"
#include "config.h"
#include "error.h"
#include "transfer_metadata_plugin.h"

namespace mooncake {
namespace {

constexpr int kConnection = 0;
constexpr int kMetadata = 1;
constexpr int kNotify = 2;
constexpr int kProbe = 3;

// Serialized func_id rides in the RPC body, next to the JSON attachment.
// The attachment itself cannot exceed MC_HANDSHAKE_MAX_LENGTH. The sum may
// exceed that value by this many bytes so a full-size JSON still fits, and
// a body of the limit plus an attachment of the limit still does not.
constexpr uint64_t kRpcEnvelopeBytes = 64;

constexpr uint64_t kDefaultInflightBytes = 512ull << 20;
constexpr int kDefaultConcurrency = 256;
constexpr int kDefaultThreads = 2;
constexpr int kDefaultPools = 1024;
constexpr int kDefaultIdlePerPeer = 2;
constexpr int kDefaultTimeoutSeconds = 60;

int ParsePositiveInt(const char *name, int fallback, int min_value,
                     int max_value) {
    const char *text = std::getenv(name);
    if (text == nullptr || text[0] == '\0') return fallback;
    char *end = nullptr;
    long value = std::strtol(text, &end, 10);
    if (end == text || *end != '\0' || value < min_value || value > max_value) {
        LOG(WARNING) << "Ignoring invalid " << name << "=\"" << text << "\"";
        return fallback;
    }
    return static_cast<int>(value);
}

uint64_t ParseUint64(const char *name, uint64_t fallback) {
    const char *text = std::getenv(name);
    if (text == nullptr || text[0] == '\0') return fallback;
    char *end = nullptr;
    unsigned long long value = std::strtoull(text, &end, 10);
    if (end == text || *end != '\0' || value == 0) {
        LOG(WARNING) << "Ignoring invalid " << name << "=\"" << text << "\"";
        return fallback;
    }
    return static_cast<uint64_t>(value);
}

bool ParseJson(std::string_view text, Json::Value &value, std::string &error) {
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    return reader->parse(text.data(), text.data() + text.size(), &value,
                         &error);
}

std::string WriteJson(const Json::Value &value) {
    return Json::FastWriter{}.write(value);
}

enum class PayloadDecision { kOk, kTooLarge, kOverBudget };

class HandshakeAdmission {
   public:
    static HandshakeAdmission &Get() {
        static HandshakeAdmission admission;
        return admission;
    }

    void Configure(uint64_t max_bytes, uint64_t max_frame) {
        std::lock_guard<std::mutex> lock(mu_);
        max_bytes_ = max_bytes;
        max_frame_ = max_frame;
    }

    PayloadDecision Reserve(uint64_t body, uint64_t attach) {
        const uint64_t sum = body + attach;
        std::lock_guard<std::mutex> lock(mu_);
        if (body > max_frame_ || attach > max_frame_ ||
            sum > max_frame_ + kRpcEnvelopeBytes) {
            return PayloadDecision::kTooLarge;
        }
        if (bytes_ + sum > max_bytes_) return PayloadDecision::kOverBudget;
        bytes_ += sum;
        unbound_ += sum;
        return PayloadDecision::kOk;
    }

    void ReleaseUnbound(uint64_t n) {
        std::lock_guard<std::mutex> lock(mu_);
        if (unbound_ >= n) unbound_ -= n;
        else unbound_ = 0;
        if (bytes_ >= n) bytes_ -= n;
        else bytes_ = 0;
    }

    void Bind(const void *key, uint64_t n) {
        std::lock_guard<std::mutex> lock(mu_);
        if (unbound_ >= n) unbound_ -= n;
        else unbound_ = 0;
        if (key != nullptr) {
            keyed_[key] += n;
        } else {
            anonymous_.push_back(n);
        }
    }

    uint64_t ReleaseKey(const void *key) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = keyed_.find(key);
        if (it == keyed_.end()) return 0;
        const uint64_t n = it->second;
        keyed_.erase(it);
        if (bytes_ >= n) bytes_ -= n;
        else bytes_ = 0;
        return n;
    }

    void ReleaseAnonymous() {
        std::lock_guard<std::mutex> lock(mu_);
        if (anonymous_.empty()) return;
        const uint64_t n = anonymous_.back();
        anonymous_.pop_back();
        if (bytes_ >= n) bytes_ -= n;
        else bytes_ = 0;
    }

   private:
    std::mutex mu_;
    uint64_t max_bytes_ = kDefaultInflightBytes;
    uint64_t max_frame_ = 1ull << 20;
    uint64_t bytes_ = 0;
    uint64_t unbound_ = 0;
    std::unordered_map<const void *, uint64_t> keyed_;
    std::vector<uint64_t> anonymous_;
};

struct HandshakeRpcProtocol : coro_rpc::protocol::coro_rpc_protocol {
    using router = coro_rpc::protocol::router<HandshakeRpcProtocol>;

    template <typename Socket>
    static async_simple::coro::Lazy<std::error_code> read_payload(
        Socket &socket, req_header &req_head, std::string &buffer,
        coro_io::heterogeneous_buffer &attachment) {
        const uint64_t body = req_head.length;
        const uint64_t attach = req_head.attach_length;
        const PayloadDecision decision =
            HandshakeAdmission::Get().Reserve(body, attach);
        if (decision == PayloadDecision::kTooLarge) {
            co_return std::make_error_code(std::errc::message_size);
        }
        if (decision == PayloadDecision::kOverBudget) {
            co_return std::make_error_code(std::errc::not_enough_memory);
        }
        auto ec = co_await coro_rpc_protocol::read_payload(socket, req_head,
                                                           buffer, attachment);
        if (ec) {
            HandshakeAdmission::Get().ReleaseUnbound(body + attach);
            co_return ec;
        }
        const void *key = nullptr;
        if (attach > 0) {
            if (std::string *stored = attachment.get_string()) {
                key = stored->data();
            }
        }
        HandshakeAdmission::Get().Bind(key, body + attach);
        co_return std::error_code{};
    }
};

struct HandshakeServerConfig : coro_rpc::config_t {
    using rpc_protocol = HandshakeRpcProtocol;
};

using HandshakeServer = coro_rpc::coro_rpc_server_base<HandshakeServerConfig>;

class BoundFdAcceptor : public coro_io::server_acceptor_base {
   public:
    BoundFdAcceptor(int fd, std::string address, uint16_t port)
        : server_acceptor_base(std::move(address), port), fd_(fd) {}

    ~BoundFdAcceptor() override = default;

    bool consumed() const { return consumed_; }

    coro_io::listen_errc listen() override {
        if (pool_ == nullptr || fd_ < 0) return coro_io::listen_errc::open_error;
        executor_ = pool_->get_executor();
        acceptor_.emplace(executor_->get_asio_executor());
        sockaddr_storage storage{};
        socklen_t length = sizeof(storage);
        if (getsockname(fd_, reinterpret_cast<sockaddr *>(&storage), &length) !=
            0) {
            return coro_io::listen_errc::bad_address;
        }
        const auto protocol = storage.ss_family == AF_INET6
                                  ? asio::ip::tcp::v6()
                                  : asio::ip::tcp::v4();
        asio::error_code ec;
        acceptor_->assign(protocol, fd_, ec);
        if (ec) return coro_io::listen_errc::listen_error;
        consumed_ = true;
        fd_ = -1;
        acceptor_->listen(asio::socket_base::max_listen_connections, ec);
        if (ec) return coro_io::listen_errc::listen_error;
        const auto endpoint = acceptor_->local_endpoint(ec);
        if (ec) return coro_io::listen_errc::listen_error;
        port_ = endpoint.port();
        return coro_io::listen_errc::ok;
    }

    async_simple::coro::Lazy<
        ylt::expected<coro_io::socket_wrapper_t, std::error_code>>
    accept() override {
        accept_started_.store(true, std::memory_order_release);
        auto socket_executor = pool_->get_executor();
        asio::ip::tcp::socket socket(socket_executor->get_asio_executor());
        co_await coro_io::dispatch(executor_->get_asio_executor());
        auto error = co_await coro_io::async_accept(*acceptor_, socket);
        if (error) {
            if (error == asio::error::operation_aborted ||
                error == asio::error::bad_descriptor) {
                acceptor_close_waiter_.set_value();
            }
            co_return ylt::unexpected<std::error_code>{error};
        }
        co_return coro_io::socket_wrapper_t{std::move(socket), socket_executor};
    }

    void close_now() override {
        if (!acceptor_) return;
        if (!accept_started_.load(std::memory_order_acquire)) {
            coro_io::detail::close_acceptor_now(*acceptor_);
            return;
        }
        asio::dispatch(executor_->get_asio_executor(), [this]() {
            if (acceptor_) coro_io::detail::close_acceptor_now(*acceptor_);
        });
    }

    void close() override {
        close_now();
        if (accept_started_.load(std::memory_order_acquire)) {
            acceptor_close_future_.wait();
        }
    }

   private:
    int fd_ = -1;
    bool consumed_ = false;
    std::optional<asio::ip::tcp::acceptor> acceptor_;
    coro_io::ExecutorWrapper<> *executor_ = nullptr;
    std::promise<void> acceptor_close_waiter_;
    std::future<void> acceptor_close_future_ =
        acceptor_close_waiter_.get_future();
    std::atomic<bool> accept_started_{false};
};

class HandshakeRpc {
   public:
    HandshakeRpc() { LoadLimits(); }
    ~HandshakeRpc() { Stop(); }

    HandshakeRpc(const HandshakeRpc &) = delete;
    HandshakeRpc &operator=(const HandshakeRpc &) = delete;

    bool running() const { return running_; }

    bool matches(uint16_t port, bool ipv6, int threads) const {
        return running_ && port_ == port && ipv6_ == ipv6 && threads_ == threads;
    }

    int Start(uint16_t port, int sockfd, bool *fd_consumed) {
        if (fd_consumed != nullptr) *fd_consumed = false;
        LoadLimits();
        HandshakeAdmission::Get().Configure(max_bytes_, max_frame_);
        static std::once_flag log_once;
        std::call_once(log_once, [] {
            easylog::set_min_severity(easylog::Severity::ERROR);
        });

        HandshakeServerConfig config;
        config.thread_num = static_cast<unsigned>(threads_);
        config.port = port;
        config.address = ipv6_ ? "::" : "0.0.0.0";
        config.is_enable_tcp_no_delay = true;
        config.conn_timeout_duration = std::chrono::seconds(timeout_seconds_);

        std::vector<std::unique_ptr<coro_io::server_acceptor_base>> acceptors;
        BoundFdAcceptor *bound = nullptr;
        if (sockfd >= 0) {
            auto owned = std::make_unique<BoundFdAcceptor>(
                sockfd, config.address, port);
            bound = owned.get();
            acceptors.push_back(std::move(owned));
        }
        if (acceptors.empty()) {
            server_ = new HandshakeServer(config);
        } else {
            server_ = new HandshakeServer(config, std::move(acceptors));
        }
        server_->register_handler<&HandshakeRpc::process>(this);
        server_->async_start();
        const auto err = server_->get_errc();
        if (bound != nullptr && bound->consumed() && fd_consumed != nullptr) {
            *fd_consumed = true;
        }
        if (err) {
            LOG(ERROR) << "coro handshake listen failed: " << err.message();
            delete server_;
            server_ = nullptr;
            return ERR_SOCKET;
        }
        port_ = port;
        running_ = true;
        LOG(INFO) << "coro handshake listening on port " << port_
                  << " threads=" << threads_
                  << " max_inflight_bytes=" << max_bytes_
                  << " max_frame=" << max_frame_;
        return 0;
    }

    void Stop() {
        {
            std::lock_guard<std::mutex> lock(lifecycle_mu_);
            stopping_ = true;
        }
        if (server_ != nullptr) {
            for (auto &acceptor : server_->get_acceptors()) acceptor->close();
        }
        {
            std::unique_lock<std::mutex> lock(lifecycle_mu_);
            while (inflight_ > 0) {
                LOG(WARNING) << "Waiting for " << inflight_
                             << " handshake handlers to finish";
                lifecycle_cv_.wait_for(lock, std::chrono::seconds(1));
            }
        }
        if (server_ != nullptr) {
            server_->stop();
            delete server_;
            server_ = nullptr;
        }
        running_ = false;
        std::lock_guard<std::mutex> lock(pools_mu_);
        for (auto &entry : pools_) {
            std::lock_guard<std::mutex> pool_lock(entry.second->mu);
            entry.second->closing = true;
            entry.second->idle.clear();
        }
        pools_.clear();
    }

    void SetCallback(int func_id, HandShakePlugin::OnReceiveCallBack callback) {
        std::lock_guard<std::mutex> lock(callback_mu_);
        callbacks_[func_id] = std::move(callback);
    }

    int Call(const std::string &host, uint16_t port, int func_id,
             const std::string &request, std::string &response) {
        if (request.size() > max_frame_) {
            LOG(ERROR) << "handshake request exceeds MC_HANDSHAKE_MAX_LENGTH";
            return ERR_INVALID_ARGUMENT;
        }
        std::vector<std::string> addresses;
        if (Resolve(host, port, addresses) != 0) return ERR_DNS;
        int last = ERR_SOCKET;
        for (const std::string &address : addresses) {
            auto result = async_simple::coro::syncAwait(
                CallOne(address, port, func_id, request));
            if (result.first == 0) {
                response = std::move(result.second);
                return 0;
            }
            last = result.first;
        }
        return last;
    }

    async_simple::coro::Lazy<void> process(int func_id) {
        auto entered = EnterHandler();
        if (!entered) co_return;
        InflightGuard guard(this);

        auto *ctx =
            co_await coro_rpc::get_context_in_coro<HandshakeRpcProtocol>();
        const std::string_view attachment = ctx->get_request_attachment();
        PayloadGuard payload(attachment);

        std::optional<Permit> permit;
        const bool offload = func_id == kConnection || func_id == kMetadata;
        if (offload) {
            if (!TryPermit()) {
                throw std::runtime_error("handshake-busy");
            }
            permit.emplace(this);
        }

        HandShakePlugin::OnReceiveCallBack callback;
        {
            std::lock_guard<std::mutex> lock(callback_mu_);
            auto it = callbacks_.find(func_id);
            if (it != callbacks_.end()) callback = it->second;
        }
        if (func_id < kConnection || func_id > kProbe) {
            throw std::runtime_error("handshake-bad-json");
        }
        if (!callback) {
            throw std::runtime_error("handshake-callback-missing");
        }

        Json::Value peer;
        Json::Value local(Json::objectValue);
        if (func_id != kProbe || !attachment.empty()) {
            std::string error;
            if (!attachment.empty() && !ParseJson(attachment, peer, error)) {
                throw std::runtime_error("handshake-bad-json: " + error);
            }
        }
        int rc = 0;
        try {
            if (offload) {
                auto posted = co_await coro_io::post([&] {
                    rc = callback(peer, local);
                });
                posted.value();
            } else {
                rc = callback(peer, local);
            }
        } catch (const std::exception &ex) {
            throw std::runtime_error(std::string("handshake-handler-failed: ") +
                                     ex.what());
        }
        if (rc != 0 && func_id != kConnection) {
            throw std::runtime_error("handshake-handler-failed");
        }
        std::string encoded = WriteJson(local);
        if (encoded.size() > max_frame_) {
            LOG(ERROR) << "handshake response exceeds MC_HANDSHAKE_MAX_LENGTH";
            ctx->close();
            throw std::runtime_error("handshake-response-too-large");
        }
        ctx->set_response_attachment(std::move(encoded));
        co_return;
    }

   private:
    struct Permit {
        explicit Permit(HandshakeRpc *rpc) : rpc_(rpc) {}
        ~Permit() { rpc_->ReleasePermit(); }
        HandshakeRpc *rpc_;
    };

    struct InflightGuard {
        explicit InflightGuard(HandshakeRpc *rpc) : rpc_(rpc) {}
        ~InflightGuard() { rpc_->LeaveHandler(); }
        HandshakeRpc *rpc_;
    };

    struct PayloadGuard {
        explicit PayloadGuard(std::string_view attachment)
            : key_(attachment.empty() ? nullptr : attachment.data()),
              anonymous_(attachment.empty()) {}
        ~PayloadGuard() {
            if (anonymous_) HandshakeAdmission::Get().ReleaseAnonymous();
            else HandshakeAdmission::Get().ReleaseKey(key_);
        }
        const void *key_;
        bool anonymous_;
    };

    struct IdleClient {
        std::unique_ptr<coro_rpc::coro_rpc_client> client;
        std::chrono::steady_clock::time_point idle_since;
    };

    struct ClientPool {
        std::mutex mu;
        std::string key;
        uint64_t generation = 0;
        int active = 0;
        bool closing = false;
        std::chrono::steady_clock::time_point last_used =
            std::chrono::steady_clock::now();
        std::vector<IdleClient> idle;
    };

    struct Lease {
        std::unique_ptr<coro_rpc::coro_rpc_client> client;
        std::shared_ptr<ClientPool> pool;
        uint64_t generation = 0;
        bool from_pool = false;
        bool broken = false;
        int max_idle = kDefaultIdlePerPeer;

        Lease() = default;
        Lease(const Lease &) = delete;
        Lease &operator=(const Lease &) = delete;
        Lease(Lease &&other) noexcept
            : client(std::move(other.client)),
              pool(std::move(other.pool)),
              generation(other.generation),
              from_pool(other.from_pool),
              broken(other.broken),
              max_idle(other.max_idle) {}
        Lease &operator=(Lease &&other) noexcept {
            if (this == &other) return *this;
            client = std::move(other.client);
            pool = std::move(other.pool);
            generation = other.generation;
            from_pool = other.from_pool;
            broken = other.broken;
            max_idle = other.max_idle;
            return *this;
        }

        ~Lease() {
            if (!pool) return;
            std::lock_guard<std::mutex> lock(pool->mu);
            if (pool->active > 0) --pool->active;
            if (broken || pool->closing || !client) return;
            if (static_cast<int>(pool->idle.size()) >= max_idle) return;
            pool->idle.push_back(
                IdleClient{std::move(client), std::chrono::steady_clock::now()});
            pool->last_used = std::chrono::steady_clock::now();
        }
    };

    void LoadLimits() {
        max_frame_ = getHandshakeMaxLength();
        threads_ = ParsePositiveInt("MC_HANDSHAKE_RPC_THREADS", kDefaultThreads,
                                    1, 8);
        max_concurrency_ = ParsePositiveInt("MC_HANDSHAKE_MAX_CONCURRENCY",
                                            kDefaultConcurrency, 1, 1024);
        max_pools_ =
            ParsePositiveInt("MC_HANDSHAKE_MAX_POOLS", kDefaultPools, 1, 1000000);
        max_idle_per_peer_ = ParsePositiveInt("MC_HANDSHAKE_MAX_IDLE_PER_PEER",
                                              kDefaultIdlePerPeer, 1, 128);
        timeout_seconds_ = ParsePositiveInt("MC_HANDSHAKE_RPC_TIMEOUT_SECONDS",
                                            kDefaultTimeoutSeconds, 1, 3600);
        idle_ttl_seconds_ = ParsePositiveInt("MC_HANDSHAKE_IDLE_TTL_SECONDS",
                                             kDefaultTimeoutSeconds, 1, 3600);
        uint64_t configured = ParseUint64("MC_HANDSHAKE_MAX_INFLIGHT_BYTES",
                                          kDefaultInflightBytes);
        const uint64_t floor = max_frame_ + kRpcEnvelopeBytes;
        max_bytes_ = configured < floor ? floor : configured;
        ipv6_ = globalConfig().use_ipv6;
        connect_timeout_seconds_ = globalConfig().handshake_connect_timeout;
        if (connect_timeout_seconds_ <= 0) connect_timeout_seconds_ = 5;
    }

    bool EnterHandler() {
        std::lock_guard<std::mutex> lock(lifecycle_mu_);
        if (stopping_) return false;
        ++inflight_;
        return true;
    }

    void LeaveHandler() {
        std::lock_guard<std::mutex> lock(lifecycle_mu_);
        if (inflight_ > 0) --inflight_;
        if (inflight_ == 0) lifecycle_cv_.notify_all();
    }

    bool TryPermit() {
        int current = inflight_offload_.load(std::memory_order_relaxed);
        while (current < max_concurrency_) {
            if (inflight_offload_.compare_exchange_weak(
                    current, current + 1, std::memory_order_acq_rel)) {
                return true;
            }
        }
        return false;
    }

    void ReleasePermit() {
        inflight_offload_.fetch_sub(1, std::memory_order_acq_rel);
    }

    int Resolve(const std::string &host, uint16_t port,
                std::vector<std::string> &addresses) const {
        addrinfo hints{};
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_family = ipv6_ ? AF_INET6 : AF_INET;
        addrinfo *result = nullptr;
        char service[8];
        std::snprintf(service, sizeof(service), "%u", port);
        if (getaddrinfo(host.c_str(), service, &hints, &result) != 0) {
            LOG(ERROR) << "handshake DNS failed for " << host << ":" << port;
            return ERR_DNS;
        }
        for (addrinfo *node = result; node != nullptr; node = node->ai_next) {
            char buffer[INET6_ADDRSTRLEN];
            if (node->ai_family == AF_INET) {
                auto *address =
                    reinterpret_cast<sockaddr_in *>(node->ai_addr);
                inet_ntop(AF_INET, &address->sin_addr, buffer, sizeof(buffer));
                addresses.emplace_back(buffer);
            } else if (node->ai_family == AF_INET6) {
                auto *address =
                    reinterpret_cast<sockaddr_in6 *>(node->ai_addr);
                inet_ntop(AF_INET6, &address->sin6_addr, buffer, sizeof(buffer));
                addresses.emplace_back(buffer);
            }
        }
        freeaddrinfo(result);
        if (addresses.empty()) return ERR_DNS;
        return 0;
    }

    std::shared_ptr<ClientPool> GetPool(const std::string &key) {
        std::lock_guard<std::mutex> lock(pools_mu_);
        const auto now = std::chrono::steady_clock::now();
        for (auto it = pools_.begin(); it != pools_.end();) {
            bool drop = false;
            {
                std::lock_guard<std::mutex> pool_lock(it->second->mu);
                ExpireIdle(*it->second, now);
                if (it->second->active == 0 && it->second->idle.empty()) {
                    it->second->closing = true;
                    drop = true;
                }
            }
            if (drop) it = pools_.erase(it);
            else ++it;
        }
        auto found = pools_.find(key);
        if (found != pools_.end()) {
            std::lock_guard<std::mutex> pool_lock(found->second->mu);
            found->second->last_used = now;
            return found->second;
        }
        while (static_cast<int>(pools_.size()) >= max_pools_) {
            std::shared_ptr<ClientPool> victim;
            auto oldest = std::chrono::steady_clock::time_point::max();
            for (auto &entry : pools_) {
                std::lock_guard<std::mutex> pool_lock(entry.second->mu);
                if (entry.second->active != 0 || entry.second->closing) continue;
                if (entry.second->last_used < oldest) {
                    oldest = entry.second->last_used;
                    victim = entry.second;
                }
            }
            if (!victim) return nullptr;
            {
                std::lock_guard<std::mutex> pool_lock(victim->mu);
                victim->closing = true;
                victim->idle.clear();
            }
            pools_.erase(victim->key);
        }
        auto created = std::make_shared<ClientPool>();
        created->key = key;
        created->last_used = now;
        pools_.emplace(key, created);
        return created;
    }

    void ExpireIdle(ClientPool &pool,
                    std::chrono::steady_clock::time_point now) const {
        const auto ttl = std::chrono::seconds(idle_ttl_seconds_);
        std::vector<IdleClient> kept;
        kept.reserve(pool.idle.size());
        for (auto &entry : pool.idle) {
            if (now - entry.idle_since < ttl) kept.push_back(std::move(entry));
        }
        pool.idle.swap(kept);
    }

    Lease Acquire(const std::shared_ptr<ClientPool> &pool) {
        Lease lease;
        lease.pool = pool;
        lease.max_idle = max_idle_per_peer_;
        std::lock_guard<std::mutex> lock(pool->mu);
        if (pool->closing) return lease;
        ++pool->active;
        lease.pool = pool;
        pool->last_used = std::chrono::steady_clock::now();
        ExpireIdle(*pool, pool->last_used);
        if (!pool->idle.empty()) {
            lease.client = std::move(pool->idle.back().client);
            pool->idle.pop_back();
            lease.from_pool = true;
            lease.generation = pool->generation;
        } else {
            lease.generation = pool->generation;
        }
        return lease;
    }

    void ClearIfCurrent(const std::shared_ptr<ClientPool> &pool,
                        uint64_t generation) {
        std::lock_guard<std::mutex> lock(pool->mu);
        if (pool->generation != generation) return;
        ++pool->generation;
        pool->idle.clear();
    }

    coro_rpc::coro_rpc_client::config ClientConfig() const {
        coro_rpc::coro_rpc_client::config config;
        config.connect_timeout_duration =
            std::chrono::seconds(connect_timeout_seconds_);
        config.request_timeout_duration = std::chrono::seconds(timeout_seconds_);
        return config;
    }

    static bool PeerAnswered(coro_rpc::errc ec) {
        switch (ec) {
            case coro_rpc::errc::rpc_throw_exception:
            case coro_rpc::errc::function_not_registered:
            case coro_rpc::errc::invalid_rpc_arguments:
            case coro_rpc::errc::invalid_rpc_result:
            case coro_rpc::errc::message_too_large:
                return true;
            default:
                return false;
        }
    }

    static int MapError(const coro_rpc::rpc_error &error) {
        const auto ec = error.code.ec;
        if (ec == coro_rpc::errc::message_too_large ||
            error.msg.find("handshake-response-too-large") != std::string::npos) {
            return ERR_INVALID_ARGUMENT;
        }
        if (error.msg.find("handshake-busy") != std::string::npos) {
            return ERR_TOO_MANY_REQUESTS;
        }
        if (error.msg.find("handshake-bad-json") != std::string::npos) {
            return ERR_MALFORMED_JSON;
        }
        if (error.msg.find("handshake-callback-missing") != std::string::npos ||
            error.msg.find("handshake-handler-failed") != std::string::npos) {
            return ERR_METADATA;
        }
        if (ec == coro_rpc::errc::bad_address) return ERR_DNS;
        if (ec == coro_rpc::errc::timed_out || ec == coro_rpc::errc::io_error ||
            ec == coro_rpc::errc::not_connected) {
            return ERR_SOCKET;
        }
        if (ec == coro_rpc::errc::protocol_error ||
            ec == coro_rpc::errc::function_not_registered ||
            ec == coro_rpc::errc::invalid_rpc_arguments ||
            ec == coro_rpc::errc::invalid_rpc_result ||
            ec == coro_rpc::errc::rpc_throw_exception) {
            return ERR_MALFORMED_JSON;
        }
        return ERR_SOCKET;
    }

    async_simple::coro::Lazy<std::pair<int, std::string>> CallOne(
        std::string host, uint16_t port, int func_id, std::string request) {
        const std::string key = host + ":" + std::to_string(port);
        for (int attempt = 0; attempt < 2; ++attempt) {
            auto pool = GetPool(key);
            if (!pool) {
                co_return std::make_pair(ERR_TOO_MANY_REQUESTS, std::string{});
            }
            Lease lease = Acquire(pool);
            if (!lease.pool || lease.pool->closing) {
                co_return std::make_pair(ERR_TOO_MANY_REQUESTS, std::string{});
            }
            if (!lease.client) {
                lease.client = std::make_unique<coro_rpc::coro_rpc_client>(
                    coro_io::get_global_executor(), ClientConfig());
                auto connected = co_await lease.client->connect(
                    host, std::to_string(port),
                    std::chrono::seconds(connect_timeout_seconds_));
                if (connected) {
                    lease.broken = true;
                    const int code =
                        connected.ec == coro_rpc::errc::bad_address ? ERR_DNS
                                                                   : ERR_SOCKET;
                    co_return std::make_pair(code, std::string{});
                }
            }
            lease.client->set_req_attachment(request);
            auto result =
                co_await lease.client->call<&HandshakeRpc::process>(func_id);
            if (!result.has_value()) {
                const auto ec = result.error().code.ec;
                const bool reuse = PeerAnswered(ec) &&
                                   ec != coro_rpc::errc::message_too_large;
                lease.broken = !reuse;
                if (lease.broken && lease.from_pool &&
                    ec != coro_rpc::errc::message_too_large) {
                    ClearIfCurrent(pool, lease.generation);
                    if (attempt == 0) continue;
                }
                co_return std::make_pair(MapError(result.error()),
                                         std::string{});
            }
            const std::string_view view = lease.client->get_resp_attachment();
            if (view.size() > max_frame_) {
                lease.broken = true;
                co_return std::make_pair(ERR_INVALID_ARGUMENT, std::string{});
            }
            co_return std::make_pair(0, std::string(view));
        }
        co_return std::make_pair(ERR_SOCKET, std::string{});
    }

    HandshakeServer *server_ = nullptr;
    bool running_ = false;
    bool stopping_ = false;
    bool ipv6_ = false;
    uint16_t port_ = 0;
    int threads_ = kDefaultThreads;
    int max_concurrency_ = kDefaultConcurrency;
    int max_pools_ = kDefaultPools;
    int max_idle_per_peer_ = kDefaultIdlePerPeer;
    int timeout_seconds_ = kDefaultTimeoutSeconds;
    int idle_ttl_seconds_ = kDefaultTimeoutSeconds;
    int connect_timeout_seconds_ = 5;
    uint64_t max_frame_ = 1ull << 20;
    uint64_t max_bytes_ = kDefaultInflightBytes;

    std::mutex lifecycle_mu_;
    std::condition_variable lifecycle_cv_;
    int inflight_ = 0;
    std::atomic<int> inflight_offload_{0};

    std::mutex callback_mu_;
    std::unordered_map<int, HandShakePlugin::OnReceiveCallBack> callbacks_;

    std::mutex pools_mu_;
    std::unordered_map<std::string, std::shared_ptr<ClientPool>> pools_;
};

class CoroHandShakePlugin : public HandShakePlugin {
   public:
    int startDaemon(uint16_t listen_port, int sockfd) override {
        const bool ipv6 = globalConfig().use_ipv6;
        const int threads = ParsePositiveInt("MC_HANDSHAKE_RPC_THREADS",
                                             kDefaultThreads, 1, 8);
        if (rpc_.running()) {
            if (rpc_.matches(listen_port, ipv6, threads)) return 0;
            LOG(ERROR) << "coro handshake already listening with different "
                          "parameters";
            return ERR_INVALID_ARGUMENT;
        }
        bool consumed = false;
        const int rc = rpc_.Start(listen_port, sockfd, &consumed);
        if (rc != 0 && sockfd >= 0 && !consumed) close(sockfd);
        return rc;
    }

    int send(std::string ip_or_host_name, uint16_t rpc_port,
             const Json::Value &local, Json::Value &peer) override {
        return Exchange(kConnection, ip_or_host_name, rpc_port, local, peer);
    }

    int sendNotify(std::string ip_or_host_name, uint16_t rpc_port,
                   const Json::Value &local, Json::Value &peer) override {
        return Exchange(kNotify, ip_or_host_name, rpc_port, local, peer);
    }

    int sendProbe(std::string ip_or_host_name, uint16_t rpc_port,
                  const Json::Value &local, Json::Value &peer) override {
        return Exchange(kProbe, ip_or_host_name, rpc_port, local, peer);
    }

    int exchangeMetadata(std::string ip_or_host_name, uint16_t rpc_port,
                         const Json::Value &local_metadata,
                         Json::Value &peer_metadata) override {
        return Exchange(kMetadata, ip_or_host_name, rpc_port, local_metadata,
                        peer_metadata);
    }

    void registerOnConnectionCallBack(OnReceiveCallBack callback) override {
        rpc_.SetCallback(kConnection, std::move(callback));
    }

    void registerOnMetadataCallBack(OnReceiveCallBack callback) override {
        rpc_.SetCallback(kMetadata, std::move(callback));
    }

    void registerOnNotifyCallBack(OnReceiveCallBack callback) override {
        rpc_.SetCallback(kNotify, std::move(callback));
    }

    void registerOnProbeCallBack(OnReceiveCallBack callback) override {
        rpc_.SetCallback(kProbe, std::move(callback));
    }

    int CallRaw(const std::string &host, uint16_t port, int func_id,
                const std::string &attachment, std::string &response) {
        return rpc_.Call(host, port, func_id, attachment, response);
    }

   private:
    int Exchange(int func_id, const std::string &host, uint16_t port,
                 const Json::Value &local, Json::Value &peer) {
        std::string response;
        const int rc =
            rpc_.Call(host, port, func_id, WriteJson(local), response);
        if (rc != 0) return rc;
        std::string error;
        if (!ParseJson(response, peer, error)) {
            LOG(ERROR) << "coro handshake response is not JSON: " << error;
            return ERR_MALFORMED_JSON;
        }
        return 0;
    }

    HandshakeRpc rpc_;
};

class InvalidHandShakePlugin : public HandShakePlugin {
   public:
    explicit InvalidHandShakePlugin(std::string configured)
        : configured_(std::move(configured)) {}

    int startDaemon(uint16_t, int) override { return Fail(); }
    int send(std::string, uint16_t, const Json::Value &,
             Json::Value &) override {
        return Fail();
    }
    int sendNotify(std::string, uint16_t, const Json::Value &,
                   Json::Value &) override {
        return Fail();
    }
    int sendProbe(std::string, uint16_t, const Json::Value &,
                  Json::Value &) override {
        return Fail();
    }
    int exchangeMetadata(std::string, uint16_t, const Json::Value &,
                         Json::Value &) override {
        return Fail();
    }
    void registerOnConnectionCallBack(OnReceiveCallBack) override {}
    void registerOnMetadataCallBack(OnReceiveCallBack) override {}
    void registerOnNotifyCallBack(OnReceiveCallBack) override {}
    void registerOnProbeCallBack(OnReceiveCallBack) override {}

   private:
    int Fail() const {
        LOG(ERROR) << "Invalid MC_HANDSHAKE_TRANSPORT=\"" << configured_
                   << "\", allowed values: socket, coro";
        return ERR_INVALID_ARGUMENT;
    }
    std::string configured_;
};

}  // namespace

std::shared_ptr<HandShakePlugin> CreateCoroHandshakePlugin() {
    return std::make_shared<CoroHandShakePlugin>();
}

std::shared_ptr<HandShakePlugin> CreateInvalidHandshakePlugin(
    const std::string &configured_value) {
    return std::make_shared<InvalidHandShakePlugin>(configured_value);
}

int CallRawHandshakeForTest(HandShakePlugin *plugin, const std::string &host,
                            uint16_t port, int func_id,
                            const std::string &attachment,
                            std::string &response) {
    auto *coro = dynamic_cast<CoroHandShakePlugin *>(plugin);
    if (coro == nullptr) return ERR_INVALID_ARGUMENT;
    return coro->CallRaw(host, port, func_id, attachment, response);
}

}  // namespace mooncake
