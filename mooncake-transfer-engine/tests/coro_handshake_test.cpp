// Copyright 2024 KVCache.AI
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

#include "common.h"
#include "error.h"
#include "handshake_rpc.h"
#include "transfer_metadata_plugin.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <future>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>
#include <ylt/struct_pack.hpp>
#include <ylt/coro_rpc/impl/protocol/coro_rpc_protocol.hpp>

namespace {

struct ForceCoroTransport {
    ForceCoroTransport() {
        setenv("MC_HANDSHAKE_TRANSPORT", "coro", 1);
        setenv("MC_HANDSHAKE_MAX_LENGTH", "1048576", 0);
        setenv("MC_HANDSHAKE_MAX_CONCURRENCY", "2", 1);
        setenv("MC_HANDSHAKE_MAX_POOLS", "2", 1);
        setenv("MC_HANDSHAKE_MAX_INFLIGHT_BYTES", "1200000", 1);
        setenv("MC_HANDSHAKE_RPC_TIMEOUT_SECONDS", "5", 1);
        setenv("MC_HANDSHAKE_IDLE_TTL_SECONDS", "1", 1);
        setenv("MC_HANDSHAKE_CONNECT_TIMEOUT", "1", 1);
        setenv("MC_HANDSHAKE_RPC_THREADS", "2", 1);
    }
};

ForceCoroTransport g_force_coro_transport;

int ListenLoopback(uint16_t &port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int on = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    if (bind(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) !=
        0) {
        close(fd);
        return -1;
    }
    sockaddr_in bound{};
    socklen_t length = sizeof(bound);
    if (getsockname(fd, reinterpret_cast<sockaddr *>(&bound), &length) != 0) {
        close(fd);
        return -1;
    }
    port = ntohs(bound.sin_port);
    return fd;
}

class CoroServer {
   public:
    void Start() {
        port_ = 0;
        fd_ = ListenLoopback(port_);
        ASSERT_GE(fd_, 0);
        plugin_ = mooncake::CreateCoroHandshakePlugin();
        plugin_->registerOnProbeCallBack(
            [](const Json::Value &, Json::Value &local) {
                local = Json::Value(Json::objectValue);
                return 0;
            });
        ASSERT_EQ(plugin_->startDaemon(port_, fd_), 0);
        fd_ = -1;
    }

    ~CoroServer() {
        plugin_.reset();
        if (fd_ >= 0) close(fd_);
    }

    mooncake::HandShakePlugin &plugin() { return *plugin_; }
    uint16_t port() const { return port_; }
    std::shared_ptr<mooncake::HandShakePlugin> shared() { return plugin_; }

   private:
    std::shared_ptr<mooncake::HandShakePlugin> plugin_;
    uint16_t port_ = 0;
    int fd_ = -1;
};

Json::Value Object() { return Json::Value(Json::objectValue); }

}  // namespace

TEST(CoroHandshake, ProbeMetadataNotifyAndConnection) {
    CoroServer server;
    server.Start();
    server.plugin().registerOnMetadataCallBack(
        [](const Json::Value &peer, Json::Value &local) {
            EXPECT_EQ(peer["name"].asString(), "remote");
            local["name"] = "local";
            return 0;
        });
    server.plugin().registerOnNotifyCallBack(
        [](const Json::Value &peer, Json::Value &local) {
            EXPECT_EQ(peer["notify_msg"].asString(), "ping");
            local["name"] = "srv";
            local["notify_msg"] = "pong";
            return 0;
        });
    server.plugin().registerOnConnectionCallBack(
        [](const Json::Value &peer, Json::Value &local) {
            EXPECT_EQ(peer["local_nic_path"].asString(), "nic0");
            local["reply_msg"] = "";
            local["peer_nic_path"] = "nic0";
            return 0;
        });

    Json::Value request = Object();
    Json::Value response;
    EXPECT_EQ(server.plugin().sendProbe("127.0.0.1", server.port(), request,
                                        response),
              0);

    request["name"] = "remote";
    EXPECT_EQ(server.plugin().exchangeMetadata("127.0.0.1", server.port(),
                                               request, response),
              0);
    EXPECT_EQ(response["name"].asString(), "local");

    request = Object();
    request["name"] = "cli";
    request["notify_msg"] = "ping";
    EXPECT_EQ(server.plugin().sendNotify("127.0.0.1", server.port(), request,
                                         response),
              0);
    EXPECT_EQ(response["notify_msg"].asString(), "pong");

    request = Object();
    request["local_nic_path"] = "nic0";
    EXPECT_EQ(
        server.plugin().send("127.0.0.1", server.port(), request, response), 0);
    EXPECT_EQ(response["peer_nic_path"].asString(), "nic0");
}

TEST(CoroHandshake, ConnectionCallbackCanBeRegisteredAfterListen) {
    CoroServer server;
    server.Start();
    Json::Value response;
    EXPECT_NE(server.plugin().send("127.0.0.1", server.port(), Object(),
                                   response),
              0);
    server.plugin().registerOnConnectionCallBack(
        [](const Json::Value &, Json::Value &local) {
            local["reply_msg"] = "";
            return 0;
        });
    EXPECT_EQ(server.plugin().send("127.0.0.1", server.port(), Object(),
                                   response),
              0);
}

TEST(CoroHandshake, RepeatedStartRequiresTheSameEndpoint) {
    CoroServer server;
    server.Start();
    EXPECT_EQ(server.plugin().startDaemon(server.port(), -1), 0);
    uint16_t other = 0;
    int fd = ListenLoopback(other);
    ASSERT_GE(fd, 0);
    EXPECT_EQ(server.plugin().startDaemon(other, fd),
              ERR_INVALID_ARGUMENT);
    close(fd);
}

TEST(CoroHandshake, IllegalTransportDoesNotReturnNull) {
    setenv("MC_HANDSHAKE_TRANSPORT", "quic", 1);
    auto plugin = mooncake::HandShakePlugin::Create("P2PHANDSHAKE");
    ASSERT_NE(plugin, nullptr);
    EXPECT_EQ(plugin->startDaemon(1, -1), ERR_INVALID_ARGUMENT);
    Json::Value response;
    EXPECT_EQ(plugin->sendProbe("127.0.0.1", 1, Object(), response),
              ERR_INVALID_ARGUMENT);
    setenv("MC_HANDSHAKE_TRANSPORT", "coro", 1);
}

TEST(CoroHandshake, LocalOversizeDoesNotConnect) {
    uint16_t port = 0;
    int fd = ListenLoopback(port);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(listen(fd, 1), 0);
    std::atomic<bool> accepted{false};
    std::thread acceptor([&] {
        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(fd, &fds);
        timeval wait{};
        wait.tv_sec = 1;
        if (select(fd + 1, &fds, nullptr, nullptr, &wait) > 0) accepted = true;
    });
    auto client = mooncake::CreateCoroHandshakePlugin();
    std::string response;
    const std::string payload(mooncake::getHandshakeMaxLength() + 1, 'x');
    const auto started = std::chrono::steady_clock::now();
    EXPECT_EQ(mooncake::CallRawHandshakeForTest(client.get(), "127.0.0.1", port,
                                               1, payload, response),
              ERR_INVALID_ARGUMENT);
    EXPECT_LT(std::chrono::steady_clock::now() - started,
              std::chrono::seconds(1));
    acceptor.join();
    EXPECT_FALSE(accepted.load());
    close(fd);
}

TEST(CoroHandshake, OversizedHeaderIsClosedBeforeTheBody) {
    CoroServer server;
    server.Start();
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(fd, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(server.port());
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(connect(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)),
              0);

    coro_rpc::protocol::coro_rpc_protocol::req_header header{};
    header.magic = coro_rpc::protocol::coro_rpc_protocol::magic_number;
    header.version = 0;
    header.length = 0;
    header.attach_length =
        static_cast<uint32_t>(mooncake::getHandshakeMaxLength() + 1);
    std::string bytes;
    struct_pack::serialize_to<struct_pack::sp_config::DISABLE_ALL_META_INFO>(
        bytes, header);
    ASSERT_EQ(send(fd, bytes.data(), bytes.size(), MSG_NOSIGNAL),
              static_cast<ssize_t>(bytes.size()));

    const auto started = std::chrono::steady_clock::now();
    char scratch;
    const ssize_t n = recv(fd, &scratch, 1, 0);
    EXPECT_LE(std::chrono::steady_clock::now() - started,
              std::chrono::seconds(2));
    EXPECT_LE(n, 0);
    close(fd);

    Json::Value response;
    EXPECT_EQ(server.plugin().sendProbe("127.0.0.1", server.port(), Object(),
                                        response),
              0);
}

TEST(CoroHandshake, BlockedConnectionDoesNotStallProbe) {
    CoroServer server;
    server.Start();
    std::promise<void> release;
    auto wait = release.get_future().share();
    std::atomic<int> entered{0};
    server.plugin().registerOnConnectionCallBack(
        [&](const Json::Value &, Json::Value &local) {
            entered.fetch_add(1);
            wait.wait();
            local = Object();
            return 0;
        });
    auto first = std::async(std::launch::async, [&] {
        Json::Value response;
        return server.plugin().send("127.0.0.1", server.port(), Object(),
                                    response);
    });
    auto second = std::async(std::launch::async, [&] {
        Json::Value response;
        return server.plugin().send("127.0.0.1", server.port(), Object(),
                                    response);
    });
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (entered.load() < 2 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(entered.load(), 2);

    const auto probe_started = std::chrono::steady_clock::now();
    Json::Value response;
    EXPECT_EQ(server.plugin().sendProbe("127.0.0.1", server.port(), Object(),
                                        response),
              0);
    EXPECT_LT(std::chrono::steady_clock::now() - probe_started,
              std::chrono::milliseconds(1000));

    Json::Value ignored;
    EXPECT_EQ(server.plugin().send("127.0.0.1", server.port(), Object(),
                                   ignored),
              ERR_TOO_MANY_REQUESTS);
    release.set_value();
    EXPECT_EQ(first.get(), 0);
    EXPECT_EQ(second.get(), 0);
}

TEST(CoroHandshake, CallbackExceptionStaysInsideTheServer) {
    CoroServer server;
    server.Start();
    server.plugin().registerOnConnectionCallBack(
        [](const Json::Value &, Json::Value &) -> int {
            throw std::runtime_error("boom");
        });
    Json::Value response;
    EXPECT_EQ(
        server.plugin().send("127.0.0.1", server.port(), Object(), response),
        ERR_METADATA);
    EXPECT_EQ(server.plugin().sendProbe("127.0.0.1", server.port(), Object(),
                                        response),
              0);
}

TEST(CoroHandshake, MalformedJsonIsRejected) {
    CoroServer server;
    server.Start();
    server.plugin().registerOnMetadataCallBack(
        [](const Json::Value &, Json::Value &local) {
            local = Object();
            return 0;
        });
    std::string response;
    EXPECT_EQ(mooncake::CallRawHandshakeForTest(server.shared().get(),
                                               "127.0.0.1", server.port(), 1,
                                               "{", response),
              ERR_MALFORMED_JSON);
    Json::Value probe;
    EXPECT_EQ(server.plugin().sendProbe("127.0.0.1", server.port(), Object(),
                                        probe),
              0);
}

TEST(CoroHandshake, RestartDropsTheStalePooledConnection) {
    uint16_t port = 0;
    int fd = ListenLoopback(port);
    ASSERT_GE(fd, 0);
    auto server = mooncake::CreateCoroHandshakePlugin();
    server->registerOnProbeCallBack([](const Json::Value &, Json::Value &local) {
        local = Object();
        return 0;
    });
    ASSERT_EQ(server->startDaemon(port, fd), 0);
    auto client = mooncake::CreateCoroHandshakePlugin();
    Json::Value response;
    ASSERT_EQ(client->sendProbe("127.0.0.1", port, Object(), response), 0);
    server.reset();

    fd = ListenLoopback(port);
    ASSERT_GE(fd, 0);
    server = mooncake::CreateCoroHandshakePlugin();
    server->registerOnProbeCallBack([](const Json::Value &, Json::Value &local) {
        local = Object();
        return 0;
    });
    ASSERT_EQ(server->startDaemon(port, fd), 0);
    EXPECT_EQ(client->sendProbe("127.0.0.1", port, Object(), response), 0);
}

TEST(CoroHandshake, PoolLimitRejectsANewPeerWhileLeasesAreActive) {
    auto accept_and_hold = [](uint16_t *port, std::atomic<int> *ready) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        int on = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
        bind(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address));
        sockaddr_in bound{};
        socklen_t length = sizeof(bound);
        getsockname(fd, reinterpret_cast<sockaddr *>(&bound), &length);
        *port = ntohs(bound.sin_port);
        listen(fd, 4);
        ready->fetch_add(1);
        int conn = accept(fd, nullptr, nullptr);
        if (conn >= 0) {
            std::this_thread::sleep_for(std::chrono::seconds(3));
            close(conn);
        }
        close(fd);
    };
    uint16_t first_port = 0;
    uint16_t second_port = 0;
    std::atomic<int> ready{0};
    std::thread first(accept_and_hold, &first_port, &ready);
    std::thread second(accept_and_hold, &second_port, &ready);
    while (ready.load() < 2) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    auto client = mooncake::CreateCoroHandshakePlugin();
    auto hold = [](mooncake::HandShakePlugin &plugin, uint16_t port) {
        Json::Value response;
        plugin.sendProbe("127.0.0.1", port, Object(), response);
    };
    auto left =
        std::async(std::launch::async, hold, std::ref(*client), first_port);
    auto right =
        std::async(std::launch::async, hold, std::ref(*client), second_port);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    const auto started = std::chrono::steady_clock::now();
    Json::Value response;
    EXPECT_EQ(client->sendProbe("127.0.0.1", 1, Object(), response),
              ERR_TOO_MANY_REQUESTS);
    EXPECT_LT(std::chrono::steady_clock::now() - started,
              std::chrono::milliseconds(500));
    left.wait();
    right.wait();
    first.join();
    second.join();
}

TEST(CoroHandshake, ByteBudgetRejectsASecondLargeFrame) {
    CoroServer server;
    server.Start();
    std::promise<void> release;
    auto wait = release.get_future().share();
    server.plugin().registerOnMetadataCallBack(
        [&](const Json::Value &, Json::Value &local) {
            wait.wait();
            local = Object();
            return 0;
        });
    const std::string payload(700000, 'a');
    auto first = std::async(std::launch::async, [&] {
        std::string response;
        return mooncake::CallRawHandshakeForTest(server.shared().get(),
                                                 "127.0.0.1", server.port(), 1,
                                                 payload, response);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    std::string response;
    // The server closes without reading the body. The client may observe
    // that as a transport error or as a protocol error, depending on how
    // much of the request had already been written.
    const int rejected = mooncake::CallRawHandshakeForTest(
        server.shared().get(), "127.0.0.1", server.port(), 1, payload,
        response);
    EXPECT_TRUE(rejected == ERR_SOCKET || rejected == ERR_MALFORMED_JSON)
        << rejected;
    release.set_value();
    first.wait();
    Json::Value probe;
    EXPECT_EQ(server.plugin().sendProbe("127.0.0.1", server.port(), Object(),
                                        probe),
              0);
}

TEST(CoroHandshake, OversizedResponseIsNotReturned) {
    CoroServer server;
    server.Start();
    server.plugin().registerOnMetadataCallBack(
        [](const Json::Value &, Json::Value &local) {
            local["blob"] = std::string(mooncake::getHandshakeMaxLength(), 'z');
            return 0;
        });
    Json::Value response;
    const int rc = server.plugin().exchangeMetadata(
        "127.0.0.1", server.port(), Object(), response);
    EXPECT_TRUE(rc == ERR_INVALID_ARGUMENT ||
                rc == ERR_SOCKET);
    EXPECT_FALSE(response.isMember("blob"));
}

TEST(CoroHandshake, MetadataFailureIsAnRpcError) {
    CoroServer server;
    server.Start();
    server.plugin().registerOnMetadataCallBack(
        [](const Json::Value &, Json::Value &) { return -1; });
    Json::Value response;
    EXPECT_EQ(server.plugin().exchangeMetadata("127.0.0.1", server.port(),
                                               Object(), response),
              ERR_METADATA);
}

TEST(CoroHandshake, AdoptFailureLeavesTheCallerAbleToClose) {
    int fd = open("/dev/null", O_RDONLY);
    ASSERT_GE(fd, 0);
    auto plugin = mooncake::CreateCoroHandshakePlugin();
    EXPECT_NE(plugin->startDaemon(1, fd), 0);
    EXPECT_EQ(close(fd), -1);
    EXPECT_EQ(errno, EBADF);
}

TEST(CoroHandshake, DnsFailureIsReported) {
    auto client = mooncake::CreateCoroHandshakePlugin();
    Json::Value response;
    EXPECT_EQ(client->sendProbe("no-such-host.invalid", 1, Object(), response),
              ERR_DNS);
}
