#include "storage/distributed/kvcs/kvcs_object_storage_adapter.h"

#include <array>
#include <unordered_set>
#include <utility>

#include "storage/distributed/kvcs/kvcs_capi_driver.h"
#include "storage/distributed/kvcs/kvcs_efc_target.h"

namespace mooncake {
namespace {

constexpr std::string_view kHealthProbeKey = "kvcs-health-probe";

template <typename T>
std::vector<tl::expected<T, ErrorCode>> Errors(size_t count, ErrorCode error) {
    std::vector<tl::expected<T, ErrorCode>> results;
    results.reserve(count);
    for (size_t i = 0; i < count; ++i)
        results.emplace_back(tl::make_unexpected(error));
    return results;
}

ObjectStorageIoResults IoErrors(size_t count, ErrorCode error) {
    return Errors<void>(count, error);
}

}  // namespace

KvcsObjectStorageAdapter::KvcsObjectStorageAdapter(std::string efc_config_path)
    : efc_config_path_(std::move(efc_config_path)) {}

KvcsObjectStorageAdapter::KvcsObjectStorageAdapter(
    std::unique_ptr<KvcsDriver> driver)
    : mountpoint_index_(1), driver_(std::move(driver)) {}

KvcsObjectStorageAdapter::~KvcsObjectStorageAdapter() = default;

tl::expected<ObjectKey, ErrorCode> KvcsObjectStorageAdapter::EncodeKey(
    std::string_view logical_key) const {
    if (logical_key.empty() || !driver_)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    auto encoded = EncodeKvcsKey(logical_key);
    if (!IsKvcsKeyStringSafe(encoded, driver_->MaxKeySize()))
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    return encoded;
}

tl::expected<void, ErrorCode> KvcsObjectStorageAdapter::Init() {
    if (initialized_) return {};
    std::string mountpoint_id = "injected";
    if (!driver_) {
        auto target = LoadKvcsEfcTarget(efc_config_path_);
        if (!target) return tl::make_unexpected(target.error());
        auto driver = CreateKvcsLowLevelDriver(target->mountpoint_index);
        if (!driver) return tl::make_unexpected(driver.error());
        mountpoint_id = std::move(target->mountpoint_id);
        mountpoint_index_ = target->mountpoint_index;
        driver_ = std::move(*driver);
    }
    if (!driver_) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    auto init = driver_->Init();
    if (!init) return init;
    if (driver_->MaxValueSize() == 0 || driver_->MaxKeySize() == 0 ||
        driver_->MaxKeySize() > kKvcsMaxKeySize)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    initialized_ = true;
    LOG(INFO) << "KVCS Low Level mountpoint_id=" << mountpoint_id
              << ", mountpoint_index=" << mountpoint_index_;
    return {};
}

tl::expected<void, ErrorCode> KvcsObjectStorageAdapter::CheckHealth() {
    if (!initialized_) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    auto probe_key = EncodeKey(kHealthProbeKey);
    if (!probe_key) return tl::make_unexpected(probe_key.error());
    const std::array<ObjectKey, 1> probe{std::move(*probe_key)};
    const auto result = driver_->BatchQuery(probe);
    if (result.size() != 1)
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    if (!result[0] && result[0].error() != ErrorCode::OBJECT_NOT_FOUND)
        return tl::make_unexpected(result[0].error());
    return {};
}

tl::expected<void, ErrorCode> KvcsObjectStorageAdapter::Put(
    const std::string& key, std::span<const char> data) {
    iovec iov{const_cast<char*>(data.data()), data.size()};
    return PutV(key, &iov, 1);
}

tl::expected<void, ErrorCode> KvcsObjectStorageAdapter::PutV(
    const std::string& key, const iovec* iov, int iovcnt) {
    auto results = PutBatch({{key, iov, iovcnt}});
    return std::move(results.front());
}

std::vector<tl::expected<void, ErrorCode>> KvcsObjectStorageAdapter::PutBatch(
    const std::vector<ObjectPutRequest>& requests) {
    if (!initialized_)
        return IoErrors(requests.size(), ErrorCode::INTERNAL_ERROR);
    ObjectStorageIoResults results(requests.size());
    std::vector<KvcsIoRequest> puts;
    std::vector<size_t> put_indices;
    puts.reserve(requests.size());
    put_indices.reserve(requests.size());
    std::unordered_set<std::string> keys;
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        if (!request.iov || request.iovcnt <= 0) {
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        auto key = EncodeKey(request.logical_key);
        if (!key) {
            results[i] = tl::make_unexpected(key.error());
            continue;
        }
        std::vector<Slice> slices;
        slices.reserve(static_cast<size_t>(request.iovcnt));
        uint64_t size = 0;
        bool valid = true;
        for (int j = 0; j < request.iovcnt; ++j) {
            const auto& iov = request.iov[j];
            if (!iov.iov_base || iov.iov_len == 0 ||
                iov.iov_len > driver_->MaxValueSize() - size) {
                valid = false;
                break;
            }
            size += iov.iov_len;
            slices.push_back({iov.iov_base, iov.iov_len});
        }
        if (!valid || size == 0) {
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        if (!keys.insert(*key).second) {
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        puts.push_back({std::move(*key), std::move(slices), size});
        put_indices.push_back(i);
    }

    try {
        auto put_results = driver_->BatchPut(puts);
        if (put_results.size() != puts.size()) {
            for (size_t index : put_indices)
                results[index] = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            return results;
        }
        for (size_t i = 0; i < put_results.size(); ++i)
            results[put_indices[i]] = std::move(put_results[i]);
    } catch (...) {
        for (size_t index : put_indices)
            results[index] = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return results;
}

tl::expected<size_t, ErrorCode> KvcsObjectStorageAdapter::Get(
    const std::string& key, void* buf, size_t len) {
    auto results = GetBatch({{key, buf, len}});
    return std::move(results.front());
}

std::vector<tl::expected<size_t, ErrorCode>> KvcsObjectStorageAdapter::GetBatch(
    const std::vector<ObjectGetRequest>& requests) {
    if (!initialized_)
        return Errors<size_t>(requests.size(), ErrorCode::INTERNAL_ERROR);
    std::vector<tl::expected<size_t, ErrorCode>> results(requests.size());
    std::vector<KvcsIoRequest> gets;
    std::vector<size_t> get_indices;
    gets.reserve(requests.size());
    get_indices.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        auto key = EncodeKey(request.logical_key);
        if (!key || !request.buffer || request.size == 0 ||
            request.size > driver_->MaxValueSize()) {
            results[i] = tl::make_unexpected(key ? ErrorCode::INVALID_PARAMS
                                                 : key.error());
            continue;
        }
        gets.push_back(
            {std::move(*key), {{request.buffer, request.size}}, request.size});
        get_indices.push_back(i);
    }
    try {
        auto get_results = driver_->BatchGet(gets);
        if (get_results.size() != gets.size()) {
            for (size_t index : get_indices)
                results[index] = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            return results;
        }
        for (size_t i = 0; i < get_results.size(); ++i) {
            const size_t request_index = get_indices[i];
            results[request_index] =
                get_results[i]
                    ? tl::expected<size_t, ErrorCode>(
                          requests[request_index].size)
                    : tl::expected<size_t, ErrorCode>(
                          tl::make_unexpected(get_results[i].error()));
        }
    } catch (...) {
        for (size_t index : get_indices)
            results[index] = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return results;
}

tl::expected<bool, ErrorCode> KvcsObjectStorageAdapter::Exists(
    const std::string& key) {
    if (!initialized_) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    auto encoded = EncodeKey(key);
    if (!encoded) return tl::make_unexpected(encoded.error());
    try {
        auto result = driver_->BatchQuery({&*encoded, 1});
        if (result.size() != 1)
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        if (result[0]) return true;
        if (result[0].error() == ErrorCode::OBJECT_NOT_FOUND) return false;
        return tl::make_unexpected(result[0].error());
    } catch (...) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
}

tl::expected<void, ErrorCode> KvcsObjectStorageAdapter::Delete(
    const std::string& key) {
    const std::array<std::string, 1> keys{key};
    auto result = DeleteBatch(keys);
    return std::move(result.front());
}

ObjectStorageIoResults KvcsObjectStorageAdapter::DeleteBatch(
    std::span<const std::string> keys) {
    if (!initialized_) return IoErrors(keys.size(), ErrorCode::INTERNAL_ERROR);
    ObjectStorageIoResults results(keys.size());
    std::vector<ObjectKey> encoded_keys;
    std::vector<size_t> delete_indices;
    encoded_keys.reserve(keys.size());
    delete_indices.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        auto encoded = EncodeKey(keys[i]);
        if (!encoded) {
            results[i] = tl::make_unexpected(encoded.error());
            continue;
        }
        encoded_keys.push_back(std::move(*encoded));
        delete_indices.push_back(i);
    }
    try {
        auto delete_results = driver_->BatchDelete(encoded_keys);
        if (delete_results.size() != encoded_keys.size()) {
            for (size_t index : delete_indices)
                results[index] = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            return results;
        }
        for (size_t i = 0; i < delete_results.size(); ++i)
            results[delete_indices[i]] = std::move(delete_results[i]);
    } catch (...) {
        for (size_t index : delete_indices)
            results[index] = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return results;
}

tl::expected<std::vector<KeyInfo>, ErrorCode>
KvcsObjectStorageAdapter::ListKeys() {
    return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
}

}  // namespace mooncake
