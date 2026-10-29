#include "storage/distributed/kvcs/kvcs_object_storage_adapter.h"
#include "storage/distributed/kvcs/kvcs_capi_driver.h"
#include "storage/distributed/kvcs/kvcs_efc_metrics.h"
#include "storage/distributed/kvcs/kvcs_efc_target.h"

#include <array>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

class MemoryDriver final : public KvcsDriver {
   public:
    tl::expected<void, ErrorCode> Init() override { return {}; }
    uint64_t MaxValueSize() const override { return 8; }

    KvcsIoResults BatchPut(std::span<const KvcsIoRequest> requests) override {
        ++put_calls_;
        KvcsIoResults results;
        for (const auto& request : requests) {
            if (values_.contains(request.logical_key)) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS));
                continue;
            }
            std::string value;
            for (const auto& slice : request.slices)
                value.append(static_cast<const char*>(slice.ptr), slice.size);
            values_[request.logical_key] = std::move(value);
            results.emplace_back();
        }
        return results;
    }

    KvcsIoResults BatchGet(std::span<const KvcsIoRequest> requests) override {
        KvcsIoResults results;
        for (const auto& request : requests) {
            auto it = values_.find(request.logical_key);
            if (it == values_.end()) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
                continue;
            }
            size_t offset = 0;
            if (request.size != it->second.size()) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::FILE_READ_FAIL));
                continue;
            }
            for (const auto& slice : request.slices) {
                std::memcpy(slice.ptr, it->second.data() + offset, slice.size);
                offset += slice.size;
            }
            if (offset == it->second.size())
                results.emplace_back();
            else
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::INVALID_PARAMS));
        }
        return results;
    }

    KvcsIoResults BatchDelete(std::span<const ObjectKey> keys) override {
        KvcsIoResults results;
        for (const auto& key : keys) {
            values_.erase(key);
            results.emplace_back();
        }
        return results;
    }

    size_t put_calls() const { return put_calls_; }

   private:
    size_t put_calls_ = 0;
    std::map<ObjectKey, std::string> values_;
};

class ScopedEfcEnvironment {
   public:
    ScopedEfcEnvironment() {
        for (const char* name :
             {"KVCS_MOUNTPOINTS_JSON", "MOONCAKE_KVCS_METRICS_ENDPOINT"}) {
            const char* value = std::getenv(name);
            saved_.emplace_back(
                name, value ? std::optional<std::string>(value) : std::nullopt);
            unsetenv(name);
        }
    }
    ~ScopedEfcEnvironment() {
        for (const auto& [name, value] : saved_) {
            if (value)
                setenv(name.c_str(), value->c_str(), 1);
            else
                unsetenv(name.c_str());
        }
    }

   private:
    std::vector<std::pair<std::string, std::optional<std::string>>> saved_;
};

KvcsObjectStorageAdapter MakeAdapter() {
    return KvcsObjectStorageAdapter(std::make_unique<MemoryDriver>());
}

constexpr std::string_view kCapacityMetrics = R"(
# HELP kvcs_efc_storage_capacity_bytes capacity
unrelated_metric NaN
kvcs_efc_storage_capacity_bytes{mountpoint_id="other",tier="g35",state="total"} 999
kvcs_efc_storage_capacity_bytes{state="used",mountpoint_id="target-mp",tier="g35"} 25
kvcs_efc_storage_ready{mountpoint_id="target-mp",tier="g35"} 1
kvcs_efc_storage_capacity_bytes{tier="g3",mountpoint_id="target-mp",state="total"} 999
kvcs_efc_storage_capacity_bytes{mountpoint_id="target-mp",state="total",tier="g35"} 100
)";

TEST(KvcsEfcMetricsCapacityTest, ParsesConfiguredG35Mountpoint) {
    auto capacity = ParseKvcsEfcCapacityMetrics(kCapacityMetrics, "target-mp");
    ASSERT_TRUE(capacity.has_value()) << capacity.error();
    EXPECT_TRUE(capacity->enabled);
    EXPECT_EQ(capacity->used_bytes, 25u);
    EXPECT_EQ(capacity->capacity_bytes, 100u);
}

TEST(KvcsEfcMetricsCapacityTest, RejectsDuplicateOrUnreadySamples) {
    const std::string duplicate = std::string(kCapacityMetrics) +
                                  "kvcs_efc_storage_ready{tier=\"g35\","
                                  "mountpoint_id=\"target-mp\"} 1\n";
    EXPECT_FALSE(ParseKvcsEfcCapacityMetrics(duplicate, "target-mp"));

    std::string unready(kCapacityMetrics);
    const auto ready = unready.find("} 1\n");
    ASSERT_NE(ready, std::string::npos);
    unready.replace(ready, 4, "} 0\n");
    EXPECT_FALSE(ParseKvcsEfcCapacityMetrics(unready, "target-mp"));
}

TEST(KvcsEfcMetricsCapacityTest, RejectsIncompleteOrInconsistentSamples) {
    constexpr std::string_view incomplete = R"(
kvcs_efc_storage_capacity_bytes{tier="g35",state="total",mountpoint_id="target-mp"} 100
kvcs_efc_storage_capacity_bytes{tier="g35",state="used",mountpoint_id="target-mp"} 25
)";
    EXPECT_FALSE(ParseKvcsEfcCapacityMetrics(incomplete, "target-mp"));

    constexpr std::string_view inconsistent = R"(
kvcs_efc_storage_capacity_bytes{tier="g35",state="total",mountpoint_id="target-mp"} 100
kvcs_efc_storage_capacity_bytes{tier="g35",state="used",mountpoint_id="target-mp"} 101
kvcs_efc_storage_ready{tier="g35",mountpoint_id="target-mp"} 1
)";
    EXPECT_FALSE(ParseKvcsEfcCapacityMetrics(inconsistent, "target-mp"));

    constexpr std::string_view malformed = R"(
kvcs_efc_storage_capacity_bytes{tier="g35",state="total",mountpoint_id="target-mp"} invalid
kvcs_efc_storage_capacity_bytes{tier="g35",state="used",mountpoint_id="target-mp"} 25
kvcs_efc_storage_ready{tier="g35",mountpoint_id="target-mp"} 1
)";
    EXPECT_FALSE(ParseKvcsEfcCapacityMetrics(malformed, "target-mp"));
}

TEST(KvcsEfcMetricsCapacityTest, ProviderUsesConfiguredEndpoint) {
    KvcsEfcMetricsCapacityProvider provider(
        "target-mp", "http://efc:9093/metrics", std::chrono::milliseconds(123),
        [](const std::string& url, std::chrono::milliseconds timeout)
            -> tl::expected<std::string, std::string> {
            EXPECT_EQ(url, "http://efc:9093/metrics");
            EXPECT_EQ(timeout, std::chrono::milliseconds(123));
            return std::string(kCapacityMetrics);
        });
    auto capacity = provider.GetCapacity();
    ASSERT_TRUE(capacity.has_value()) << capacity.error();
    EXPECT_EQ(capacity->used_bytes, 25u);
    EXPECT_EQ(capacity->capacity_bytes, 100u);
}

TEST(KvcsEfcMetricsCapacityTest, ProviderPropagatesFetchFailureAsUnknown) {
    KvcsEfcMetricsCapacityProvider provider(
        "target-mp", "http://efc:9093/metrics", std::chrono::milliseconds(123),
        [](const std::string&, std::chrono::milliseconds)
            -> tl::expected<std::string, std::string> {
            return tl::make_unexpected("timeout");
        });
    EXPECT_FALSE(provider.GetCapacity().has_value());

    KvcsEfcMetricsCapacityProvider throwing_provider(
        "target-mp", "http://efc:9093/metrics", std::chrono::milliseconds(123),
        [](const std::string&, std::chrono::milliseconds)
            -> tl::expected<std::string, std::string> {
            throw std::runtime_error("fetch failed");
        });
    EXPECT_FALSE(throwing_provider.GetCapacity().has_value());
}

TEST(KvcsObjectStorageAdapterTest, PutGetAndRemove) {
    auto adapter = MakeAdapter();
    ASSERT_TRUE(adapter.Init());

    std::string value = "12345678";
    iovec put_iov{value.data(), value.size()};
    ASSERT_TRUE(adapter.PutBatch({{"key", &put_iov, 1}})[0]);

    std::string output(value.size(), '\0');
    ASSERT_TRUE(adapter.GetBatch({{"key", output.data(), output.size()}})[0]);
    EXPECT_EQ(output, value);

    ASSERT_TRUE(adapter.Delete("key"));
    auto missing = adapter.GetBatch({{"key", output.data(), output.size()}});
    ASSERT_FALSE(missing[0]);
    EXPECT_EQ(missing[0].error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST(KvcsObjectStorageAdapterTest, RejectsOversizeBeforeProviderWrite) {
    auto driver = std::make_unique<MemoryDriver>();
    auto* driver_ptr = driver.get();
    KvcsObjectStorageAdapter adapter(std::move(driver));
    ASSERT_TRUE(adapter.Init());
    std::string value(9, 'x');
    iovec put_iov{value.data(), value.size()};
    auto result = adapter.PutBatch({{"large", &put_iov, 1}});
    ASSERT_FALSE(result[0]);
    EXPECT_EQ(result[0].error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(driver_ptr->put_calls(), 0u);
}

TEST(KvcsObjectStorageAdapterTest, DeleteKeepsPerKeyErrorsIsolated) {
    auto adapter = MakeAdapter();
    ASSERT_TRUE(adapter.Init());
    std::string value = "value";
    iovec put_iov{value.data(), value.size()};
    ASSERT_TRUE(adapter.PutBatch({{"valid", &put_iov, 1}})[0]);

    const std::array<std::string, 2> keys{"valid", std::string(300, 'x')};
    auto removed = adapter.DeleteBatch(keys);
    ASSERT_EQ(removed.size(), keys.size());
    EXPECT_TRUE(removed[0]);
    ASSERT_FALSE(removed[1]);
    EXPECT_EQ(removed[1].error(), ErrorCode::INVALID_PARAMS);
    std::string output(value.size(), '\0');
    auto missing = adapter.GetBatch({{"valid", output.data(), output.size()}});
    ASSERT_FALSE(missing[0]);
    EXPECT_EQ(missing[0].error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST(KvcsObjectStorageAdapterTest, GetRejectsMasterSizeMismatch) {
    auto adapter = MakeAdapter();
    ASSERT_TRUE(adapter.Init());
    std::string value = "value";
    iovec put_iov{value.data(), value.size()};
    ASSERT_TRUE(adapter.PutBatch({{"key", &put_iov, 1}})[0]);
    std::string output(value.size() - 1, '\0');
    auto result = adapter.GetBatch({{"key", output.data(), output.size()}});
    ASSERT_FALSE(result[0]);
    EXPECT_EQ(result[0].error(), ErrorCode::FILE_READ_FAIL);
}

TEST(KvcsObjectStorageAdapterTest, PutOverwritesAndKeepsErrorsIsolated) {
    auto adapter = MakeAdapter();
    ASSERT_TRUE(adapter.Init());
    std::string first = "one";
    iovec first_iov{first.data(), first.size()};
    ASSERT_TRUE(adapter.PutBatch({{"key", &first_iov, 1}})[0]);
    std::string replacement = "new";
    iovec replacement_iov{replacement.data(), replacement.size()};
    ASSERT_TRUE(adapter.PutBatch({{"key", &replacement_iov, 1}})[0]);
    std::string output(replacement.size(), '\0');
    ASSERT_TRUE(adapter.GetBatch({{"key", output.data(), output.size()}})[0]);
    EXPECT_EQ(output, replacement);

    std::string second = "two";
    iovec invalid_iov{nullptr, second.size()};
    iovec second_iov{second.data(), second.size()};
    const std::array<ObjectPutRequest, 2> requests{
        ObjectPutRequest{"second", &invalid_iov, 1},
        ObjectPutRequest{"second", &second_iov, 1}};
    auto invalid = adapter.PutBatch(
        std::vector<ObjectPutRequest>(requests.begin(), requests.end()));
    ASSERT_FALSE(invalid[0]);
    EXPECT_EQ(invalid[0].error(), ErrorCode::INVALID_PARAMS);
    ASSERT_TRUE(invalid[1]);
}

TEST(KvcsObjectStorageAdapterTest, RejectsInvalidRequests) {
    auto adapter = MakeAdapter();
    ASSERT_TRUE(adapter.Init());
    std::string value = "x";
    iovec put_iov{value.data(), value.size()};
    auto result = adapter.PutBatch({{"", &put_iov, 1}});
    ASSERT_FALSE(result[0]);
    EXPECT_EQ(result[0].error(), ErrorCode::INVALID_PARAMS);
}

TEST(KvcsObjectStorageAdapterTest, DefaultEfcTargetRejectsMultipleMountpoints) {
    ScopedEfcEnvironment environment;
    auto target = LoadKvcsEfcTarget("");
    ASSERT_TRUE(target);
    EXPECT_EQ(target->mountpoint_index, 1u);
    EXPECT_EQ(target->mountpoint_id, "kvcachestore-default");
    EXPECT_EQ(target->metrics_endpoint, "http://127.0.0.1:9093/metrics");
    setenv("MOONCAKE_KVCS_METRICS_ENDPOINT", "http://efc:9093/metrics", 1);
    target = LoadKvcsEfcTarget("");
    ASSERT_TRUE(target);
    EXPECT_EQ(target->metrics_endpoint, "http://efc:9093/metrics");
    setenv("KVCS_MOUNTPOINTS_JSON",
           R"({"mountPoints":[{"mountPointID":"one","subpath":"/tenant"}]})",
           1);
    target = LoadKvcsEfcTarget("");
    ASSERT_TRUE(target);
    EXPECT_EQ(target->mountpoint_id, "one");
    setenv("KVCS_MOUNTPOINTS_JSON",
           R"({"mountPoints":[{"mountPointID":"one"},{"mountPointID":"two"}]})",
           1);
    auto multiple = LoadKvcsEfcTarget("");
    ASSERT_FALSE(multiple);
    EXPECT_EQ(multiple.error(), ErrorCode::INVALID_PARAMS);
}

#ifdef MOONCAKE_KVCS_TEST_NO_SDK
TEST(KvcsObjectStorageAdapterTest, MissingSdkIsExplicitlyUnsupported) {
    ScopedEfcEnvironment environment;
    KvcsObjectStorageAdapter adapter;
    auto result = adapter.Init();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::NOT_SUPPORTED);
}
#endif

#ifdef MOONCAKE_KVCS_TEST_SDK
TEST(KvcsObjectStorageAdapterTest, OfficialMockSingleValueRoundTrip) {
    if (!std::getenv("MOONCAKE_KVCS_TEST_MOCK")) GTEST_SKIP();
    auto created = CreateKvcsLowLevelDriver(1);
    ASSERT_TRUE(created);
    auto& driver = **created;
    ASSERT_TRUE(driver.Init());
    std::string value = "value";
    const KvcsIoRequest put{.logical_key = "pr1-mock-roundtrip",
                            .slices = {{value.data(), value.size()}},
                            .size = value.size()};
    ASSERT_TRUE(driver.BatchPut({&put, 1})[0]);

    std::string output(value.size(), '\0');
    const KvcsIoRequest get{.logical_key = put.logical_key,
                            .slices = {{output.data(), output.size()}},
                            .size = output.size()};
    ASSERT_TRUE(driver.BatchGet({&get, 1})[0]);
    EXPECT_EQ(output, value);
    const std::array<ObjectKey, 1> keys{put.logical_key};
    ASSERT_TRUE(driver.BatchDelete(keys)[0]);
    auto after_delete = driver.BatchGet({&get, 1});
    ASSERT_FALSE(after_delete[0]);
    EXPECT_EQ(after_delete[0].error(), ErrorCode::OBJECT_NOT_FOUND);
}
#endif

}  // namespace
}  // namespace mooncake
