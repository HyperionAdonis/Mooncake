#pragma once

#include <chrono>
#include <functional>
#include <string>
#include <string_view>

#include <ylt/util/tl/expected.hpp>

#include "segment/usage.h"

namespace mooncake {

// TODO(kvcs): This Prometheus scrape is a temporary HTTP bridge
// until KVCS exposes target capacity through its public Low Level API. Keep
// Prometheus details behind this provider. Its output uses Mooncake's existing
// DFS usage snapshot rather than introducing another capacity model. This
// change only publishes the snapshot; the follow-up watermark path must reuse
// the existing DFS configuration and eviction task. A failed or malformed
// scrape means capacity is unknown and must never trigger metadata changes.
class KvcsEfcMetricsCapacityProvider {
   public:
    using Fetch = std::function<tl::expected<std::string, std::string>(
        const std::string&, std::chrono::milliseconds)>;

    KvcsEfcMetricsCapacityProvider(
        std::string mountpoint_id, std::string metrics_endpoint,
        std::chrono::milliseconds timeout = std::chrono::seconds(2),
        Fetch fetch = {});

    tl::expected<DfsUsageSnapshot, std::string> GetCapacity() const;

   private:
    std::string mountpoint_id_;
    std::string metrics_endpoint_;
    std::chrono::milliseconds timeout_;
    Fetch fetch_;
};

tl::expected<DfsUsageSnapshot, std::string> ParseKvcsEfcCapacityMetrics(
    std::string_view metrics, std::string_view mountpoint_id);

}  // namespace mooncake
