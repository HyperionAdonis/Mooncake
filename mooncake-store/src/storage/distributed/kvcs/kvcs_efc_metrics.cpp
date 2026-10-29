#include "storage/distributed/kvcs/kvcs_efc_metrics.h"

#include <cmath>
#include <cstdlib>
#include <exception>
#include <limits>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "ascii_string.h"
#include "common/network.h"

namespace mooncake {
namespace {

struct PrometheusSample {
    std::string_view name;
    std::unordered_map<std::string, std::string> labels;
    std::string_view value;
};

tl::expected<PrometheusSample, std::string> ParseSample(std::string_view line) {
    line = TrimAsciiWhitespace(line);
    if (line.empty() || line.front() == '#')
        return tl::make_unexpected("ignored");

    const size_t name_end = line.find_first_of("{ \t");
    if (name_end == std::string_view::npos)
        return tl::make_unexpected("missing metric value");
    PrometheusSample sample{
        .name = line.substr(0, name_end), .labels = {}, .value = {}};
    size_t pos = name_end;
    if (line[pos] == '{') {
        ++pos;
        while (true) {
            while (pos < line.size() && (line[pos] == ' ' || line[pos] == '\t'))
                ++pos;
            if (pos == line.size())
                return tl::make_unexpected("unterminated labels");
            if (line[pos] == '}') {
                ++pos;
                break;
            }

            const size_t label_start = pos;
            while (pos < line.size() && line[pos] != '=' && line[pos] != ' ' &&
                   line[pos] != '\t')
                ++pos;
            const std::string label(
                line.substr(label_start, pos - label_start));
            while (pos < line.size() && (line[pos] == ' ' || line[pos] == '\t'))
                ++pos;
            if (label.empty() || pos + 1 >= line.size() || line[pos] != '=' ||
                line[pos + 1] != '"')
                return tl::make_unexpected("invalid label");
            pos += 2;

            std::string value;
            bool closed = false;
            while (pos < line.size()) {
                const char ch = line[pos++];
                if (ch == '"') {
                    closed = true;
                    break;
                }
                if (ch == '\\') {
                    if (pos == line.size())
                        return tl::make_unexpected("invalid label escape");
                    const char escaped = line[pos++];
                    if (escaped == 'n')
                        value.push_back('\n');
                    else if (escaped == '\\' || escaped == '"')
                        value.push_back(escaped);
                    else
                        return tl::make_unexpected("invalid label escape");
                } else {
                    value.push_back(ch);
                }
            }
            if (!closed ||
                !sample.labels.emplace(label, std::move(value)).second)
                return tl::make_unexpected("invalid or duplicate label");

            while (pos < line.size() && (line[pos] == ' ' || line[pos] == '\t'))
                ++pos;
            if (pos == line.size())
                return tl::make_unexpected("unterminated labels");
            if (line[pos] == ',') {
                ++pos;
                continue;
            }
            if (line[pos] == '}') {
                ++pos;
                break;
            }
            return tl::make_unexpected("invalid label separator");
        }
    }

    while (pos < line.size() && (line[pos] == ' ' || line[pos] == '\t')) ++pos;
    if (pos == line.size()) return tl::make_unexpected("missing metric value");
    const size_t value_end = line.find_first_of(" \t", pos);
    sample.value = line.substr(pos, value_end - pos);
    return sample;
}

tl::expected<long double, std::string> ParseNumber(std::string_view value) {
    const std::string token(value);
    char* end = nullptr;
    const long double parsed = std::strtold(token.c_str(), &end);
    if (end != token.c_str() + token.size() || !std::isfinite(parsed))
        return tl::make_unexpected("invalid metric value");
    return parsed;
}

bool HasLabel(const PrometheusSample& sample, std::string_view name,
              std::string_view value) {
    const auto it = sample.labels.find(std::string(name));
    return it != sample.labels.end() && it->second == value;
}

tl::expected<uint64_t, std::string> ToBytes(std::string_view token) {
    auto parsed = ParseNumber(token);
    if (!parsed) return tl::make_unexpected(parsed.error());
    const long double value = *parsed;
    long double integral = 0;
    if (value < 0 || std::modf(value, &integral) != 0 ||
        value > static_cast<long double>(std::numeric_limits<uint64_t>::max()))
        return tl::make_unexpected("capacity is not a non-negative integer");
    return static_cast<uint64_t>(value);
}

}  // namespace

tl::expected<DfsUsageSnapshot, std::string> ParseKvcsEfcCapacityMetrics(
    std::string_view metrics, std::string_view mountpoint_id) {
    if (mountpoint_id.empty())
        return tl::make_unexpected("mountpoint id is empty");

    std::optional<uint64_t> total;
    std::optional<uint64_t> used;
    std::optional<long double> ready;
    size_t start = 0;
    while (start <= metrics.size()) {
        const size_t end = metrics.find('\n', start);
        const auto line = metrics.substr(start, end == std::string_view::npos
                                                    ? metrics.size() - start
                                                    : end - start);
        start = end == std::string_view::npos ? metrics.size() + 1 : end + 1;

        auto parsed = ParseSample(line);
        if (!parsed) {
            if (parsed.error() == "ignored") continue;
            return tl::make_unexpected(parsed.error());
        }
        const auto& sample = *parsed;
        if (!HasLabel(sample, "tier", "g35") ||
            !HasLabel(sample, "mountpoint_id", mountpoint_id))
            continue;

        if (sample.name == "kvcs_efc_storage_capacity_bytes") {
            std::optional<uint64_t>* destination = nullptr;
            if (HasLabel(sample, "state", "total"))
                destination = &total;
            else if (HasLabel(sample, "state", "used"))
                destination = &used;
            else
                continue;
            if (destination->has_value())
                return tl::make_unexpected("duplicate capacity sample");
            auto bytes = ToBytes(sample.value);
            if (!bytes) return tl::make_unexpected(bytes.error());
            *destination = *bytes;
        } else if (sample.name == "kvcs_efc_storage_ready") {
            if (ready.has_value())
                return tl::make_unexpected("duplicate ready sample");
            auto value = ParseNumber(sample.value);
            if (!value) return tl::make_unexpected(value.error());
            ready = *value;
        }
    }

    if (!total || !used || !ready)
        return tl::make_unexpected("capacity metrics are incomplete");
    if (*ready != 1) return tl::make_unexpected("target is not ready");
    if (*total == 0 || *used > *total)
        return tl::make_unexpected("capacity metrics are inconsistent");
    return DfsUsageSnapshot{.enabled = true,
                            .used_bytes = *used,
                            .capacity_bytes = *total,
                            .file_count = 0};
}

KvcsEfcMetricsCapacityProvider::KvcsEfcMetricsCapacityProvider(
    std::string mountpoint_id, std::string metrics_endpoint,
    std::chrono::milliseconds timeout, Fetch fetch)
    : mountpoint_id_(std::move(mountpoint_id)),
      metrics_endpoint_(std::move(metrics_endpoint)),
      timeout_(timeout),
      fetch_(std::move(fetch)) {
    if (!fetch_) {
        fetch_ = [](const std::string& url, std::chrono::milliseconds timeout) {
            auto response = httpGet(url, timeout);
            if (!response) {
                return tl::expected<std::string, std::string>(
                    tl::make_unexpected("HTTP GET failed: " +
                                        std::to_string(response.error())));
            }
            return tl::expected<std::string, std::string>(std::move(*response));
        };
    }
}

tl::expected<DfsUsageSnapshot, std::string>
KvcsEfcMetricsCapacityProvider::GetCapacity() const {
    if (mountpoint_id_.empty() || metrics_endpoint_.empty() ||
        timeout_.count() <= 0)
        return tl::make_unexpected("invalid EFC metrics capacity config");
    try {
        auto metrics = fetch_(metrics_endpoint_, timeout_);
        if (!metrics) return tl::make_unexpected(metrics.error());
        return ParseKvcsEfcCapacityMetrics(*metrics, mountpoint_id_);
    } catch (const std::exception& error) {
        return tl::make_unexpected(std::string("metrics fetch failed: ") +
                                   error.what());
    } catch (...) {
        return tl::make_unexpected("metrics fetch failed");
    }
}

}  // namespace mooncake
