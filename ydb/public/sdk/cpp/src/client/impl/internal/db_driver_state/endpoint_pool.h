#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/internal/internal_header.h>

#include <ydb/public/api/protos/ydb_discovery.pb.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/common/balancing_policies.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/internal_client/client.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/plain_status/status.h>
#include <ydb/public/sdk/cpp/src/client/impl/endpoints/endpoints.h>

#include <library/cpp/threading/future/future.h>
#include <util/generic/is_in.h>

#include <mutex>

namespace NYdb::inline Dev {

struct TListEndpointsResult {
    Ydb::Discovery::ListEndpointsResult Result;
    TPlainStatus DiscoveryStatus;

    bool SupportsLongPolling() const {
        return !Result.generation().empty() && IsIn(Result.features(), Ydb::Discovery::ListEndpointsResult::LONG_POLLING);
    }
};

using TAsyncListEndpointsResult = NThreading::TFuture<TListEndpointsResult>;

struct TEndpointUpdateResult {
    std::vector<std::string> Removed;
    TPlainStatus DiscoveryStatus;
};

class TEndpointPool {
public:
    explicit TEndpointPool(TDbDriverState& state);
    ~TEndpointPool();
    std::pair<NThreading::TFuture<TEndpointUpdateResult>, bool> UpdateAsync(const std::string& fromGeneration = {});
    TEndpointRecord GetEndpoint(const TEndpointKey& preferredEndpoint, bool onlyPreferred = false) const;
    TDuration TimeSinceLastUpdate() const;
    void BanEndpoint(const std::string& endpoint);
    void BanNodeId(std::uint64_t nodeId);
    int GetPessimizationRatio();
    bool LinkObjToEndpoint(const TEndpointKey& endpoint, TEndpointObj* obj, const void* tag);
    void ForEachEndpoint(const TEndpointElectorSafe::THandleCb& cb, const void* tag) const;
    void ForEachLocalEndpoint(const TEndpointElectorSafe::THandleCb& cb, const void* tag) const;
    void ForEachForeignEndpoint(const TEndpointElectorSafe::THandleCb& cb, const void* tag) const;
    TBalancingPolicy::TImpl::EPolicyType GetBalancingPolicyType() const;
    // TODO: Remove this mess
    void SetStatCollector(NSdkStats::TStatCollector& statCollector);
    static constexpr std::int32_t GetLocalityShift();

private:
    bool IsPreferredEndpoint(const Ydb::Discovery::EndpointInfo& endpoint,
                             const std::string& selfLocation,
                             const std::unordered_map<std::string, Ydb::Bridge::PileState>& pileStates) const;
    EPileState GetPileState(const Ydb::Bridge::PileState::State& state) const;

private:
    TDbDriverState& State_;
    std::atomic<bool> PollScheduled_{false};
    std::mutex Mutex_;
    TEndpointElectorSafe Elector_;
    NThreading::TPromise<TEndpointUpdateResult> DiscoveryPromise_;
    std::atomic_uint64_t LastUpdateTime_;
    const TBalancingPolicy::TImpl BalancingPolicy_;

    NSdkStats::TStatCollector* StatCollector_ = nullptr;

    // Max, min load factor returned by discovery service
    static constexpr float LoadMax = 100.0;
    static constexpr float LoadMin = -100.0;
    // Is used to convert float to integer load factor
    // same integer values will be selected randomly.
    static constexpr float Multiplicator = 10.0;
};

} // namespace NYdb
