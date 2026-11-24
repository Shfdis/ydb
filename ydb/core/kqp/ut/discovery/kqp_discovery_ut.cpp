#include <ydb/core/discovery/discovery.h>

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NKqp;

namespace NKikimr::NDiscovery {

auto UnpackDiscoveryData(const TString& data) {
    Ydb::Discovery::ListEndpointsResponse leResponse;
    Ydb::Discovery::ListEndpointsResult leResult;
    auto ok = leResponse.ParseFromString(data);
    UNIT_ASSERT(ok);
    ok = leResponse.operation().result().UnpackTo(&leResult);
    UNIT_ASSERT(ok);
    return leResult;
}

void ValidateDiscovererResponse(TTestActorRuntime& runtime, TActorId edge) {
    auto discoveryDataEvent = runtime.GrabEdgeEvent<TEvDiscovery::TEvDiscoveryData>(edge)->Release();
    UNIT_ASSERT(discoveryDataEvent);

    auto discoveryData = UnpackDiscoveryData(discoveryDataEvent->CachedMessage);
    auto discoverySslData = UnpackDiscoveryData(discoveryDataEvent->CachedMessageSsl);

    UNIT_ASSERT_EQUAL(discoveryData.endpoints_size(), 1);
    UNIT_ASSERT_EQUAL(discoverySslData.endpoints_size(), 0);
}

Y_UNIT_TEST_SUITE(Discovery) {
    Y_UNIT_TEST(WatchEndpointBoard) {
        TKikimrRunner kikimr(TKikimrSettings{});
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        const auto edge = runtime.AllocateEdgeActor();
        const TString path = MakeEndpointsBoardPath("/Root/test-watch");
        const auto publisher = runtime.Register(CreateBoardPublishActor(path, "endpoint", edge, 0, true));
        const auto watch = runtime.Register(CreateDiscoverer([path](const TString&) { return path; },
            "/Root", false, edge, {}, true));
        using TData = TEvDiscovery::TEvDiscoveryData;
        UNIT_ASSERT(runtime.GrabEdgeEventIf<TData>(edge, [](const auto& ev) {
            return ev->Get()->InfoEntries.size() == 1;
        }, TDuration::Seconds(10)));
        runtime.Send(publisher, edge, new TEvents::TEvPoisonPill);
        auto empty = runtime.GrabEdgeEvent<TData>(edge, TDuration::Seconds(10));
        UNIT_ASSERT(empty && empty->Get()->InfoEntries.empty());
        UNIT_ASSERT_EQUAL(empty->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::Ok);
        runtime.Send(watch, edge, new TEvents::TEvPoisonPill);
    }

    Y_UNIT_TEST(DelayedNameserviceResponse) {
        TKikimrRunner kikimr(TKikimrSettings{});

        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto edge = runtime.AllocateEdgeActor();
 
        auto cache = runtime.Register(CreateDiscoveryCache({}, edge));
        auto discoverer = runtime.Register(CreateDiscoverer(&MakeEndpointsBoardPath, "/Root", true, edge, cache));
        Y_UNUSED(discoverer);

        auto getNodeEvent = runtime.GrabEdgeEvent<TEvInterconnect::TEvGetNode>(edge)->Release();

        runtime.Send(GetNameserviceActorId(), edge, getNodeEvent.Release());

        auto nodeInfoEvent = runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeInfo>(edge)->Release();

        Sleep(TDuration::Seconds(1));

        runtime.Send(cache, edge, nodeInfoEvent.Release());

        ValidateDiscovererResponse(runtime, edge);
    }

    Y_UNIT_TEST(SecondDiscovererRequestBeforeNameserviceResponse) {
        TKikimrRunner kikimr(TKikimrSettings{});

        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto edge = runtime.AllocateEdgeActor();

        auto discovererEdge1 = runtime.AllocateEdgeActor();
        auto discovererEdge2 = runtime.AllocateEdgeActor();

        auto cache = runtime.Register(CreateDiscoveryCache({}, edge));
        auto discoverer1 = runtime.Register(CreateDiscoverer(&MakeEndpointsBoardPath, "/Root", true, discovererEdge1, cache));
        Y_UNUSED(discoverer1);

        auto getNodeEvent = runtime.GrabEdgeEvent<TEvInterconnect::TEvGetNode>(edge)->Release();

        runtime.Send(GetNameserviceActorId(), edge, getNodeEvent.Release());

        auto nodeInfoEvent = runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeInfo>(edge)->Release();

        Sleep(TDuration::Seconds(1));

        auto discoverer2 = runtime.Register(CreateDiscoverer(&MakeEndpointsBoardPath, "/Root", true, discovererEdge2, cache));
        Y_UNUSED(discoverer2);

        Sleep(TDuration::Seconds(1));

        runtime.Send(cache, edge, nodeInfoEvent.Release());

        ValidateDiscovererResponse(runtime, discovererEdge1);
        ValidateDiscovererResponse(runtime, discovererEdge2);
    }
}

}
