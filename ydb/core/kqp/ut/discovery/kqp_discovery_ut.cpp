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
    Y_UNIT_TEST(LongPollDeadlineAndBoardUpdate) {
        TKikimrRunner kikimr(TKikimrSettings{}.SetUseRealThreads(false));
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        const auto edge = runtime.AllocateEdgeActor();
        const auto cache = runtime.Register(CreateDiscoveryCache({}, edge));
        runtime.EnableScheduleForActor(cache);
        auto getNode = runtime.GrabEdgeEvent<TEvInterconnect::TEvGetNode>(edge)->Release();
        runtime.Send(GetNameserviceActorId(), edge, getNode.Release());
        auto node = runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeInfo>(edge)->Release();
        runtime.Send(cache, edge, node.Release());

        const auto path = MakeEndpointsBoardPath("/Root/long-poll-test");
        const auto lookup = [&](TMonotonic deadline) {
            runtime.Register(CreateDiscoverer([path](const TString&) { return path; },
                "/Root", true, edge, cache, deadline));
        };
        using TBoard = TEvStateStorage::TEvBoardInfo;
        auto board = MakeHolder<TBoard>(TBoard::EStatus::Ok, path);
        NKikimrStateStorage::TEndpointBoardEntry endpoint;
        endpoint.SetAddress("a");
        endpoint.SetPort(2135);
        board->InfoEntries[edge].Payload = endpoint.SerializeAsString();
        runtime.Send(cache, edge, board.Release());

        const auto deadline = runtime.GetCurrentMonotonicTime() + TDuration::Seconds(2);
        lookup(deadline);
        auto reply = runtime.GrabEdgeEvent<TEvDiscovery::TEvDiscoveryData>(edge, TDuration::Seconds(10));
        UNIT_ASSERT(reply);
        UNIT_ASSERT(runtime.GetCurrentMonotonicTime() >= deadline);
        auto result = UnpackDiscoveryData(reply->Get()->CachedMessage);
        UNIT_ASSERT(result.allow_long_polling());
        UNIT_ASSERT_VALUES_EQUAL(result.endpoints(0).address(), "a");

        lookup(runtime.GetCurrentMonotonicTime() + TDuration::Seconds(30));
        UNIT_ASSERT(!runtime.GrabEdgeEvent<TEvDiscovery::TEvDiscoveryData>(edge, TDuration::MilliSeconds(150)));
        auto update = MakeHolder<TEvStateStorage::TEvBoardInfoUpdate>(TBoard::EStatus::Ok, path);
        update->Updates[edge].Dropped = true;
        runtime.Send(cache, edge, update.Release());
        reply = runtime.GrabEdgeEvent<TEvDiscovery::TEvDiscoveryData>(edge, TDuration::Seconds(10));
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL(UnpackDiscoveryData(reply->Get()->CachedMessage).endpoints_size(), 0);
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
