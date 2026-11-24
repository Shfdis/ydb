#include <ydb/core/discovery/discovery.h>

#include <ydb/core/base/statestorage_impl.h>
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
        TKikimrRunner kikimr(TKikimrSettings{}.SetUseRealThreads(false));
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        const TString path = MakeEndpointsBoardPath("/Root/test-watch");
        const auto cache = runtime.Register(CreateDiscoveryCache());
        const auto first = runtime.AllocateEdgeActor();
        const auto second = runtime.AllocateEdgeActor();
        TVector<TActorId> lookups;
        auto observer = runtime.AddObserver<TEvStateStorage::TEvResolveBoard>([&](auto& ev) {
            if (ev->Get()->Subscribe && ev->Get()->Path == path) {
                lookups.push_back(ev->Sender);
            }
        });
        auto watch = [&](TActorId edge, bool serialize) {
            return runtime.Register(CreateDiscoverer([path](const TString&) { return path; },
                "/Root", serialize, edge, cache, true));
        };
        auto expectEndpoints = [&](TActorId edge, size_t count) {
            auto event = runtime.GrabEdgeEvent<TEvDiscovery::TEvDiscoveryData>(edge, TDuration::Seconds(10));
            UNIT_ASSERT(event);
            UNIT_ASSERT_EQUAL(event->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::Ok);
            UNIT_ASSERT_VALUES_EQUAL(event->Get()->HasEndpoints, count != 0);
            if (edge == first) {
                UNIT_ASSERT_VALUES_EQUAL(event->Get()->InfoEntries.size(), count);
            } else {
                UNIT_ASSERT(event->Get()->InfoEntries.empty());
                UNIT_ASSERT_VALUES_EQUAL(UnpackDiscoveryData(event->Get()->CachedMessage).endpoints_size(), count);
            }
        };
        const auto watch1 = watch(first, false);
        const auto watch2 = watch(second, true);
        expectEndpoints(first, 0);
        expectEndpoints(second, 0);
        UNIT_ASSERT_VALUES_EQUAL(lookups.size(), 1);
        observer.Remove();

        NKikimrStateStorage::TEndpointBoardEntry entry;
        entry.SetAddress("localhost");
        entry.SetPort(2135);
        auto publish = [&] {
            return runtime.Register(CreateBoardPublishActor(path, entry.SerializeAsString(), first, 0, true));
        };
        auto publisher = publish();
        expectEndpoints(first, 1);
        expectEndpoints(second, 1);
        auto blockCacheTick = runtime.AddObserver<TEvents::TEvWakeup>([&](auto& ev) {
            if (ev->Recipient == cache) {
                ev.Reset();
            }
        });
        runtime.Send(publisher, first, new TEvents::TEvPoisonPill);
        expectEndpoints(first, 0);
        expectEndpoints(second, 0);
        runtime.Send(watch1, first, new TEvents::TEvPoisonPill);
        publisher = publish();
        expectEndpoints(second, 1);

        runtime.Send(watch2, second, new TEvents::TEvPoisonPill);
        TDispatchOptions stopped;
        stopped.FinalEvents.emplace_back([&](IEventHandle& ev) {
            return ev.GetTypeRewrite() == TEvents::TEvPoison::EventType && ev.Recipient == lookups.front() && ev.Sender == cache;
        });
        UNIT_ASSERT(runtime.DispatchEvents(stopped, TDuration::Seconds(10)));
        runtime.Send(publisher, first, new TEvents::TEvPoisonPill);
        runtime.Send(cache, first, new TEvents::TEvPoisonPill);
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
