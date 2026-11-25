#include "grpc_request_proxy.h"

#include "rpc_calls.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/location.h>
#include <ydb/core/discovery/discovery.h>
#include <ydb/core/util/wilson.h>
#include <ydb/library/wilson_ids/wilson.h>

#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NGRpcService {

using namespace NActors;
using namespace Ydb;

class TListEndpointsRPC : public TActorBootstrapped<TListEndpointsRPC> {
    THolder<TEvListEndpointsRequest> Request;
    const TActorId CacheId;
    TActorId Discoverer;

    NWilson::TSpan Span;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TListEndpointsRPC(TEvListEndpointsRequest::TPtr &msg, TActorId cacheId)
        : Request(msg->Release().Release())
        , CacheId(cacheId)
        , Span(TWilsonGrpc::RequestActor, Request->GetWilsonTraceId(), "ListEndpointsRpc")
    {}

    void Bootstrap() {
        const auto& request = *Request->GetProtoRequest();
        NDiscovery::TListEndpointsOptions options;
        options.EndpointId = Request->GetEndpointId();
        options.Services.insert(request.service().begin(), request.service().end());
        options.Ssl = Request->SslServer();
        if (AppData()->FeatureFlags.GetEnableSubscriptionsInDiscovery() && !request.snapshot_hash().empty()) {
            options.SnapshotHash = request.snapshot_hash();
            Request->SetFinishAction([system = TActivationContext::ActorSystem(), self = SelfId()] {
                system->Send(self, new TEvents::TEvPoisonPill);
            });
        }
        // request endpoints
        Discoverer = Register(CreateDiscoverer(&MakeEndpointsBoardPath,
            request.database(), true, SelfId(), CacheId, std::move(options)));

        Become(&TThis::StateWait);
    }

    void PassAway() override {
        if (Discoverer) {
            Send(Discoverer, new TEvents::TEvPoisonPill());
        }
        Span.EndOk();

        TActorBootstrapped<TListEndpointsRPC>::PassAway();
    }

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDiscovery::TEvDiscoveryData, Handle);
            hFunc(TEvDiscovery::TEvError, Handle);
            sFunc(TEvents::TEvPoison, Cancel);
        }
    }

    void Cancel() {
        Request->ReplyWithRpcStatus(grpc::StatusCode::CANCELLED, {}, {});
        PassAway();
    }

    void Handle(TEvDiscovery::TEvDiscoveryData::TPtr &ev) {
        Discoverer = {};
        auto* result = ev->Get();
        ReplySerialized(Request->SslServer() ? std::move(result->CachedMessageSsl) : std::move(result->CachedMessage),
            Ydb::StatusIds::SUCCESS);
    }

    void Handle(TEvDiscovery::TEvError::TPtr &ev) {
        Discoverer = {};

        auto issue = MakeIssue(ErrorToIssueCode(ev->Get()->Status), ev->Get()->Error);
        Request->RaiseIssue(issue);
        Reply(ErrorToStatusCode(ev->Get()->Status));
    }

    static NKikimrIssues::TIssuesIds::EIssueCode ErrorToIssueCode(TEvDiscovery::TEvError::EStatus status) {
        switch (status) {
            case TEvDiscovery::TEvError::KEY_PARSE_ERROR: return NKikimrIssues::TIssuesIds::KEY_PARSE_ERROR;
            case TEvDiscovery::TEvError::RESOLVE_ERROR: return NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR;
            case TEvDiscovery::TEvError::DATABASE_NOT_EXIST: return NKikimrIssues::TIssuesIds::DATABASE_NOT_EXIST;
            case TEvDiscovery::TEvError::ACCESS_DENIED: return NKikimrIssues::TIssuesIds::ACCESS_DENIED;
            default: return NKikimrIssues::TIssuesIds::DEFAULT_ERROR;
        }
    }

    static Ydb::StatusIds::StatusCode ErrorToStatusCode(TEvDiscovery::TEvError::EStatus status) {
        switch (status) {
            case TEvDiscovery::TEvError::KEY_PARSE_ERROR: return Ydb::StatusIds::BAD_REQUEST;
            case TEvDiscovery::TEvError::RESOLVE_ERROR: return Ydb::StatusIds::UNAVAILABLE;
            case TEvDiscovery::TEvError::DATABASE_NOT_EXIST: return Ydb::StatusIds::NOT_FOUND;
            case TEvDiscovery::TEvError::ACCESS_DENIED: return Ydb::StatusIds::NOT_FOUND;
            default: return Ydb::StatusIds::BAD_REQUEST;
        }
    }

    void ReplySerialized(TString message, Ydb::StatusIds::StatusCode status) {
        Request->SendSerializedResult(std::move(message), status);
        PassAway();
    }

    void Reply(Ydb::StatusIds::StatusCode status) {
        Request->ReplyWithYdbStatus(status);
        PassAway();
    }
};

TActorId RegisterDiscoveryResetService(const TActorContext& ctx, bool servesLongPolls) {
    auto* system = ctx.ActorSystem();
    // All discovery frontends use the primary request proxy on this node.
    if (!AppData(ctx)->FeatureFlags.GetEnableDiscoveryReset() ||
        system->LookupLocalService(CreateGRpcRequestProxyId()) != ctx.SelfID) {
        return {};
    }
    const auto cache = servesLongPolls ? ctx.Register(CreateDiscoveryCache()) : TActorId{};
    system->RegisterLocalService(MakeDiscoveryResetServiceID(ctx.SelfID.NodeId()),
        ctx.Register(CreateDiscoveryResetService(cache)));
    return cache;
}

void TGRpcRequestProxy::Handle(TEvListEndpointsRequest::TPtr& ev, const TActorContext& ctx) {
    if (!DiscoveryCacheActorID) {
        DiscoveryCacheActorID = ctx.Register(CreateDiscoveryCache());
    }

    ctx.Register(new TListEndpointsRPC(ev, DiscoveryCacheActorID));
}

}
