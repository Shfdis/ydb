#include "grpc_request_proxy.h"

#include "rpc_calls.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/discovery/discovery.h>

#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <ydb/library/actors/core/hfunc.h>

#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr::NGRpcService {

using namespace NActors;
using namespace Ydb;

class TListEndpointsRPC : public TActorBootstrapped<TListEndpointsRPC> {
    THolder<TEvListEndpointsRequest> Request;
    const bool Streaming;
    size_t PendingWrites = 0;
    static constexpr size_t MaxPendingWrites = 8;
    const TActorId CacheId;
    TActorId Discoverer;

    bool SentSnapshot = false;

    NWilson::TSpan Span;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TListEndpointsRPC(TEvListEndpointsRequest::TPtr &msg, TActorId cacheId)
        : Request(msg->Release().Release())
        , Streaming(dynamic_cast<TEvStreamListEndpointsRequest*>(Request.Get()) != nullptr)
        , CacheId(cacheId)
        , Span(TWilsonGrpc::RequestActor, Request->GetWilsonTraceId(), "ListEndpointsRpc")
    {}

    void Bootstrap() {
        if (Streaming) {
            if (!AppData()->FeatureFlags.GetEnableSubscriptionsInDiscovery()) {
                Request->ReplyWithRpcStatus(grpc::StatusCode::UNIMPLEMENTED, "Endpoint subscriptions disabled", {});
                return PassAway();
            }
            // Re-resolve the database periodically, including its serverless resources domain.
            Schedule(TDuration::Minutes(1), new TEvents::TEvWakeup(1));
            auto* system = TActivationContext::ActorSystem();
            Request->SetFinishAction([system, self = SelfId()] { system->Send(self, new TEvents::TEvPoisonPill); });
            Request->SetStreamingNotify([system, self = SelfId()](size_t) { system->Send(self, new TEvents::TEvWakeup); });
        }
        Discoverer = Register(CreateDiscoverer(&MakeEndpointsBoardPath,
            Request->GetProtoRequest()->database(), Request->GetEndpointId().empty() && Request->GetProtoRequest()->Getservice().empty(),
            SelfId(), CacheId, Streaming));

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
            hFunc(TEvents::TEvWakeup, Handle);
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag) {
            if (SentSnapshot) {
                Request->FinishStream(Ydb::StatusIds::SUCCESS);
            } else {
                Request->ReplyWithRpcStatus(grpc::StatusCode::UNAVAILABLE, "Endpoint discovery timed out", {});
            }
            return PassAway();
        }
        --PendingWrites;
    }

    void Cancel() {
        Request->ReplyWithRpcStatus(grpc::StatusCode::CANCELLED, {}, {});
        PassAway();
    }

    void Handle(TEvDiscovery::TEvDiscoveryData::TPtr &ev) {
        if (!Streaming) {
            Discoverer = {};
        } else if (PendingWrites == MaxPendingWrites) {
            Request->ReplyWithRpcStatus(grpc::StatusCode::RESOURCE_EXHAUSTED, "Discovery reader fell behind", {});
            return PassAway();
        }
        auto* data = ev->Get();
        const bool ssl = Request->SslServer();
        TString message = ssl ? std::move(data->CachedMessageSsl) : std::move(data->CachedMessage);
        if (message.empty()) {
            const auto& services = Request->GetProtoRequest()->service();
            NDiscovery::TCachedMessageData filtered(std::move(data->InfoEntries), data->SelfLocation,
                data->BridgeInfo, Request->GetEndpointId(), {services.begin(), services.end()});
            message = filtered.GetMessage(ssl, Streaming);
        }
        SentSnapshot = true;
        Request->SendSerializedResult(std::move(message), Ydb::StatusIds::SUCCESS);
        if (Streaming) {
            ++PendingWrites;
        } else {
            PassAway();
        }
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

    void Reply(Ydb::StatusIds::StatusCode status) {
        Request->ReplyWithYdbStatus(status);
        PassAway();
    }
};

void TGRpcRequestProxy::Handle(TEvListEndpointsRequest::TPtr& ev, const TActorContext& ctx) {
    if (!DiscoveryCacheActorID) {
        DiscoveryCacheActorID = ctx.Register(CreateDiscoveryCache());
    }

    ctx.Register(new TListEndpointsRPC(ev, DiscoveryCacheActorID));
}

}
