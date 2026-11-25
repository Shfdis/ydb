#pragma once

#include "json_handlers.h"
#include "json_pipe_req.h"

#include <ydb/core/base/nameservice.h>
#include <ydb/core/discovery/discovery.h>
#include <library/cpp/json/json_writer.h>

namespace NKikimr::NViewer {

class TResetDiscovery : public TViewerPipeClient {
    TSet<ui32> Pending;
    TSet<ui32> Failed;
    ui64 ResetPolls = 0;

public:
    using TViewerPipeClient::ReplyAndPassAway;

    TResetDiscovery(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev, "/viewer/reset_discovery_streams")
    {}

    void Bootstrap() override {
        if (Event->Get()->Request.GetMethod() != HTTP_METHOD_POST) {
            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "POST required"));
        }
        if (AppData()->AdministrationAllowedSIDs.empty() || !Viewer->CheckAccessAdministration(GetRequest())) {
            return ReplyAndPassAway(GETHTTPACCESSDENIED("text/plain", "Cluster administrator required"));
        }
        Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
        Become(&TResetDiscovery::StateWork, Timeout, new TEvents::TEvWakeup());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(TEvDiscovery::TEvResetEndpointsResult, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            sFunc(TEvents::TEvWakeup, TimeoutExpired);
        }
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        for (const auto& node : ev->Get()->Nodes) {
            Pending.insert(node.NodeId);
            Send(MakeDiscoveryResetServiceID(node.NodeId), new TEvDiscovery::TEvResetEndpoints(),
                IEventHandle::FlagTrackDelivery, node.NodeId);
        }
        if (Pending.empty()) {
            TimeoutExpired();
        }
    }

    void Handle(TEvDiscovery::TEvResetEndpointsResult::TPtr& ev) {
        if (ev->Sender.NodeId() == ev->Cookie) {
            CompleteNode(ev->Cookie, ev->Get());
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        CompleteNode(ev->Cookie, nullptr);
    }

    void CompleteNode(ui32 node, const TEvDiscovery::TEvResetEndpointsResult* reply) {
        if (!Pending.erase(node)) {
            return;
        }
        if (reply && reply->Record.HasResetPolls()) {
            ResetPolls += reply->Record.GetResetPolls();
        } else {
            Failed.insert(node);
        }
        if (Pending.empty()) {
            ReplyAndPassAway();
        }
    }

    void TimeoutExpired() {
        Failed.insert(Pending.begin(), Pending.end());
        ReplyAndPassAway(GetHTTPGATEWAYTIMEOUT("application/json", NJson::WriteJson(Result(), false)));
    }

    NJson::TJsonValue Result() const {
        NJson::TJsonValue result;
        result["reset_polls"] = ResetPolls;
        for (ui32 node : Failed) {
            result["unconfirmed_nodes"].AppendValue(node);
        }
        return result;
    }

    void ReplyAndPassAway() override {
        const auto result = Result();
        ReplyAndPassAway(Failed.empty() ? GetHTTPOKJSON(result)
            : GetHTTPINTERNALERROR("application/json", NJson::WriteJson(result, false)));
    }

    static YAML::Node GetSwagger() {
        return TSimpleYamlBuilder({
            .Method = "post",
            .Tag = "viewer",
            .Summary = "Complete pending discovery long polls",
            .Description = "Send an empty ListEndpoints result to pending long polls on all nodes.",
        });
    }
};

} // namespace NKikimr::NViewer
