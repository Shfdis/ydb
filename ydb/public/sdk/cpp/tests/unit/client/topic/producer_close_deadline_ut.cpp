#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/producer.h>

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>

using namespace NYdb;
using namespace NYdb::NTopic;

namespace NYdb::inline Dev::NTopic {

struct TProducerMessageInfoTestHelper {
    static bool ShutdownBeforeDeadlineIsInstalled(TProducer& producer) {
        // Take the real worker token after earlier callbacks finish. Unlike
        // TryAcquireMainWorker, failed attempts do not request extra passes.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        std::uint8_t expected = TProducer::Idle;
        while (!producer.MainWorkerState.compare_exchange_weak(
                expected, TProducer::Running, std::memory_order_acq_rel, std::memory_order_acquire)) {
            UNIT_ASSERT_C(std::chrono::steady_clock::now() < deadline,
                "The producer worker did not become idle");
            expected = TProducer::Idle;
            std::this_thread::yield();
        }

        bool ownsWorker = true;
        bool cleanedUp = false;
        const auto cleanup = [&] {
            if (ownsWorker) {
                producer.MainWorkerState.store(TProducer::Idle, std::memory_order_release);
                ownsWorker = false;
            }
            producer.SetCloseDeadline(TDuration::Zero());
            producer.NonBlockingClose();
            producer.RunMainWorker(-1);
            return producer.ShutdownFuture.Wait(TDuration::Seconds(10));
        };
        Y_SCOPE_EXIT(&) {
            if (!cleanedUp) {
                cleanup();
            }
        };

        {
            std::lock_guard lock(producer.GlobalLock);
            UNIT_ASSERT(!producer.MessagesWorker->IsQueueEmpty());
            UNIT_ASSERT(!producer.ShutdownFuture.HasValue());
            // This is the state between the first two steps of Close(): the
            // closed flag is visible, but SetCloseDeadline has not run yet.
            UNIT_ASSERT(!producer.Closed.exchange(true));
        }
        producer.RunMainWorkerAcquired(-1);
        ownsWorker = false;
        const bool shutdown = producer.ShutdownFuture.HasValue();

        // Finish the staged close before asserting, including in the negative
        // control where the old initial deadline already resolved shutdown.
        const bool stopped = cleanup();
        cleanedUp = true;
        UNIT_ASSERT_C(stopped, "The producer worker did not finish cleanup");
        return shutdown;
    }
};

} // namespace NYdb::inline Dev::NTopic

namespace {

class TTopicService final : public Ydb::Topic::V1::TopicService::Service {
public:
    grpc::Status DescribeTopic(
            grpc::ServerContext*,
            const Ydb::Topic::DescribeTopicRequest*,
            Ydb::Topic::DescribeTopicResponse* response) override {
        Ydb::Topic::DescribeTopicResult result;
        auto* partition = result.add_partitions();
        partition->set_partition_id(0);
        partition->set_active(true);
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        operation->mutable_result()->PackFrom(result);
        return grpc::Status::OK;
    }

    grpc::Status StreamWrite(
            grpc::ServerContext*,
            grpc::ServerReaderWriter<Ydb::Topic::StreamWriteMessage::FromServer,
                                     Ydb::Topic::StreamWriteMessage::FromClient>* stream) override {
        Ydb::Topic::StreamWriteMessage::FromClient request;
        while (stream->Read(&request)) {
            Ydb::Topic::StreamWriteMessage::FromServer response;
            response.set_status(Ydb::StatusIds::SUCCESS);
            if (request.has_init_request()) {
                auto* init = response.mutable_init_response();
                init->set_session_id("close-deadline-test");
                init->set_partition_id(0);
                init->mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_RAW);
            } else if (request.has_write_request()) {
                ReceivedWrite.TrySetValue();
                // Keep the real message in flight for the entire observation.
                continue;
            } else {
                response.mutable_update_token_response();
            }
            if (!stream->Write(response)) {
                break;
            }
        }
        return grpc::Status::OK;
    }

    NThreading::TPromise<void> ReceivedWrite = NThreading::NewPromise<void>();
};

class TServer {
public:
    TServer() {
        grpc::ServerBuilder builder;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &Port);
        builder.RegisterService(&Service);
        Server = builder.BuildAndStart();
        UNIT_ASSERT(Server);
    }

    ~TServer() {
        Server->Shutdown(std::chrono::system_clock::now());
        Server->Wait();
    }

    TDriver MakeDriver() const {
        return TDriver(TDriverConfig()
            .SetEndpoint("127.0.0.1:" + std::to_string(Port))
            .SetDatabase("/Root")
            .SetDiscoveryMode(EDiscoveryMode::Off));
    }

    TTopicService Service;

private:
    int Port = 0;
    std::unique_ptr<grpc::Server> Server;
};

} // namespace

Y_UNIT_TEST_SUITE(ProducerCloseDeadline) {
    Y_UNIT_TEST(UnsetDeadlineDoesNotCloseWithUnacknowledgedMessage) {
        TServer server;
        auto driver = server.MakeDriver();
        TTopicClient client(driver);
        TProducerSettings settings;
        settings.Path("topic");
        settings.ProducerIdPrefix("producer");
        settings.Codec(ECodec::RAW);
        settings.AsyncExecutionMode(false);
        auto producer = std::static_pointer_cast<TProducer>(client.CreateProducer(settings));
        auto message = TWriteMessage("message");
        message.SeqNo(1);
        UNIT_ASSERT(producer->Write(std::move(message)).Status == EWriteStatus::Queued);
        UNIT_ASSERT(server.Service.ReceivedWrite.GetFuture().Wait(TDuration::Seconds(10)));

        const bool shutdown = TProducerMessageInfoTestHelper::ShutdownBeforeDeadlineIsInstalled(*producer);
        producer.reset();
        UNIT_ASSERT_C(!shutdown, "The unset close deadline stopped a producer with an unacknowledged message");
    }
}
