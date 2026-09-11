UNITTEST()

SIZE(SMALL)
FORK_SUBTESTS()

PEERDIR(
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/topic/impl
)

SRCS(
    codecs_ut.cpp
    producer_close_deadline_ut.cpp
)

END()
