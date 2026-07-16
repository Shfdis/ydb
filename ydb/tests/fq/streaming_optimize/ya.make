PY3TEST()

<<<<<<< HEAD
FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(4)
=======
>>>>>>> b9c9193f06e ([YQ-5323] Watermarks: DQ: advanced mode (#45303))

TEST_SRCS(
    test_sql_negative.py
    test_sql_streaming.py
)

<<<<<<< HEAD
SIZE(MEDIUM)
REQUIREMENTS(cpu:2)
=======
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
    FORK_SUBTESTS()
ENDIF()
>>>>>>> b9c9193f06e ([YQ-5323] Watermarks: DQ: advanced mode (#45303))

INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/tools/solomon_emulator/recipe/recipe.inc)

DEPENDS(
    ydb/tests/tools/fqrun
    yql/essentials/tools/astdiff
    yql/essentials/tools/sql2yql
    yql/essentials/tests/common/test_framework/udfs_deps
)

DATA(
    arcadia/ydb/tests/fq/streaming_optimize/cfg
    arcadia/ydb/tests/fq/streaming_optimize/suites
)

PEERDIR(
    ydb/tests/fq/tools
    yql/essentials/tests/common/test_framework
)

END()
