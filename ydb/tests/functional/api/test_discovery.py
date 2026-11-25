# -*- coding: utf-8 -*-
import abc
from contextlib import ExitStack
import six
import time
import logging

import grpc
import pytest
import requests
from hamcrest import assert_that, is_, not_

from ydb.public.api.grpc.ydb_discovery_v1_pb2_grpc import DiscoveryServiceStub
from ydb.public.api.protos import ydb_discovery_pb2 as discovery
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common import types
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.oss.ydb_sdk_import import ydb


logger = logging.getLogger(__name__)


def wait_until_discovery_includes_all_slot_ports(
    cluster,
    resolver,
    timeout_seconds=30,
    step_seconds=0.5,
    message=None,
):
    """Wait until ListEndpoints returns every tenant slot gRPC port (handles board publish lag)."""
    required = {slot.grpc_port for slot in cluster.slots.values()}
    last_observed = {"ports": None, "debug_details": None}

    def capture_debug_details():
        debug_details_getter = getattr(resolver, "debug_details", None)
        if not callable(debug_details_getter):
            return
        try:
            last_observed["debug_details"] = debug_details_getter()
        except Exception as e:
            last_observed["debug_details"] = "debug_details() failed: %r" % (e,)

    def predicate():
        try:
            resolved = resolver.resolve()
            if resolved is None:
                last_observed["ports"] = None
                return False

            ports = sorted(endpoint.port for endpoint in resolved.endpoints)
            last_observed["ports"] = ports
            return required <= set(ports)
        finally:
            # Capture debug_details after resolve() so the failure message
            # reflects the most recent attempt (resolve() itself appends details).
            capture_debug_details()

    assert_that(
        wait_for(predicate, timeout_seconds=timeout_seconds, step_seconds=step_seconds),
        is_(True),
        message
        or (
            "ListEndpoints should include every tenant slot port; "
            "expected %s, last resolved %s, resolver details %r"
            % (sorted(required), last_observed["ports"], last_observed["debug_details"])
        ),
    )


def _long_poll_snapshot(reply):
    assert reply.operation.status == StatusIds.SUCCESS
    result = discovery.ListEndpointsResult()
    assert reply.operation.result.Unpack(result)
    assert result.snapshot_hash or not result.endpoints
    return result


@pytest.mark.parametrize("enabled", [False, True])
def test_reset_discovery_long_polls(enabled):
    admin = {"Authorization": "root@builtin"}
    config = KikimrConfigGenerator(
        nodes=2, erasure=types.Erasure.NONE, default_clusteradmin="root@builtin",
        extra_feature_flags=["enable_discovery_long_polling"] if enabled else [],
    )
    config.yaml_config["domains_config"]["security_config"].setdefault("viewer_allowed_sids", []).append("reader@builtin")
    cluster = KiKiMR(config)
    cluster.start()
    try:
        node = cluster.nodes[1]
        url = f"http://{node.host}:{node.mon_port}/viewer/reset_discovery_streams"
        with ExitStack() as channels:
            ports = {frontend.grpc_port for frontend in cluster.nodes.values()}
            rpcs = [DiscoveryServiceStub(channels.enter_context(grpc.insecure_channel(frontend.endpoint))).ListEndpoints
                    for frontend in cluster.nodes.values()]
            ordinary = discovery.ListEndpointsRequest(database="/Root")

            def all_ports_present():
                for rpc in rpcs:
                    try:
                        reply = rpc(ordinary, timeout=5)
                    except grpc.RpcError:
                        return False
                    result = discovery.ListEndpointsResult()
                    if reply.operation.status != StatusIds.SUCCESS or not reply.operation.result.Unpack(result):
                        return False
                    if not ports <= {ep.port for ep in result.endpoints}:
                        return False
                return True

            assert wait_for(all_ports_present, timeout_seconds=20, step_seconds=0.2)
            if not enabled:
                for rpc in rpcs:
                    for snapshot_hash in (b"", b"previous"):
                        reply = rpc(discovery.ListEndpointsRequest(database="/Root", snapshot_hash=snapshot_hash), timeout=5)
                        result = discovery.ListEndpointsResult()
                        assert reply.operation.status == StatusIds.SUCCESS and reply.operation.result.Unpack(result)
                        assert ports <= {ep.port for ep in result.endpoints}
                        assert not result.snapshot_hash
                assert requests.post(url, headers=admin, timeout=5).status_code == 404
                return

            hashes = [_long_poll_snapshot(rpc(ordinary, timeout=5)).snapshot_hash for rpc in rpcs]
            polls = []
            for index, rpc in enumerate(rpcs):
                request = discovery.ListEndpointsRequest(database="/Root", snapshot_hash=b"stale")
                assert _long_poll_snapshot(rpc(request, timeout=5)).snapshot_hash == hashes[index]
                request.snapshot_hash = hashes[1 - index]
                if request.snapshot_hash != hashes[index]:
                    # This node returns a different client-visible snapshot.
                    assert _long_poll_snapshot(rpc(request, timeout=5)).snapshot_hash == hashes[index]
                    request.snapshot_hash = hashes[index]
                polls.append((rpc, rpc.future(request, timeout=25), hashes[index]))
            with pytest.raises(grpc.RpcError) as expired:
                rpcs[-1](request, timeout=0.3)
            assert expired.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
            assert requests.get(url, headers=admin, timeout=5).status_code == 400
            assert requests.post(url, headers={"Authorization": "reader@builtin"}, timeout=5).status_code == 403
            for _, poll, _ in polls:
                with pytest.raises(grpc.FutureTimeoutError):
                    poll.result(timeout=0.5)

            response = requests.post(url, headers=admin, timeout=10)
            assert response.status_code == 200, response.text
            assert response.json()["reset_polls"] == len(polls)
            for rpc, poll, snapshot_hash in polls:
                reset = _long_poll_snapshot(poll.result(timeout=5))
                assert not reset.endpoints
                assert not reset.snapshot_hash
                refreshed = _long_poll_snapshot(rpc(ordinary, timeout=5))
                assert ports <= {ep.port for ep in refreshed.endpoints}
                assert refreshed.snapshot_hash == snapshot_hash
            assert requests.post(url, headers=admin, timeout=5).json()["reset_polls"] == 0
    finally:
        cluster.stop()


class TestDiscoveryExtEndpoint(object):
    @classmethod
    def setup_class(cls):
        conf = KikimrConfigGenerator()
        cls.ext_port_1 = conf.port_allocator.get_node_port_allocator(0).ext_port
        cls.ext_port_2 = conf.port_allocator.get_node_port_allocator(1).ext_port
        conf.clone_grpc_as_ext_endpoint(cls.ext_port_1, "extserv1")
        conf.clone_grpc_as_ext_endpoint(cls.ext_port_2, "extserv2")
        cls.cluster = KiKiMR(
            configurator=conf
        )
        cls.cluster.start()
        cls.database_name = '/Root/database'
        cls.logger = logger.getChild(cls.__name__)
        cls.cluster.create_database(
            cls.database_name,
            storage_pool_units_count={
                'hdd': 1
            }
        )
        cls.cluster.register_and_start_slots(cls.database_name, count=2)
        cls.cluster.wait_tenant_up(cls.database_name)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def test_scenario(self):
        ext_port_1 = TestDiscoveryExtEndpoint.ext_port_1
        ext_port_2 = TestDiscoveryExtEndpoint.ext_port_2
        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port), self.database_name)
        resolver = ydb.DiscoveryEndpointsResolver(driver_config)
        driver = ydb.Driver(driver_config)
        driver.wait(timeout=10)

        wait_until_discovery_includes_all_slot_ports(
            self.cluster,
            resolver,
            message="ListEndpoints via default gRPC should include every tenant slot port; missing after wait",
        )

        endpoint_ports = [endpoint.port for endpoint in resolver.resolve().endpoints]
        # Discovery has been performed using default endpoint
        # but ext endpoint marked with label
        # such ext endpoint should not present in discovery
        assert_that(ext_port_1 not in endpoint_ports)
        assert_that(ext_port_2 not in endpoint_ports)

        for slot in self.cluster.slots.values():
            assert_that(slot.grpc_port in endpoint_ports)
            assert_that(slot.grpc_port != ext_port_1)
            assert_that(slot.grpc_port != ext_port_2)

        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, ext_port_1), self.database_name)
        resolver = ydb.DiscoveryEndpointsResolver(driver_config)
        driver = ydb.Driver(driver_config)
        driver.wait(timeout=10)

        endpoint_ports = [endpoint.port for endpoint in resolver.resolve().endpoints]
        # Discovery has been performed using external endpoint with label
        # only endpoint with such label expected
        assert_that(ext_port_1 in endpoint_ports)
        assert_that(ext_port_2 not in endpoint_ports)

        for slot in self.cluster.slots.values():
            assert_that(slot.grpc_port not in endpoint_ports)

        # Just check again to cover discovery cache issue
        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, ext_port_1), self.database_name)
        resolver = ydb.DiscoveryEndpointsResolver(driver_config)
        driver = ydb.Driver(driver_config)
        driver.wait(timeout=10)

        endpoint_ports = [endpoint.port for endpoint in resolver.resolve().endpoints]
        assert_that(ext_port_1 in endpoint_ports)
        assert_that(ext_port_2 not in endpoint_ports)

        for slot in self.cluster.slots.values():
            assert_that(slot.grpc_port not in endpoint_ports)

        # Repeat using other ext endpoint
        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, ext_port_2), self.database_name)
        resolver = ydb.DiscoveryEndpointsResolver(driver_config)
        driver = ydb.Driver(driver_config)
        driver.wait(timeout=10)

        endpoint_ports = [endpoint.port for endpoint in resolver.resolve().endpoints]
        assert_that(ext_port_1 not in endpoint_ports)
        assert_that(ext_port_2 in endpoint_ports)

        for slot in self.cluster.slots.values():
            assert_that(slot.grpc_port not in endpoint_ports)


@six.add_metaclass(abc.ABCMeta)
class AbstractTestDiscoveryFaultInjection(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(configurator=KikimrConfigGenerator(extra_feature_flags=["enable_discovery_long_polling"]))
        cls.cluster.start()
        cls.database_name = '/Root/database'
        cls.logger = logger.getChild(cls.__name__)
        cls.cluster.create_database(
            cls.database_name,
            storage_pool_units_count={
                'hdd': 1
            }
        )
        cls.cluster.register_and_start_slots(cls.database_name, count=2)
        cls.cluster.wait_tenant_up(cls.database_name)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    @abc.abstractproperty
    def fault_injection_name(self):
        pass

    @abc.abstractmethod
    def inject_fault(self, slot):
        pass

    @abc.abstractmethod
    def extract_fault(self, slot):
        pass

    def test_scenario(self):
        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port), self.database_name)
        resolver = ydb.DiscoveryEndpointsResolver(driver_config)
        driver = ydb.Driver(driver_config)
        driver.wait(timeout=10)

        wait_until_discovery_includes_all_slot_ports(
            self.cluster,
            resolver,
            message="ListEndpoints should include every tenant slot before fault injection",
        )

        initial_ports = [endpoint.port for endpoint in resolver.resolve().endpoints]
        initial_ports = initial_ports[:1]

        for slot in self.cluster.slots.values():
            if slot.grpc_port in initial_ports:
                self.inject_fault(slot)
                self.logger.info(
                    "Injected fault, slot with gRPC port: %s" % slot.grpc_port
                )

        for _ in range(10):
            try:
                session = driver.table_client.session().create()
            except Exception:
                pass

        self.logger.info("Waiting for slot to move")
        moved = False
        while not moved:
            resolv = resolver.resolve()

            if resolv is None:
                time.sleep(3)
                continue

            for endpoint in resolv.endpoints:
                if endpoint.port not in initial_ports:
                    moved = True

            if not moved:
                self.logger.debug("No additional endpoints for database")
                time.sleep(3)

        driver._discovery_thread.execute_discovery()
        driver._discovery_thread.execute_discovery()
        for slot in self.cluster.slots.values():
            if slot.grpc_port in initial_ports:
                self.extract_fault(slot)
                self.logger.info("Extracted fault, slot with gRPC port: %s" % slot.grpc_port)

        last_recovery_error = {"exception": None}

        def prepare_select_one_succeeds():
            session = None
            try:
                session = driver.table_client.session().create()
                session.prepare('select 1')
                return True
            except Exception as e:
                last_recovery_error["exception"] = e
                return False
            finally:
                if session is not None:
                    try:
                        session.delete()
                    except Exception:
                        pass

        assert_that(
            wait_for(prepare_select_one_succeeds, timeout_seconds=120, step_seconds=1.0),
            is_(True),
            "Database did not recover after fault extraction (prepare select 1); last error: %r"
            % (last_recovery_error["exception"],),
        )

        # ensure zero errors once healthy
        for _ in range(9):
            session = driver.table_client.session().create()
            try:
                session.prepare('select 1')
            finally:
                try:
                    session.delete()
                except Exception:
                    pass


class TestDiscoveryFaultInjectionSlotStop(AbstractTestDiscoveryFaultInjection):
    def inject_fault(self, slot):
        with grpc.insecure_channel(self.cluster.nodes[1].endpoint) as channel:
            list_endpoints = DiscoveryServiceStub(channel).ListEndpoints
            request = discovery.ListEndpointsRequest(database=self.database_name)
            initial = _long_poll_snapshot(list_endpoints(request, timeout=5))
            request.snapshot_hash = initial.snapshot_hash
            pending = [list_endpoints.future(request, timeout=25) for _ in range(2)]
            with pytest.raises(grpc.FutureTimeoutError):
                pending[0].result(timeout=1)
            slot.stop()
            expected = {s.grpc_port for s in self.cluster.slots.values()} - {slot.grpc_port}
            for call in pending:
                result = _long_poll_snapshot(call.result())
                assert {ep.port for ep in result.endpoints} == expected
                assert result.snapshot_hash != initial.snapshot_hash
            # The board changed before this poll started.
            current = _long_poll_snapshot(list_endpoints(request, timeout=5))
            assert {ep.port for ep in current.endpoints} == expected
            assert current.snapshot_hash != initial.snapshot_hash

    def extract_fault(self, slot):
        slot.start()

    @property
    def fault_injection_name(self):
        return 'slot_stop'


class TestMirror3DCDiscovery(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(erasure=types.Erasure.MIRROR_3_DC, use_in_memory_pdisks=False))
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def test_mirror3dc_discovery_logic(self):
        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port), '/Root', use_all_nodes=False)
        driver = ydb.Driver(driver_config)
        driver.wait(timeout=10)

        while driver._store.size < 9:
            driver._discovery_thread.execute_discovery()
            time.sleep(1)

        def ensure_locality(step_name):
            self_location_ports = [self.cluster.nodes[idx].grpc_port for idx in range(1, 10, 3)]
            for iteration_id in range(100):
                connection = driver._store.get()

                self_location = False
                for port in self_location_ports:
                    if str(port) in connection.endpoint:
                        self_location = True

                assert_that(
                    self_location,
                    is_(True),
                    "%s, iteration id %d: expected endpoint with ports %s, but actual is %s" % (
                        step_name,
                        iteration_id,
                        str(self_location_ports),
                        connection.endpoint
                    )
                )

        ensure_locality("Initial discovery")

        for idx in range(1, 10, 3):
            self.cluster.nodes[idx].stop()

        for _ in range(6):
            try:
                driver.table_client.session().create()
            except Exception:
                continue

        for _ in range(100):
            connection = driver._store.get()

            assert_that(
                connection,
                is_(
                    not_(
                        None
                    )
                )
            )

        for idx in range(1, 10, 3):
            self.cluster.nodes[idx].start()

        while driver._store.size < 9:
            driver._discovery_thread.execute_discovery()
            time.sleep(1)

        ensure_locality("Test finish")
