from abc import abstractmethod
from collections.abc import Callable
from typing import Protocol, assert_never
from pathlib import Path


class HasSetOption(Protocol):
    @abstractmethod
    def _set_option(self, code: int, value: bytes) -> None: ...


def option[
    T: (int, bytes, Path)
](code: int, typ: type[T]) -> Callable[[HasSetOption, T], None]:
    if issubclass(typ, int):
        conv = lambda v: v.to_bytes(8, "little")
    elif issubclass(typ, bytes):
        conv = lambda v: v
    elif issubclass(typ, Path):
        conv = lambda v: str(v).encode()
    else:
        assert_never(typ)

    return lambda o, v: o._set_option(code, conv(v))


def noneoption(code: int) -> Callable[[HasSetOption], None]:
    return lambda o: o._set_option(code, b"")


class DatabaseOptions(HasSetOption):
    set_location_cache_size = option(10, int)
    set_max_watches = option(20, int)
    set_machine_id = option(21, bytes)
    set_datacenter_id = option(22, bytes)
    set_transaction_timeout = option(500, int)  # milliseconds
    set_transaction_retry_limit = option(501, int)
    set_transaction_max_retry_delay = option(502, int)
    set_transaction_size_limit = option(503, int)  # bytes
    set_transaction_logging_max_field_length = option(405, int)
    set_transaction_causal_read_risky = noneoption(504)
    set_snapshot_ryw_enable = noneoption(26)
    set_snapshot_ryw_disable = noneoption(27)


class NetworkOptions(HasSetOption):
    set_knob = option(40, bytes)
    set_trace_enable = option(30, Path)
    set_trace_max_logs_size = option(32, int)
    set_trace_roll_size = option(31, int)
    set_trace_format = option(34, bytes)
    set_trace_clock_source = option(35, bytes)
    set_disable_multi_version_client_api = noneoption(60)
    set_callbacks_on_external_threads = noneoption(61)
    set_external_client_library = option(62, Path)
    set_external_client_directory = option(63, Path)
    set_tls_cert_bytes = option(42, bytes)
    set_tls_cert_file = option(43, Path)
    set_tls_key_bytes = option(45, bytes)
    set_tls_key_file = option(46, Path)
    set_tls_verify_peers = option(47, bytes)
    set_tls_ca_bytes = option(52, bytes)
    set_tls_ca_file = option(53, Path)
    set_tls_password = option(54, bytes)
    set_tls_disable_plaintext_connection = noneoption(55)
    set_disable_local_client = noneoption(64)
    set_client_threads_per_version = option(65, int)
    set_disable_client_statistics_logging = noneoption(70)
    set_enable_run_loop_profiling = noneoption(71)
    set_distributed_client_tracer = option(90, bytes)


class TransactionOptions(HasSetOption):
    set_snapshot_ryw_disable = noneoption(601)
    set_snapshot_ryw_enable = noneoption(600)
    set_priority_batch = noneoption(201)
    set_priority_system_immediate = noneoption(200)
    set_causal_read_risky = noneoption(20)
    set_causal_write_risky = noneoption(10)
    set_next_write_no_write_conflict_range = noneoption(30)
    set_read_your_writes_disable = noneoption(51)
    set_access_system_keys = noneoption(301)
    set_read_system_keys = noneoption(302)
    set_retry_limit = option(501, int)
    set_max_retry_delay = option(502, int)
    set_size_limit = option(503, int)
    set_timeout = option(500, int)
    set_transaction_logging_max_field_length = option(405, int)
    set_debug_transaction_identifier = option(403, bytes)
    set_log_transaction = noneoption(404)
