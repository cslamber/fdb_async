from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
import functools
import inspect
import itertools
import json
import logging
import time
from pathlib import Path
from typing import Never, TYPE_CHECKING, overload

from .itypes import (
    ClientStatus,
    KeyValue,
    KeySelector,
    VersionstampTemplate,
    StreamingMode,
)
from . import c_api, opts

_log = logging.getLogger(__name__)
FDBError = c_api.FDBError  # unfortunate


class _NetworkOptions(opts.NetworkOptions):
    def _set_option(self, code: int, value: bytes) -> None:
        c_api.network_set_option(code, value)


options = _NetworkOptions()


def _transaction_timer(name: str) -> Generator[None, None | bool, Never]:  # type: ignore[misc]
    if not _log.isEnabledFor(logging.WARNING):
        while True:
            yield

    start = time.monotonic_ns()
    for retries in itertools.count():
        not_ = "" if (yield) else "not "
        now = time.monotonic_ns()
        if now - start >= 1_000_000_000:
            msg = (
                "fdb WARNING: long transaction "
                '(%dns elapsed in transaction "%s": %d retries, %scommitted)'
            )
            _log.warning(msg, now - start, name, retries, not_)


class Tenant:
    def __init__(self, ptr: c_api.Tenant, db: Database) -> None:
        self._ptr: c_api.Tenant | c_api.Database = ptr
        self._db = db

    async def transact[T](self, fn: Callable[[Transaction], Awaitable[T]]) -> T:  # type: ignore[return]
        timer = _transaction_timer(fn.__qualname__)
        tr = Transaction(self._ptr.create_transaction(), self)
        for _ in timer:
            try:
                ret = await fn(tr)
                await tr._tr.commit()
                timer.send(True)
                return ret
            except FDBError as e:
                await tr._tr.on_error(e.code)


class Database(Tenant, opts.DatabaseOptions):
    def __init__(self, cluster_file: Path | None = None) -> None:
        self._ptr: c_api.Database = c_api.create_database(
            b"" if cluster_file is None else str(cluster_file).encode()
        )
        self._db = self
        self._fdb_thread = c_api.fdb_thread()

    async def client_status(self) -> ClientStatus:
        return json.loads(await self._ptr.get_client_status())

    def main_thread_busyness(self) -> float:
        return self._ptr.get_main_thread_busyness()

    def open_tenant(self, name: bytes) -> Tenant:
        return Tenant(self._ptr.open_tenant(name), self)


class Snapshot:
    _snapshot: bool = True

    def __init__(self, tr: c_api.Transaction, db: Database | Tenant) -> None:
        self._tr = tr
        self._db = db

    @property
    def db(self) -> Database:
        return self._db._db

    async def get_range(
        self,
        begin: bytes | KeySelector,
        end: bytes | KeySelector,
        *,
        limit: int | None = None,
        mode: StreamingMode = StreamingMode.iterator,
        reverse: bool = False,
    ) -> AsyncGenerator[KeyValue]:
        if limit == 0:
            return
        elif limit is None:
            limit = 0

        if isinstance(begin, bytes):
            begin = KeySelector.first_greater_or_equal(begin)
        if isinstance(end, bytes):
            end = KeySelector.first_greater_or_equal(end)

        for iteration in itertools.count(1):
            # TODO implement target_bytes
            kvs, more = await self._tr.get_range(
                begin, end, limit, 0, mode, iteration, self._snapshot, reverse
            )
            for kv in kvs:
                yield kv

            if limit:
                limit -= len(kvs)
                if limit <= 0:
                    return

            if not more:
                return

            if reverse:
                end = KeySelector.first_greater_or_equal(kvs[-1].key)
            else:
                begin = KeySelector.first_greater_than(kvs[-1].key)

    async def get_key(self, key: bytes | KeySelector) -> bytes:
        if isinstance(key, bytes):
            return key
        return await self._tr.get_key(key, snapshot=self._snapshot)

    # mypy doesn't handle functools.singledispatchmethod correctly yet
    if TYPE_CHECKING:

        @overload
        async def __getitem__(self, key: bytes | KeySelector) -> bytes | None: ...
        @overload
        def __getitem__(
            self, key: slice[bytes | KeySelector]
        ) -> AsyncGenerator[KeyValue]: ...
        def __getitem__(self, key): ...

    else:

        @functools.singledispatchmethod
        async def __getitem__(self, key: bytes | KeySelector) -> bytes | None:
            return await self._tr.get(await self.get_key(key), snapshot=self._snapshot)

        @__getitem__.register
        def _(self, key: slice[bytes | KeySelector]) -> AsyncGenerator[KeyValue]:
            return self.get_range(key.start, key.stop, reverse=key.step == -1)


class ReadOnlyTransaction(Snapshot):
    _snapshot = False

    def add_read_conflict_range(self, begin: bytes, end: bytes) -> None:
        self._tr.add_conflict_range(begin, end, False)

    @property
    def snapshot(self) -> Snapshot:
        return Snapshot(self._tr, self.db)


class Transaction(ReadOnlyTransaction, opts.TransactionOptions):
    @property
    def readonly(self) -> ReadOnlyTransaction:
        return ReadOnlyTransaction(self._tr, self.db)

    @property
    def snapshot(self) -> Snapshot:
        return self.readonly.snapshot

    # TODO functools.singledispatchmethod
    @overload
    def __setitem__(
        self, key: bytes, val: VersionstampTemplate | bytes | None
    ) -> None: ...
    @overload
    def __setitem__(self, key: VersionstampTemplate, val: bytes) -> None: ...
    def __setitem__(self, key, val) -> None:
        if isinstance(key, VersionstampTemplate):
            self._atomic_op(14, key.encode(), val)
        elif val is None:
            self._tr.clear(key)
        elif isinstance(val, VersionstampTemplate):
            # not sure why this exists, just setting the value entirely seems sufficient?
            self._atomic_op(15, key, val.encode())
        else:
            self._tr.set(key, val)

    def __delitem__(self, key: bytes | slice[bytes]) -> None:
        if isinstance(key, slice):
            self._tr.clear_range(key.start, key.stop)
        else:
            self._tr.clear(key)

    async def watch(self, key: bytes) -> None:
        await self._tr.watch(key)

    def add_write_conflict_range(self, begin: bytes, end: bytes) -> None:
        self._tr.add_conflict_range(begin, end, True)

    async def tag_throttled_duration(self) -> float:
        return await self._tr.get_tag_throttled_duration()

    async def total_cost(self) -> int:
        return await self._tr.get_total_cost()

    async def approximate_size(self) -> int:
        return await self._tr.get_approximate_size()

    def _atomic_op(self, op: int, key: bytes, param: bytes) -> None:
        self._tr.atomic_op(key, param, op)

    # mypy can't handle partialmethod so partial will do
    add = functools.partial(_atomic_op, op=2)
    bit_and = functools.partial(_atomic_op, op=6)
    bit_or = functools.partial(_atomic_op, op=7)
    bit_xor = functools.partial(_atomic_op, op=8)
    append_if_fits = functools.partial(_atomic_op, op=9)
    max = functools.partial(_atomic_op, op=12)
    min = functools.partial(_atomic_op, op=13)
    byte_min = functools.partial(_atomic_op, op=16)
    byte_max = functools.partial(_atomic_op, op=17)
    compare_and_clear = functools.partial(_atomic_op, op=20)

    # for opts
    def _set_option(self, code: int, val: bytes) -> None:
        self._tr.set_option(code, val)


versionstamp = VersionstampTemplate(b"\x00" * 10, 0)


# compatibility
def transactional(fn, arg="tr"):
    sig = inspect.getsignature(fn)

    def inner(*args, **kwargs):
        ba = sig.bind(*args, **kwargs)
        arg = ba["tr"]
        if not isinstance(arg, Tenant):
            return fn(*ba.args, **ba.kwargs)

        @arg.transact
        def ret(tr):
            ba["tr"] = tr
            return fn(*ba.args, **ba.kwargs)

        return ret

    return functools.update_wrapper(inner, fn)


# TODO tuples
