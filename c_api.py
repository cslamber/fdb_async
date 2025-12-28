import asyncio
import ctypes
import functools
import inspect
import threading
from collections.abc import Callable
from ctypes.util import find_library
from typing import Any, Annotated

from .itypes import KeyValue, KeySelector, ErrorPredicate, StreamingMode

capi = ctypes.CDLL(find_library("fdb"))

# TODO handle this better
HEADER_VERSION = API_VERSION = 740


class KeyValueStruct(ctypes.Structure):
    _fields_ = [
        ("key", ctypes.c_char_p),
        ("key_length", ctypes.c_int),
        ("value", ctypes.c_char_p),
        ("value_length", ctypes.c_int),
    ]
    _pack_ = 4


class KeyStruct(ctypes.Structure):
    _fields_ = [("key", ctypes.c_char_p), ("key_length", ctypes.c_int)]
    _pack_ = 4


# why is this not in std?
py_incref = ctypes.pythonapi.Py_IncRef
py_decref = ctypes.pythonapi.Py_DecRef
py_incref.argtypes = py_decref.argtypes = [ctypes.py_object]
py_incref.restype = py_decref.restype = None


def errcheck(result: int, _func: object, _args: object) -> None:
    if result:
        raise FDBError(result)


class FDBError(Exception):
    def __init__(self, code: int) -> None:
        self.code = code

    def __str__(self) -> str:
        return get_error(self.code).decode()


# weakref to stop the thread if all databases get cleaned up
# NOTE: doesn't work because fdb does not allow restarting the thread
_network_thread: Callable[[], object] = lambda: None
network_thread_lock = threading.Lock()
initialized = False


class NetworkThread(threading.Thread):
    name = "fdb-network-thread"
    daemon = False

    def run(self) -> None:
        # TODO error handling
        run_network()

    # the python program will not exit until this thread joins
    def __del__(self) -> None:
        stop_network()


def fdb_init() -> None:
    global initialized
    if initialized:
        return

    select_api_version_impl(API_VERSION, HEADER_VERSION)
    initialized = True


def fdb_thread() -> object:
    global _network_thread
    with network_thread_lock:
        if thread := _network_thread():
            return None

        fdb_init()
        setup_network()
        thread = NetworkThread()
        thread.start()
        _network_thread = lambda: thread
        return None
        # if fdb allows restarting thread, then replace with:
        # _network_thread = weakref.ref(thread); return thread


@ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.c_void_p)
def hook_future_cb(_fdb_fut: ctypes.c_void_p, py_fut: ctypes.c_void_p) -> None:
    fut: asyncio.Future = ctypes.cast(py_fut, ctypes.py_object).value
    py_decref(fut)
    fut.get_loop().call_soon_threadsafe(fut.set_result, None)


future_result_handlers: dict[object, Callable] = {}

NoExcept = Annotated[None, "noexcept"]


# generate a type-safe body for fdb api functions
def api_func[T: Callable](fn: T, prefix="") -> T:
    sig = inspect.signature(fn)
    ret = sig.return_annotation

    # ctypes doesn't allow for multiple-argument from_param functions nor param
    # names, so I manually implement them here by generating a function with the
    # same signature that outputs the tuple of arguments to the c function -
    # maybe change in the future
    c_func = capi[f"fdb_{prefix}{fn.__name__}"]
    c_func.argtypes = []
    argfunc_return = ""

    for name, param in sig.parameters.items():
        ann = param.annotation
        if ann is bytes:
            c_func.argtypes += [ctypes.c_char_p, ctypes.c_int]
            argfunc_return += f"{name}, len({name}), "
        elif ann is KeySelector:
            c_func.argtypes += [ctypes.c_char_p] + [ctypes.c_int] * 3
            argfunc_return += (
                f"{name}.key, len({name}).key, {name}.or_equal, {name}.offset, "
            )
        else:
            argfunc_return += f"{name}, "
            if hasattr(ann, "__metadata__"):
                c_func.argtypes += [ann.__metadata__[0]]
            elif ann is param.empty:
                c_func.argtypes += [ctypes.c_void_p]
            else:
                c_func.argtypes += [ctypes.c_int]

    argfunc_globals: dict[str, Any] = {}
    exec(
        f"def argfunc{sig.format(quote_annotation_strings=True)}: return ({argfunc_return})",
        argfunc_globals,
    )
    argfunc = argfunc_globals["argfunc"]
    inner: Callable

    if inspect.iscoroutinefunction(fn):
        res_func = future_result_handlers[ret]
        c_func.restype = ctypes.c_void_p

        async def inner(*args, **kwargs) -> Any:
            fdb_fut = Future(c_func(*argfunc(*args, **kwargs)))
            # asyncio futures are not thread safe so the fast path of this has
            # to call_soon_threadsafe: do the check manually in case I can avoid
            # it
            if not fdb_fut.is_ready():
                fut = asyncio.get_running_loop().create_future()
                py_incref(fut)  # reference held by C
                try:
                    fdb_fut.set_callback(hook_future_cb, ctypes.py_object(fut))
                except:
                    py_decref(fut)
                    raise

                try:
                    await fut
                except:
                    fdb_fut.cancel()
                    # I have to be able handle the potential race of _cancel()
                    # against the callback firing, in which I have to know
                    # whether the callback actually ran and decremented the
                    # refcount of `fut` by checking _is_ready()
                    if not fdb_fut.is_ready():
                        py_decref(fut)
                    raise
            return res_func(fdb_fut)

    else:
        if ret == NoExcept:
            c_func.restype = None
        elif ret is float:
            c_func.restype = ctypes.c_double
        elif ret is bool:
            c_func.restype = bool  # int => bool
        else:
            c_func.errcheck = errcheck  # type: ignore[assignment]

        if isinstance(ret, type) and issubclass(ret, Handle):

            def inner(*args, **kwargs) -> Any:
                out = ctypes.c_void_p()
                c_func(*argfunc(*args, **kwargs), ctypes.byref(out))
                return ret(out)

        else:

            def inner(*args, **kwargs) -> Any:
                return c_func(*argfunc(*args, **kwargs))

    return functools.update_wrapper(inner, fn)  # type: ignore


class Handle:
    __slots__ = ("_as_parameter_",)

    def __init__(self, ptr: ctypes.c_void_p):
        self._as_parameter_ = ptr

    def destroy(self) -> None:
        raise NotImplementedError

    def __del__(self) -> None:
        self.destroy()

    def __init_subclass__(cls):
        cls.__slots__ = ()
        for name, member in cls.__dict__.items():
            if inspect.isfunction(member):
                setattr(cls, name, api_func(member, cls.__name__.lower() + "_"))


def transaction_commmitted_version(self: Transaction) -> int:
    out = ctypes.c_int64()
    self.get_committed_version(ctypes.byref(out))
    return out.value


# mypy: disable-error-code="empty-body"
class Future(Handle):
    @staticmethod
    def register(fn):
        future_result_handlers[inspect.signature(fn).return_annotation] = fn

    def destroy(self) -> NoExcept: ...
    def cancel(self) -> NoExcept: ...
    def is_ready(self) -> bool: ...
    def set_callback(
        self, fn: Annotated[object, type(hook_future_cb)], param
    ) -> None: ...

    def get_error(self) -> None: ...
    def get_int64(self, out) -> None: ...
    def get_double(self, out) -> None: ...
    def get_value(self, present, buf, length) -> None: ...
    def get_key(self, buf, length) -> None: ...
    def get_key_array(self, out, length) -> None: ...
    def get_keyvalue_array(self, out, length, more) -> None: ...
    def get_string_array(self, buf, length) -> None: ...

    register(get_error)

    @register
    def _(self) -> int:
        out = ctypes.c_int64()
        self.get_int64(ctypes.byref(out))
        return out.value

    @register
    def _(self) -> float:
        out = ctypes.c_double()
        self.get_double(ctypes.byref(out))
        return out.value

    # TODO maybe buffer protocol to avoid copies
    @register
    def _(self) -> bytes | None:
        present = ctypes.c_int()
        buf = ctypes.c_void_p()
        length = ctypes.c_int()
        self.get_value(ctypes.byref(present), ctypes.byref(buf), ctypes.byref(length))
        return ctypes.string_at(buf, length.value) if present.value else None

    @register
    def _(self) -> bytes:
        buf = ctypes.c_void_p()
        length = ctypes.c_int()
        self.get_key(ctypes.byref(buf), ctypes.byref(length))
        return ctypes.string_at(buf, length.value)

    @register
    def _(self) -> list[bytes]:
        buf = ctypes.POINTER(KeyStruct)()
        count = ctypes.c_int()
        self.get_key_array(ctypes.byref(buf), ctypes.byref(count))
        return [ctypes.string_at(x.key, x.key_length) for x in buf[: count.value]]

    @register
    def _(self) -> tuple[list[KeyValue], bool]:
        buf = ctypes.POINTER(KeyValueStruct)()
        count = ctypes.c_int()
        more = ctypes.c_int()
        self.get_keyvalue_array(
            ctypes.byref(buf), ctypes.byref(count), ctypes.byref(more)
        )
        return (
            [
                KeyValue(
                    ctypes.string_at(x.key, x.key_length),
                    ctypes.string_at(x.value, x.value_length),
                )
                for x in buf[: count.value]
            ],
            more.value > 0,
        )

    @register
    def _(self) -> list[bytes]:
        buf = ctypes.pointer(ctypes.c_char_p())
        count = ctypes.c_int()
        self.get_string_array(ctypes.byref(buf), ctypes.byref(count))
        return [s.value for s in buf[: count.value]]


class Transaction(Handle):
    def destroy(self) -> NoExcept: ...
    def set_option(self, option: int, value: bytes) -> None: ...
    def set_read_version(self, version: Annotated[int, ctypes.c_int64]) -> NoExcept: ...

    # direct translations of C API functions
    async def get_read_version(self) -> int: ...
    async def get_estimated_range_size_bytes(self, begin: bytes, end: bytes) -> int: ...
    async def get(self, key: bytes, snapshot: bool) -> bytes | None: ...
    async def get_range_split_points(
        self, begin: bytes, end: bytes, chunk_size: Annotated[int, ctypes.c_int64]
    ) -> list[bytes]: ...
    async def get_key(self, selector: KeySelector, snapshot: bool) -> bytes: ...
    async def get_addresses_for_key(self, key: bytes) -> list[bytes]: ...
    async def get_range(
        self,
        begin: KeySelector,
        end: KeySelector,
        limit: int,
        target_bytes: int,
        mode: StreamingMode,
        iteration: int,
        snapshot: bool,
        reverse: bool,
    ) -> tuple[list[KeyValue], bool]: ...

    def set(self, key: bytes, val: bytes) -> NoExcept: ...
    def clear(self, key: bytes) -> NoExcept: ...
    def clear_range(self, begin: bytes, end: bytes) -> NoExcept: ...
    def atomic_op(self, key: bytes, param: bytes, op: int) -> NoExcept: ...
    async def commit(self) -> None: ...
    def get_committed_version(self, out) -> None: ...
    async def get_tag_throttled_duration(self) -> float: ...
    async def get_total_cost(self) -> int: ...
    async def get_approximate_size(self) -> int: ...
    async def get_versionstamp(self) -> bytes: ...
    async def watch(self, key: bytes) -> None: ...
    async def on_error(self, code: int) -> None: ...
    def cancel(self) -> NoExcept: ...
    def reset(self) -> NoExcept: ...
    def add_conflict_range(self, begin: bytes, end: bytes, is_write: bool) -> None: ...


class Database(Handle):
    def destroy(self) -> NoExcept: ...
    def set_option(self, option: int, value: bytes) -> None: ...
    def open_tenant(self, name: bytes) -> Tenant: ...
    def create_transaction(self) -> Transaction: ...
    async def reboot_worker(
        self, address: bytes, check: bool, duration: int
    ) -> int: ...
    async def force_recovery_with_data_loss(self, dcId: bytes) -> None: ...
    async def create_snapshot(self, snapshot_command: bytes) -> None: ...
    def get_main_thread_busyness(self) -> float: ...
    async def get_client_status(self) -> bytes: ...


class Tenant(Handle):
    def destroy(self) -> NoExcept: ...
    def create_transaction(self) -> Transaction: ...


@api_func
def get_error(code: int) -> bytes: ...
@api_func
def create_database(
    cluster_file_path: Annotated[bytes, ctypes.c_char_p],
) -> Database: ...
@api_func
def network_set_option(code: int, value: bytes) -> None: ...
@api_func
def select_api_version_impl(runtime_version: int, header_version: int) -> None: ...
@api_func
def setup_network() -> None: ...
@api_func
def run_network() -> None: ...
@api_func
def stop_network() -> None: ...
@api_func
def error_predicate(predicate_test: ErrorPredicate, code: int) -> bool: ...
