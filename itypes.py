from typing import Literal, NamedTuple, NotRequired, TypedDict
from dataclasses import dataclass
from enum import Enum


class KeyValue(NamedTuple):
    key: bytes
    value: bytes


@dataclass
class KeySelector:
    key: bytes
    or_equal: bool  # redundant with key + b'\x00'
    offset: int

    def __add__(self, offset: int) -> KeySelector:
        return self.__replace__(offset=self.offset + offset)

    def __sub__(self, offset: int) -> KeySelector:
        return self + -offset

    @classmethod
    def last_less_than(cls, key: bytes) -> KeySelector:
        return cls(key, False, 0)

    @classmethod
    def last_less_or_equal(cls, key: bytes) -> KeySelector:
        return cls(key, True, 0)

    @classmethod
    def first_greater_than(cls, key: bytes) -> KeySelector:
        return cls.last_less_or_equal(key) + 1

    @classmethod
    def first_greater_or_equal(cls, key: bytes) -> KeySelector:
        return cls.last_less_than(key) + 1


@dataclass
class VersionstampTemplate:
    template: bytes
    offset: int

    def __add__(self, rhs: object) -> VersionstampTemplate:
        if isinstance(rhs, bytes):
            return VersionstampTemplate(self.template + rhs, self.offset)
        return NotImplemented

    def __radd__(self, lhs: object) -> VersionstampTemplate:
        if isinstance(lhs, bytes):
            return VersionstampTemplate(lhs + self.template, self.offset + len(lhs))
        return NotImplemented

    def encode(self) -> bytes:
        return self.template + self.offset.to_bytes(4, "little")


class ErrorPredicate(Enum):
    retryable = 50000
    maybe_committed = 50001
    retryable_not_committed = 50002


class StreamingMode(Enum):
    want_all = -2
    iterator = -1
    exact = 0
    small = 1
    medium = 2
    large = 3
    serial = 4


###
### Misc
###


class ClientStatus(TypedDict):
    Healthy: bool
    InitializationState: Literal[
        "initializing", "initialization_failed", "created", "incompatible", "closed"
    ]
    InitializationError: NotRequired[int]
    ProtocolVersion: NotRequired[int]
    ConnectionRecord: str
    DatabaseStatus: NotRequired[DatabaseStatus]
    ErrorRetrievingDatabaseStatus: NotRequired[int]
    AvailableClients: list[AvailableClient]


class AvailableClient(TypedDict):
    ProtocolVersion: int
    ReleaseVersion: str
    ThreadIndex: int


class DatabaseStatus(TypedDict):
    Healthy: bool
    ClusterID: str  # UUID
    Coordinators: list[str]
    CurrentCoordinator: str
    GrvProxies: list[str]
    CommitProxies: list[str]
    StorageServers: list[StorageServer]
    Connections: list[Connection]


class StorageServer(TypedDict):
    Address: str
    SSID: str


class Connection(TypedDict):
    Address: str
    Status: Literal["failed", "connected", "connecting", "disconnected"]
    Compatible: bool
    ConnectFailedCount: int
    LastConnectTime: int
    PingCount: int
    PingTimeoutCount: int
    BytesSampleTime: int
    BytesReceived: int
    BytesSent: int
    ProtocolVersion: NotRequired[int]
