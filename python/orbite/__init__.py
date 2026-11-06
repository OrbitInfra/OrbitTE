"""TENT-compatible, point-to-point OrbiTE bindings.

Integer addresses must refer to engine allocations or retained buffer exports.
External buffers are registered with ``owner=buffer`` or ``register_buffer``.
Do not access registered storage concurrently with a transfer.
"""
from dataclasses import dataclass, field
from enum import IntEnum
import operator
from typing import ClassVar

from ._native import (
    _Engine, TentException, InvalidArgumentError, AddressNotRegisteredError,
    DeviceNotFoundError, InvalidEntryError, RdmaError, CudaError, MetadataError,
    RpcServiceError, InternalError, NotImplementedError,
)

LOCAL_SEGMENT_ID = 0
kWildcardLocation = "*"
PRIO_HIGH, PRIO_MEDIUM, PRIO_LOW = range(3)


class OpCode(IntEnum):
    READ = 0
    WRITE = 1


class TransferStatusEnum(IntEnum):
    INITIAL = 0
    PENDING = 1
    INVALID = 2
    CANCELED = 3
    COMPLETED = 4
    TIMEOUT = 5
    FAILED = 6


class Permission(IntEnum):
    LocalReadWrite = 0
    GlobalReadOnly = 1
    GlobalReadWrite = 2


class TransportType(IntEnum):
    UNSPEC = 0
    RDMA = 1
    MNNVL = 2
    SHM = 3
    NVLINK = 4
    GDS = 5
    IOURING = 6
    TCP = 7
    AscendDirect = 8
    SUNRISE_LINK = 9
    TPU = 10
    UB = 11
    MPCOMM = 12
    HP_TCP = 13
    XPU = 14
    HYLINK = 15
    FABRIC = 16
    MLU = 17
    MEMFABRIC = 18


class IntentType(IntEnum):
    INTENT_UNSPEC = 0
    FOREGROUND_GET = 1
    BACKGROUND_PREFETCH = 2
    MIGRATION = 3
    CHECKPOINT = 4
    WEIGHT_LOADING = 5
    STAGING_INTERNAL = 6


class SegmentInfoType(IntEnum):
    Memory = 0
    File = 1


for _enum in (OpCode, TransferStatusEnum, Permission, TransportType, IntentType, SegmentInfoType):
    globals().update(_enum.__members__)


class _Record:
    __slots__ = ()
    _fields: ClassVar[dict] = {}

    def __setattr__(self, name, value):
        rule = self._fields.get(name)
        if rule in ("u64", "i32"):
            value = operator.index(value)
            low, high = (0, 2**64 - 1) if rule == "u64" else (-2**31, 2**31 - 1)
            if not low <= value <= high:
                raise OverflowError(f"{name} is outside {rule}")
        elif rule is str or rule == "optional_string":
            if not (rule == "optional_string" and value is None) and not isinstance(value, str):
                raise TypeError(f"{name} must be a string")
        elif rule is bool:
            value = bool(value)
        elif isinstance(rule, type) and issubclass(rule, IntEnum):
            value = rule(operator.index(value))
        elif rule == "buffers":
            if not isinstance(value, (tuple, list)) or any(not isinstance(v, SegmentInfoBuffer) for v in value):
                raise TypeError("buffers must contain SegmentInfoBuffer records")
            value = list(value)
        object.__setattr__(self, name, value)


@dataclass(slots=True)
class Notification(_Record):
    _fields: ClassVar[dict] = {"name": str, "msg": str}
    name: str = ""
    msg: str = ""


@dataclass(slots=True)
class Request(_Record):
    _fields: ClassVar[dict] = {"opcode": OpCode, "source": "u64", "target_id": "u64", "target_offset": "u64", "length": "u64", "priority": "i32", "transport_hint": TransportType, "policy_name": "optional_string", "deadline_ns": "u64", "intent_type": IntentType}
    opcode: OpCode = OpCode.READ
    source: int = 0
    target_id: int = 0
    target_offset: int = 0
    length: int = 0
    priority: int = PRIO_HIGH
    transport_hint: TransportType = TransportType.UNSPEC
    policy_name: str | None = None
    deadline_ns: int = 0
    intent_type: IntentType = IntentType.INTENT_UNSPEC


@dataclass(slots=True)
class TransferStatus(_Record):
    _fields: ClassVar[dict] = {"state": TransferStatusEnum, "bytes": "u64"}
    state: TransferStatusEnum = TransferStatusEnum.INITIAL
    bytes: int = 0


def _status(row):
    return TransferStatus(TransferStatusEnum(row[0]), row[1])


@dataclass(slots=True)
class SegmentInfoBuffer(_Record):
    _fields: ClassVar[dict] = {"base": "u64", "length": "u64", "location": str}
    base: int = 0
    length: int = 0
    location: str = ""


@dataclass(slots=True)
class SegmentInfo(_Record):
    _fields: ClassVar[dict] = {"type": SegmentInfoType, "buffers": "buffers"}
    type: SegmentInfoType = SegmentInfoType.Memory
    buffers: list[SegmentInfoBuffer] = field(default_factory=list)


@dataclass(slots=True)
class MemoryOptions(_Record):
    _fields: ClassVar[dict] = {"location": str, "perm": Permission, "type": TransportType, "shm_path": str, "shm_offset": "u64", "internal": bool}
    location: str = kWildcardLocation
    perm: Permission = Permission.GlobalReadWrite
    type: TransportType = TransportType.UNSPEC
    shm_path: str = ""
    shm_offset: int = 0
    internal: bool = False


class MemoryGuard:
    """Retains the engine. Early drop never frees registered or active storage."""
    def __init__(self, engine, address, size):
        self._engine, self._address, self._size = engine, address, size

    def address(self):
        return self._address

    def size(self):
        return self._size

    def __enter__(self):
        return self.address()

    def __exit__(self, *args):
        if self._address:
            try:
                self._engine._native.abandon_memory(self._address)
            except TentException:
                if not args or args[0] is None:
                    raise
            finally:
                self._address = 0

    def __del__(self):
        try:
            self.__exit__()
        except Exception:
            # The native table retains failed cleanup and its budget.
            pass


class BatchGuard:
    def __init__(self, engine, batch_id):
        self._engine, self._id, self._released = engine, batch_id, False

    def id(self):
        return self._id

    def release(self):
        if not self._released:
            self._engine._native.abandon_batch(self._id)
            self._released = True

    def __enter__(self):
        return self.id()

    def __exit__(self, *args):
        try:
            self.release()
        except TentException:
            if not args or args[0] is None:
                raise

    def __del__(self):
        try:
            self.release()
        except Exception:
            pass


class TransferEngine:
    def __init__(self, config_path=None):
        self._native = _Engine(config_path)

    def available(self):
        return self._native.available()

    def get_segment_name(self):
        return self._native.get_segment_name()

    def get_rpc_server_address(self):
        return self._native.get_rpc_server_address()

    def get_rpc_server_port(self):
        return self._native.get_rpc_server_port()

    def get_local_topology(self):
        return self._native.get_local_topology()

    def export_local_segment(self):
        return self._native.export_local_segment()

    def import_remote_segment(self, shared_handle):
        return self._native.import_remote_segment(shared_handle)

    def open_segment(self, segment_name):
        return self._native.open_segment(segment_name)

    def close_segment(self, handle):
        return self._native.close_segment(handle)

    def get_segment_info(self, handle):
        kind, rows = self._native.get_segment_info(handle)
        return SegmentInfo(SegmentInfoType(kind), [SegmentInfoBuffer(*r) for r in rows])

    def allocate_local_memory(self, size, location=kWildcardLocation):
        return self.allocate_local_memory_ex(size, MemoryOptions(location=location))

    def allocate_local_memory_ex(self, size, options):
        return self._native.allocate(size, options)

    def free_local_memory(self, addr):
        return self._native.free(addr)

    def allocate_memory_guard(self, size, location=kWildcardLocation):
        return self.allocate_memory_guard_ex(size, MemoryOptions(location=location))

    def allocate_memory_guard_ex(self, size, options):
        return MemoryGuard(self, self.allocate_local_memory_ex(size, options), size)

    def register_local_memory(self, addr, size, permission=Permission.GlobalReadWrite, *, owner=None):
        return self.register_local_memory_ex(addr, size, MemoryOptions(perm=permission), owner=owner)

    def register_local_memory_ex(self, addr, size, options, *, owner=None):
        return self._native.register([addr], [size], options, [owner])

    def register_local_memory_batch(self, addr_list, size_list, permission=Permission.GlobalReadWrite, *, owners=None):
        return self.register_local_memory_batch_ex(addr_list, size_list, MemoryOptions(perm=permission), owners=owners)

    def register_local_memory_batch_ex(self, addr_list, size_list, options, *, owners=None):
        return self._native.register(addr_list, size_list, options, owners)

    def register_buffer(self, buffer, permission=Permission.GlobalReadWrite):
        """Register a writable contiguous exporter and return its retained address."""
        return self._native.register_buffer(buffer, MemoryOptions(perm=permission))

    def unregister_local_memory(self, addr, size=0):
        return self._native.unregister([addr], [size])

    def unregister_local_memory_batch(self, addr_list, size_list=()):
        return self._native.unregister(addr_list, size_list)

    def allocate_transfer_batch(self, batch_size):
        return self._native.allocate_transfer_batch(batch_size)

    def free_transfer_batch(self, batch_id):
        return self._native.free_transfer_batch(batch_id)

    def allocate_batch_guard(self, batch_size):
        return BatchGuard(self, self.allocate_transfer_batch(batch_size))

    def submit_transfer(self, batch_id, request_list, notifi=None):
        return self._native.submit_transfer(batch_id, request_list, notifi)

    def submit_transfer_notif(self, batch_id, request_list, name, message):
        return self.submit_transfer(batch_id, request_list, Notification(name, message))

    def cancel_transfer(self, batch_id, task_id):
        return self._native.cancel_transfer(batch_id, task_id)

    def send_notifi(self, target_id, notifi):
        return self._native.send_notifi(target_id, notifi)

    def recv_notifi(self):
        return [Notification(*n) for n in self._native.recv_notifi()]

    def get_transfer_status(self, batch_id, task_id):
        return _status(self._native.get_transfer_status(batch_id, task_id))

    def get_transfer_status_list(self, batch_id):
        return [_status(s) for s in self._native.get_transfer_status_list(batch_id)]

    def get_transfer_status_overall(self, batch_id):
        return _status(self._native.get_transfer_status_overall(batch_id))

    # Additive ownership/progress helpers; not part of frozen pybind.cpp.
    def progress_batch(self, batch_id):
        return _status(self._native.progress_batch(batch_id))

    def wait(self, batch_id, timeout):
        """Wait for logical completion and physical drain; timeout retains owners."""
        return _status(self._native.wait(batch_id, timeout))

    def terminal_result(self, batch_id, task_id=None):
        """Return (native_code, effect_known); observational, never replay permission."""
        return self._native.terminal_result(batch_id, task_id)

    def close(self):
        return self._native.close()
