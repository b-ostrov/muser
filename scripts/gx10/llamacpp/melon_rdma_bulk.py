#!/usr/bin/env python3
"""ctypes binding for the sending half of native/melon_rdma/melon_rdma_bulk.c.

`muser_v2_send.py` uses this to move Handoff V2 segment payloads over RDMA
while the frames that announce them — and everything that decides whether the
receiver accepts them — stay on the mTLS stream. See melon_rdma_bulk.h for the
lane's contract.
"""
from __future__ import annotations

import ctypes
import os
import sys

ENDPOINT_BYTES = 64
_ROLE_SENDER = 2
_OK = 0
_TIMEOUT = -2

_HERE = os.path.dirname(os.path.abspath(__file__))


def _library_path() -> str:
    name = "libmelon_rdma_bulk.dylib" if sys.platform == "darwin" else "libmelon_rdma_bulk.so"
    return os.environ.get("MELON_RDMA_BULK_LIB", os.path.join(_HERE, name))


class MelonBulkError(RuntimeError):
    pass


class MelonBulkTimeout(MelonBulkError):
    pass


_lib: ctypes.CDLL | None = None


def _load() -> ctypes.CDLL:
    global _lib
    if _lib is not None:
        return _lib
    path = _library_path()
    try:
        lib = ctypes.CDLL(path)
    except OSError as error:
        raise MelonBulkError(f"cannot load the RDMA bulk library {path}: {error}") from error
    vp, cp, u32p, u64p = ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_uint32), ctypes.POINTER(ctypes.c_uint64)
    lib.melon_bulk_create.argtypes = [cp, ctypes.c_int, ctypes.c_int, ctypes.c_uint64, cp, cp]
    lib.melon_bulk_create.restype = vp
    lib.melon_bulk_local_endpoint.argtypes = [vp, cp]
    lib.melon_bulk_local_endpoint.restype = ctypes.c_int
    lib.melon_bulk_connect.argtypes = [vp, cp]
    lib.melon_bulk_connect.restype = ctypes.c_int
    lib.melon_bulk_gid_index.argtypes = [vp]
    lib.melon_bulk_gid_index.restype = ctypes.c_int
    lib.melon_bulk_max_put.argtypes = [vp]
    lib.melon_bulk_max_put.restype = ctypes.c_uint64
    lib.melon_bulk_put.argtypes = [vp, cp, ctypes.c_uint64, u32p, u64p, ctypes.c_int]
    lib.melon_bulk_put.restype = ctypes.c_int
    lib.melon_bulk_flush.argtypes = [vp, ctypes.c_int]
    lib.melon_bulk_flush.restype = ctypes.c_int
    lib.melon_bulk_wire_ns.argtypes = [vp, ctypes.POINTER(ctypes.c_int)]
    lib.melon_bulk_wire_ns.restype = ctypes.c_uint64
    lib.melon_bulk_close.argtypes = [vp]
    lib.melon_bulk_close.restype = None
    lib.melon_bulk_last_error.argtypes = []
    lib.melon_bulk_last_error.restype = cp
    _lib = lib
    return lib


def _error() -> str:
    message = _load().melon_bulk_last_error()
    return message.decode("utf-8", "replace") if message else "unknown RDMA bulk error"


class MelonBulkSender:
    """One transfer's sending end of the lane. Not thread-safe."""

    def __init__(self, dev: str, gid_index: int) -> None:
        lib = _load()
        handle = lib.melon_bulk_create(dev.encode(), gid_index, _ROLE_SENDER, 0, None, None)
        if not handle:
            raise MelonBulkError(f"RDMA bulk sender on {dev}: {_error()}")
        self._lib = lib
        self._handle = handle
        self._connected = False

    def endpoint(self) -> bytes:
        out = ctypes.create_string_buffer(ENDPOINT_BYTES)
        self._lib.melon_bulk_local_endpoint(self._handle, out)
        return out.raw

    def connect(self, receiver_endpoint: bytes) -> None:
        if len(receiver_endpoint) != ENDPOINT_BYTES:
            raise MelonBulkError(f"receiver endpoint is {len(receiver_endpoint)} bytes")
        if self._lib.melon_bulk_connect(self._handle, receiver_endpoint) != _OK:
            raise MelonBulkError(f"RDMA bulk connect: {_error()}")
        self._connected = True

    @property
    def gid_index(self) -> int:
        return self._lib.melon_bulk_gid_index(self._handle)

    @property
    def max_put(self) -> int:
        return self._lib.melon_bulk_max_put(self._handle)

    def put(self, payload: bytes, timeout_ms: int) -> tuple[int, int]:
        """Posts one payload and returns (index, position) for its header.

        Returns once the write is posted, not once it lands; the receiver's
        credit is what keeps the ring from being overrun."""
        if not isinstance(payload, bytes):
            payload = bytes(payload)
        index = ctypes.c_uint32()
        position = ctypes.c_uint64()
        rc = self._lib.melon_bulk_put(
            self._handle, payload, len(payload), ctypes.byref(index), ctypes.byref(position), timeout_ms
        )
        if rc == _TIMEOUT:
            raise MelonBulkTimeout(_error())
        if rc != _OK:
            raise MelonBulkError(_error())
        return index.value, position.value

    def flush(self, timeout_ms: int) -> None:
        rc = self._lib.melon_bulk_flush(self._handle, timeout_ms)
        if rc == _TIMEOUT:
            raise MelonBulkTimeout(_error())
        if rc != _OK:
            raise MelonBulkError(_error())

    def wire_ns(self) -> tuple[int, bool]:
        """(time the link had this lane's bytes in flight, from NIC stamps?).

        The union of [post, completion] over every completed write: the RDMA
        counterpart of TCP_INFO busy time. Without NIC completion stamps it is
        stamped at reap time, which can only overstate it."""
        hardware = ctypes.c_int()
        value = self._lib.melon_bulk_wire_ns(self._handle, ctypes.byref(hardware))
        return int(value), bool(hardware.value)

    def close(self) -> None:
        if self._handle:
            self._lib.melon_bulk_close(self._handle)
            self._handle = None

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
