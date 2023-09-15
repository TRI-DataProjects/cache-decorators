from __future__ import annotations

import hashlib
import inspect
from functools import lru_cache
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from hashlib import _Hash
    from typing import Callable

    from _typeshed import ReadableBuffer


class HashingProtocol(Protocol):
    def __call__(
        self,
        string: ReadableBuffer = b"",
        *,
        usedforsecurity: bool = True,
    ) -> _Hash: ...


def hash_str(string: str, hasher: HashingProtocol = hashlib.sha256) -> str:
    return hasher(string.encode("utf-8"), usedforsecurity=False).hexdigest()


@lru_cache
def hash_func(func: Callable, hasher: HashingProtocol = hashlib.sha256) -> str:
    src = inspect.getsource(func)
    return hasher(src.encode("utf-8"), usedforsecurity=False).hexdigest()
