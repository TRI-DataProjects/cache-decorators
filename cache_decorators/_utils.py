from __future__ import annotations

import hashlib
import inspect
import logging
import os
import sys
from errno import ENOENT
from typing import TYPE_CHECKING, Any, Callable, ParamSpec

from filelock import FileLock

if TYPE_CHECKING:
    from pathlib import Path

if sys.platform == "win32":
    from ctypes import WinError, windll
    from stat import FILE_ATTRIBUTE_HIDDEN

    def win_file_hidden(path: Path) -> bool:
        attr = path.stat().st_file_attributes
        return attr & FILE_ATTRIBUTE_HIDDEN == FILE_ATTRIBUTE_HIDDEN

    def win_hide_file(path: Path) -> None:
        # SetFileAttributesW returns False when an error occurs
        if not windll.kernel32.SetFileAttributesW(
            str(path.resolve()),
            FILE_ATTRIBUTE_HIDDEN,
        ):
            raise WinError()


_LOGGER = logging.getLogger("cache_decorators")


def hide_file(path: Path) -> Path:
    path = path.resolve()
    if not path.exists():
        raise FileNotFoundError(ENOENT, os.strerror(ENOENT), str(path))

    # UNIX like systems hide files that begin with "."
    if not path.name.startswith("."):
        new_path = path.parent / ("." + path.name)
        path = path.rename(new_path)

    # Set file attributes on win machines
    if sys.platform == "win32" and not win_file_hidden(path):
        win_hide_file(path)

    return path


def get_filelock(target_file: Path, timeout: float = -1) -> FileLock:
    lock_file = target_file.parent / (target_file.name + ".lock")
    return FileLock(lock_file=lock_file, timeout=timeout)


def hash_str(string: str) -> str:
    return hashlib.md5(string.encode("utf-8"), usedforsecurity=False).hexdigest()


def hash_func(func: Callable) -> str:
    src = inspect.getsource(func)
    return hashlib.md5(src.encode("utf-8"), usedforsecurity=False).hexdigest()


P = ParamSpec("P")


def bind_args_to_kwargs(
    func: Callable[P, Any],
    *args: P.args,
    **kwargs: P.kwargs,
) -> dict[str, Any]:
    signature = inspect.signature(func)
    bound_args = signature.bind(*args)
    kw_out = bound_args.arguments | kwargs
    _LOGGER.debug(
        "Mapped '%s'(*%s,**%s) to '%s'(%s)",
        func.__name__,
        args,
        kwargs,
        func.__name__,
        kw_out,
    )
    return kw_out


__all__ = [
    "hide_file",
    "get_filelock",
    "hash_str",
    "hash_func",
    "bind_args_to_kwargs",
]
