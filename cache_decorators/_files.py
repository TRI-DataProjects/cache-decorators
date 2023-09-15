from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from errno import ENOENT
from typing import TYPE_CHECKING, Protocol

from filelock import FileLock

if TYPE_CHECKING:
    from datetime import timedelta
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


class PathTimeDelta(Protocol):
    def __call__(self, path: Path) -> timedelta: ...


def t_since_last_mod(path: Path) -> timedelta:
    m_time = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    cur_time = datetime.now(tz=timezone.utc)
    return cur_time - m_time


def t_since_last_access(path: Path) -> timedelta:
    a_time = datetime.fromtimestamp(path.stat().st_atime, tz=timezone.utc)
    cur_time = datetime.now(tz=timezone.utc)
    return cur_time - a_time


def file_past_timeout(
    path: Path,
    timeout: int,
    delta_func: PathTimeDelta = t_since_last_mod,
) -> bool:
    if not path.exists():
        return True
    if timeout < 0:
        return False
    elapsed = delta_func(path).seconds
    return elapsed > timeout


def get_filelock(target_file: Path, timeout: float = -1) -> FileLock:
    lock_file = target_file.parent / (target_file.name + ".lock")
    return FileLock(lock_file=lock_file, timeout=timeout)
