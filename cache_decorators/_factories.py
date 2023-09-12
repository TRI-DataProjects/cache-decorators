from __future__ import annotations

import logging
from functools import partial, wraps
from pathlib import Path
from typing import TYPE_CHECKING, ParamSpec, TypeVar

from ._cachers import BaseCacher, ParquetCacher
from ._utils import bind_args_to_kwargs, get_filelock, hash_func, hash_str, hide_file

if TYPE_CHECKING:
    from collections.abc import Callable

    from ibis.expr.types import Table

P = ParamSpec("P")
R = TypeVar("R")


_DEFAULT_CACHER = ParquetCacher()
_DEFAULT_DIR = Path.cwd() / ".cache"
_FORCE_UPDATE = False
_LOGGER = logging.getLogger("cache_decorators")


def _uncache_dir(func_dir: Path) -> None:
    if not func_dir.exists():
        return
    _LOGGER.debug("Uncaching dir '%s'", func_dir)
    try:
        for target_file in func_dir.glob("*"):
            with get_filelock(target_file, 0):
                target_file.unlink()
    except OSError:
        pass
    else:
        func_dir.rmdir()


def _uncache_file(target_file: Path) -> None:
    if not target_file.exists():
        return
    _LOGGER.debug("Uncaching file '%s'", target_file)
    with get_filelock(target_file):
        target_file.unlink()


def decorate_function(
    func: Callable[P, R],  # type:ignore[reportInvalidTypeVarUse]
    cache_dir: Path,
    cacher: BaseCacher,
    update_decider: Callable[[Path], bool],
) -> Callable[P, Table]:
    func_dir = cache_dir / hash_func(func)
    func_dir.mkdir(exist_ok=True)

    def target_file(**kwargs) -> Path:
        args_hash = hash_str(repr(kwargs))
        return func_dir / f"{args_hash}{cacher.file_extension}"

    def uncache(*args: P.args, **kwargs: P.kwargs) -> None:
        if args or kwargs:
            kwargs = bind_args_to_kwargs(
                func,
                *args,
                **kwargs,
            )  # type:ignore[reportGeneralTypeIssues]
            _uncache_file(target_file(**kwargs))
        else:
            _uncache_dir(func_dir)

    @wraps(func)
    def cache_wrapper(*args: P.args, **kwargs: P.kwargs) -> Table:
        kwargs = bind_args_to_kwargs(
            func,
            *args,
            **kwargs,
        )  # type:ignore[reportGeneralTypeIssues]
        _target_file = target_file(**kwargs)

        _LOGGER.debug("Wrapping '%s': '%s'", func.__name__, func_dir)
        _LOGGER.debug("Acquiring lock on '%s'", _target_file)
        with get_filelock(_target_file):
            if _FORCE_UPDATE or update_decider(_target_file, **kwargs):
                data = cacher.pre_process(
                    func(**kwargs),  # type:ignore[reportGeneralTypeIssues]
                )
                cacher.eager_write(_target_file, data)
            t = cacher.lazy_read(_target_file)
        _LOGGER.debug("Released lock on '%s'", _target_file)

        return cacher.post_process(t)

    cache_wrapper.__setattr__("uncache", uncache)
    return cache_wrapper


def validate_cache_dir(cache_dir: Path | str | None) -> Path:
    cache_dir = Path(cache_dir) if cache_dir else _DEFAULT_DIR
    if not cache_dir.exists():
        _LOGGER.debug("Making and hiding cache dir '%s'", cache_dir)
        cache_dir.mkdir(parents=True)
        cache_dir = hide_file(cache_dir)
    return cache_dir


class CacheDecoratorFactory:
    def __init__(
        self,
        *,
        cacher: BaseCacher | None = None,
        cache_dir: Path | str | None = None,
    ) -> None:
        self.cacher = cacher if cacher else _DEFAULT_CACHER
        self.cache_dir = validate_cache_dir(cache_dir)

    def decider(self, target_path: Path, **kwargs) -> bool:  # noqa: ARG002
        return not target_path.exists()

    def __call__(
        self,
        func: Callable[P, R],  # type:ignore[reportInvalidTypeVarUse]
    ) -> Callable[P, Table]:
        return decorate_function(
            func,
            self.cache_dir,
            self.cacher,
            self.decider,
        )


class CachedFileReaderDecoratorFactory:
    def __init__(
        self,
        *,
        path_kwd: str,
        cacher: BaseCacher | None = None,
        cache_dir: Path | str | None = None,
    ) -> None:
        self.path_kwd = path_kwd
        self.cacher = cacher if cacher else _DEFAULT_CACHER
        self.cache_dir = validate_cache_dir(cache_dir)

    def decider(self, func: Callable, target_path: Path, **kwargs) -> bool:
        arg = kwargs.get(self.path_kwd, None)
        if arg is None:
            msg = (
                f"{self.__class__.__name__} expected a pathlike keyword argument"
                f" '{self.path_kwd}' be passed to the wrapped function"
                f" '{func.__name__}'."
            )
            raise TypeError(msg) from None

        try:
            input_file = Path(arg)
        except TypeError as e:
            msg = f"Could not convert '{arg}' to '{Path.__name__}'."
            raise TypeError(msg) from e

        return (
            not target_path.exists()
            or input_file.stat().st_mtime > target_path.stat().st_mtime
        )

    def __call__(
        self,
        func: Callable[P, R],  # type:ignore[reportInvalidTypeVarUse]
    ) -> Callable[P, Table]:
        decider = partial(self.decider, func)
        return decorate_function(
            func,
            self.cache_dir,
            self.cacher,
            decider,
        )


__all__ = [
    "decorate_function",
    "validate_cache_dir",
    "CacheDecoratorFactory",
    "CachedFileReaderDecoratorFactory",
]
