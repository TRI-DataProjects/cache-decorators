from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING

from public import public

from ._cache_protocols import (
    FileReadWrite,
    FileTimeoutDecider,
    FileUpdateDecider,
    Processor,
    ReadWriteParquet,
    UpdateDecider,
)
from ._files import get_filelock, hide_file
from ._hashing import hash_func, hash_str
from ._inspection import bind_to_kwargs

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractContextManager
    from typing import Any, ParamSpec, TypeVar

    from ibis.expr.types import Table

    P = ParamSpec("P")
    R = TypeVar("R")


_LOGGER = logging.getLogger("cache_decorators")
_DEFAULT_DIR = (Path.cwd() / ".cache").resolve()
public(_DEFAULT_DIR=_DEFAULT_DIR)
_FORCE_UPDATE = False
public(_FORCE_UPDATE=_FORCE_UPDATE)


@public
class Cacher(ABC):
    def __init__(
        self,
        decider: UpdateDecider,
        processor: Processor | None = None,
    ) -> None:
        self._should_update = decider
        self._processor = processor

    def __call__(
        self,
        func: Callable[P, R],  # type:ignore[reportInvalidTypeVarUse]
    ) -> Callable[P, Table]:
        def uncache(*args: P.args, **kwargs: P.kwargs) -> None:
            self._do_uncache(func, *args, **kwargs)

        @wraps(func)
        def decorated(*args: P.args, **kwargs: P.kwargs) -> Table:
            return self._do_cache(func, *args, **kwargs)

        decorated.__setattr__("uncache", uncache)
        return decorated

    def _preprocess(self, data) -> Any:  # noqa: ANN001, ANN401
        if not self._processor:
            return data
        return self._processor.preprocess(data)

    def _postprocess(self, tbl: Table) -> Table:
        if not self._processor:
            return tbl
        return self._processor.postprocess(tbl)

    @abstractmethod
    def _resource_context(
        self,
        r_id: str,
        timeout: float = -1,
    ) -> AbstractContextManager:
        raise NotImplementedError

    # @abstractmethod
    # def _resource_stats(
    #     self,
    #     r_id: str,
    # ) -> ResourceStats:
    #     raise NotImplementedError

    @abstractmethod
    def _uncache_resource(
        self,
        r_id: str,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def _read_cache(
        self,
        r_id: str,
    ) -> Table:
        raise NotImplementedError

    @abstractmethod
    def _write_cache(
        self,
        r_id: str,
        data,  # noqa: ANN001
    ) -> None:
        raise NotImplementedError

    def _do_cache(
        self,
        func: Callable[P, R],  # type:ignore[reportInvalidTypeVarUse]
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Table:
        bound_kwargs = bind_to_kwargs(
            func,
            *args,
            **kwargs,
        )
        r_id = hash_str(repr({"*func*": hash_func(func)} | bound_kwargs))
        with self._resource_context(r_id):
            if _FORCE_UPDATE or self._should_update(r_id, **bound_kwargs):
                data = self._preprocess(
                    func(**bound_kwargs),  # type:ignore[reportGeneralTypeIssues]
                )
                self._write_cache(r_id, data)
            return self._postprocess(self._read_cache(r_id))

    def _do_uncache(
        self,
        func: Callable[P, R],  # type:ignore[reportInvalidTypeVarUse]
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        bound_kwargs = bind_to_kwargs(
            func,
            *args,
            **kwargs,
        )
        r_id = hash_str(repr({"*func*": hash_func(func)} | bound_kwargs))
        with self._resource_context(r_id):
            self._uncache_resource(r_id)


@public
class FileCacher(Cacher):
    def __init__(
        self,
        cache_dir: Path | str | None = None,
        read_write: FileReadWrite | None = None,
        update_decider: FileUpdateDecider | None = None,
        processor: Processor | None = None,
    ) -> None:
        self._cache_dir: Path = self._validate_cache_dir(cache_dir)
        self._read_write: FileReadWrite = (
            read_write if read_write is not None else ReadWriteParquet()
        )
        decider = self._wrap_decider(
            (update_decider if update_decider is not None else FileTimeoutDecider()),
        )
        super().__init__(decider, processor)

    def _validate_cache_dir(self, cache_dir: Path | str | None) -> Path:
        cache_dir = Path(cache_dir).resolve() if cache_dir else _DEFAULT_DIR
        if not cache_dir.exists():
            _LOGGER.debug("Making and hiding cache dir '%s'", cache_dir)
            cache_dir.mkdir(parents=True)
            cache_dir = hide_file(cache_dir)
        return cache_dir

    def _wrap_decider(self, fud: FileUpdateDecider) -> UpdateDecider:
        def wrapper(r_id: str, **kwargs) -> bool:  # noqa: ANN003
            target_file = self._path_from_resource_id(r_id)
            return fud(target_file, **kwargs)

        return wrapper

    def _path_from_resource_id(self, r_id: str) -> Path:
        return self._cache_dir / f"{r_id}{self._read_write.file_extension}"

    def _resource_context(
        self,
        r_id: str,
        timeout: float = -1,
    ) -> AbstractContextManager:
        target_file = self._path_from_resource_id(r_id)
        return get_filelock(target_file, timeout)

    def _read_cache(self, r_id: str) -> Table:
        return self._read_write.read_file(self._path_from_resource_id(r_id))

    def _write_cache(self, r_id: str, data) -> None:  # noqa: ANN001
        return self._read_write.write_file(self._path_from_resource_id(r_id), data)

    def _uncache_resource(self, r_id: str) -> None:
        self._path_from_resource_id(r_id).unlink()
