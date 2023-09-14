from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING, Any, ParamSpec, Protocol, TypeVar

import ibis

from ._utils import (
    PathTimeDelta,
    bind_args_to_kwargs,
    file_past_timeout,
    get_filelock,
    hash_func,
    hash_str,
    hide_file,
    t_since_last_mod,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractContextManager

    from ibis.expr.types import Table


P = ParamSpec("P")
R = TypeVar("R")


_LOGGER = logging.getLogger("cache_decorators")
_DEFAULT_DIR = (Path.cwd() / ".cache").resolve()
_FORCE_UPDATE = False


class UpdateDecider(Protocol):
    def __call__(self, r_id: str, **kwargs) -> bool: ...  # noqa: ANN003


class FileUpdateDecider(Protocol):
    def __call__(self, target_resource: Path, **kwargs) -> bool: ...  # noqa: ANN003


class Processor(Protocol):
    def pre_process(self, data) -> Any: ...  # noqa: ANN001, ANN401
    def post_process(self, tbl: Table) -> Table: ...


class NoopProcessor(Processor):
    def pre_process(self, data) -> Any:  # noqa: ANN001, ANN401
        return data

    def post_process(self, tbl: Table) -> Table:
        return tbl


class Validator(Protocol):
    def __call__(self, func: Callable, **kwargs) -> None: ...


class CacherBase(ABC):
    def __init__(
        self,
        decider: UpdateDecider,
        processor: Processor | None = None,
    ) -> None:
        self._should_update = decider
        self._processor = processor if processor is not None else NoopProcessor()

    def decorate(
        self,
        func: Callable[P, R],  # type:ignore[reportInvalidTypeVarUse]
    ) -> Callable[P, Table]:
        def uncache(*args: P.args, **kwargs: P.kwargs) -> None:
            self.do_uncache(func, *args, **kwargs)

        @wraps(func)
        def decorated(*args: P.args, **kwargs: P.kwargs) -> Table:
            return self.do_cache(func, *args, **kwargs)

        decorated.__setattr__("uncache", uncache)
        return decorated

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

    def do_cache(
        self,
        func: Callable[P, R],  # type:ignore[reportInvalidTypeVarUse]
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Table:
        bound_kwargs = bind_args_to_kwargs(
            func,
            *args,
            **kwargs,
        )
        r_id = hash_str(repr({"*func*": hash_func(func)} | bound_kwargs))
        with self._resource_context(r_id):
            if _FORCE_UPDATE or self._should_update(r_id, **bound_kwargs):
                data = self._processor.pre_process(
                    func(**bound_kwargs),  # type:ignore[reportGeneralTypeIssues]
                )
                self._write_cache(r_id, data)
            return self._processor.post_process(self._read_cache(r_id))

    def do_uncache(
        self,
        func: Callable[P, R],  # type:ignore[reportInvalidTypeVarUse]
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        bound_kwargs = bind_args_to_kwargs(
            func,
            *args,
            **kwargs,
        )
        r_id = hash_str(repr({"*func*": hash_func(func)} | bound_kwargs))
        with self._resource_context(r_id):
            self._uncache_resource(r_id)


class FileReadWrite(Protocol):
    @property
    def file_extension(self) -> str: ...
    def read_file(self, path: Path) -> Table: ...
    def write_file(self, path: Path, data) -> None: ...  # noqa: ANN001


class FileTimeoutDecider(FileUpdateDecider):
    def __init__(
        self,
        timeout: int = -1,
        delta_func: PathTimeDelta = t_since_last_mod,
    ) -> None:
        self.timeout = timeout
        self.delta_func = delta_func

    def __call__(self, target_resource: Path, **kwargs) -> bool:
        return file_past_timeout(
            target_resource,
            self.timeout,
            self.delta_func,
        )


class FileCacher(CacherBase):
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


class ReadWriteParquet(FileReadWrite):
    @property
    def file_extension(self) -> str:
        return ".parquet"

    def read_file(self, path: Path) -> Table:
        return ibis.read_parquet(path)

    def write_file(self, path: Path, data) -> None:  # noqa: ANN001
        data.to_parquet(path)


class ReadWriteCSV(FileReadWrite):
    @property
    def file_extension(self) -> str:
        return ".csv"

    def read_file(self, path: Path) -> Table:
        return ibis.read_csv(path)

    def write_file(self, path: Path, data) -> None:  # noqa: ANN001
        data.to_csv(path)


class ReadWriteDelta(FileReadWrite):
    @property
    def file_extension(self) -> str:
        return ".delta"

    def read_file(self, path: Path) -> Table:
        return ibis.read_delta(path)

    def write_file(self, path: Path, data) -> None:  # noqa: ANN001
        data.to_delta(path)


__all__ = [
    "_DEFAULT_DIR",
    "CacherBase",
    "FileCacher",
    "FileTimeoutDecider",
    "FileReadWrite",
    "ReadWriteParquet",
    "ReadWriteCSV",
    "ReadWriteDelta",
]
