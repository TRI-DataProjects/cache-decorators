from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import ibis

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from ibis.expr.types import Table


class BaseCacher(ABC):
    def __init__(
        self,
        pre_process: Callable | None = None,
        post_process: Callable[[Table], Table] | None = None,
    ) -> None:
        self._pre_process = pre_process
        self._post_process = post_process

    def pre_process(self, data):  # noqa: ANN001, ANN202
        if self._pre_process is None:
            return data
        return self._pre_process(data)

    def post_process(self, t: Table) -> Table:
        if self._post_process is None:
            return t
        return self._post_process(t)

    @property
    @abstractmethod
    def file_extension(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def lazy_read(self, cache_file: Path) -> Table:
        raise NotImplementedError

    @abstractmethod
    def eager_write(self, cache_file: Path, data) -> None:  # noqa: ANN001
        raise NotImplementedError


class CSVCacher(BaseCacher):
    @property
    def file_extension(self) -> str:
        return ".csv"

    def lazy_read(self, cache_file: Path) -> Table:
        return ibis.read_csv(cache_file)

    def eager_write(self, cache_file: Path, data) -> None:  # noqa: ANN001
        data.to_csv(cache_file)


class DeltaCacher(BaseCacher):
    @property
    def file_extension(self) -> str:
        return ".delta"

    def lazy_read(self, cache_file: Path) -> Table:
        return ibis.read_delta(cache_file)

    def eager_write(self, cache_file: Path, data) -> None:  # noqa: ANN001
        data.to_delta(cache_file)


class JSONCacher(BaseCacher):
    @property
    def file_extension(self) -> str:
        return ".json"

    def lazy_read(self, cache_file: Path) -> Table:
        return ibis.read_json(cache_file)

    def eager_write(self, cache_file: Path, data) -> None:  # noqa: ANN001
        with cache_file.open("w") as f:
            json.dump(data, f)


class ParquetCacher(BaseCacher):
    @property
    def file_extension(self) -> str:
        return ".parquet"

    def lazy_read(self, cache_file: Path) -> Table:
        return ibis.read_parquet(cache_file)

    def eager_write(self, cache_file: Path, data) -> None:  # noqa: ANN001
        data.to_parquet(cache_file)


__all__ = [
    "BaseCacher",
    "CSVCacher",
    "DeltaCacher",
    "JSONCacher",
    "ParquetCacher",
]
