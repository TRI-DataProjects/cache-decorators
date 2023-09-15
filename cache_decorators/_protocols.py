from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

import ibis
from public import public

from ._utils import (
    PathTimeDelta,
    file_past_timeout,
    t_since_last_mod,
)

if TYPE_CHECKING:
    from ibis.expr.types import Table


@public
class UpdateDecider(Protocol):
    def __call__(self, r_id: str, **kwargs) -> bool: ...


@public
class FileUpdateDecider(Protocol):
    def __call__(self, target_resource: Path, **kwargs) -> bool: ...


@public
class FileTimeoutDecider(FileUpdateDecider):
    def __init__(
        self,
        timeout: int = -1,
        delta_func: PathTimeDelta = t_since_last_mod,
    ) -> None:
        self.timeout = timeout
        self.delta_func = delta_func

    def __call__(self, target_resource: Path, **kwargs) -> bool:  # noqa: ARG002
        return file_past_timeout(
            target_resource,
            self.timeout,
            self.delta_func,
        )


@public
class FileComparisonDecider(FileUpdateDecider):
    def __init__(self, input_file_kwd: str) -> None:
        self.input_file_kwd = input_file_kwd

    def __call__(self, target_resource: Path, **kwargs) -> bool:
        input_file = Path(kwargs[self.input_file_kwd])
        return (not target_resource.exists()) or (
            t_since_last_mod(input_file) < t_since_last_mod(target_resource)
        )


@public
class Processor(Protocol):
    def preprocess(self, data) -> Any: ...  # noqa: ANN001, ANN401
    def postprocess(self, tbl: Table) -> Table: ...


@public
class FileReadWrite(Protocol):
    @property
    def file_extension(self) -> str: ...
    def read_file(self, path: Path) -> Table: ...
    def write_file(self, path: Path, data) -> None: ...  # noqa: ANN001


@public
class ReadWriteParquet(FileReadWrite):
    @property
    def file_extension(self) -> str:
        return ".parquet"

    def read_file(self, path: Path) -> Table:
        return ibis.read_parquet(path)

    def write_file(self, path: Path, data) -> None:  # noqa: ANN001
        data.to_parquet(path)


@public
class ReadWriteCSV(FileReadWrite):
    @property
    def file_extension(self) -> str:
        return ".csv"

    def read_file(self, path: Path) -> Table:
        return ibis.read_csv(path)

    def write_file(self, path: Path, data) -> None:  # noqa: ANN001
        data.to_csv(path)


@public
class ReadWriteDelta(FileReadWrite):
    @property
    def file_extension(self) -> str:
        return ".delta"

    def read_file(self, path: Path) -> Table:
        return ibis.read_delta(path)

    def write_file(self, path: Path, data) -> None:  # noqa: ANN001
        data.to_delta(path)
