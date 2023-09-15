"""
Description.

hmm
"""
from importlib.metadata import version

from ._cachers import (
    CacherBase,
    FileCacher,
)
from ._protocols import (
    FileComparisonDecider,
    FileReadWrite,
    ReadWriteCSV,
    ReadWriteDelta,
    ReadWriteParquet,
)

__version__ = version(__package__)

__all__ = [
    "__version__",
    "CacherBase",
    "FileCacher",
    "FileComparisonDecider",
    "FileReadWrite",
    "ReadWriteCSV",
    "ReadWriteDelta",
    "ReadWriteParquet",
]
