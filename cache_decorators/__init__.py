"""
Description.

hmm
"""
from importlib.metadata import version

from ._cache_protocols import (
    FileComparisonDecider,
    FileReadWrite,
    ReadWriteCSV,
    ReadWriteDelta,
    ReadWriteParquet,
    UpdateDecider,
)
from ._cachers import (
    Cacher,
    FileCacher,
)

__version__ = version(__package__)

__all__ = [
    "__version__",
    "Cacher",
    "FileCacher",
    "FileComparisonDecider",
    "FileReadWrite",
    "ReadWriteCSV",
    "ReadWriteDelta",
    "ReadWriteParquet",
    "UpdateDecider",
]
