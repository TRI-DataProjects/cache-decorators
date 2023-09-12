from __future__ import annotations

from typing import Any, Callable, Generator

import pytest
from cache_decorators import _utils as utils


@pytest.mark.parametrize(
    ("string", "md5_hash"),
    [
        ("Hello, World!", "65a8e27d8879283831b664bd8b7f0ad4"),
        ("I must not fear", "3ec9241eabe6b373e8b4fcfb5ba81312"),
        (
            (
                "Sunt quo quas vel. Et ducimus eum dolores aut. Ratione soluta magnam"
                " consequatur sint quasi doloremque eius. Quos facere aperiam corrupti"
                " qui aut. Sequi hic sed velit. Facilis et est quasi et consequatur"
                " qui."
            ),
            "b050678730a9fb5dd792cd499fb5aed0",
        ),
        pytest.param("6*9", "420", marks=pytest.mark.xfail),
    ],
)
def test_hash_str(string: str, md5_hash: str) -> None:
    assert utils.hash_str(string) == md5_hash


def __function_generator() -> Generator[tuple[Callable, str], Any, None]:
    def func_a() -> str:
        return "Hello, World!"

    def func_b(x: int) -> int:
        return x + 1

    def func_c(y: float) -> float:
        return y**y

    yield func_a, "829296c6efc222f845bd6fb540da6851"
    yield func_b, "76026ebc676138503ec041aed5a567a9"
    yield func_c, "c6b418b17594deee285cdecc7fcd33b9"


def test_hash_func() -> None:
    for func, md5_hash in __function_generator():
        assert utils.hash_func(func) == md5_hash
