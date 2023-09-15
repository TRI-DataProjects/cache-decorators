from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

import pytest
from cache_decorators import _hashing

if TYPE_CHECKING:
    from typing import Any, Callable, Generator


@pytest.mark.parametrize(
    ("string", "str_hash"),
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
def test_hash_str_md5(string: str, str_hash: str) -> None:
    assert _hashing.hash_str(string, hashlib.md5) == str_hash


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


def test_hash_func_md5() -> None:
    for func, md5_hash in __function_generator():
        assert _hashing.hash_func(func, hashlib.md5) == md5_hash
