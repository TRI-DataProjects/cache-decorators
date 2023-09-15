from __future__ import annotations

import inspect
import logging
from functools import lru_cache
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Callable, ParamSpec

    P = ParamSpec("P")

_LOGGER = logging.getLogger("cache_decorators")


@lru_cache
def bind_to_kwargs(
    func: Callable[P, Any],
    *args: P.args,
    **kwargs: P.kwargs,
) -> dict[str, Any]:
    kw_out = inspect.signature(func).bind(*args, **kwargs).arguments | kwargs
    _LOGGER.debug(
        "Bound '%s'(*%s,**%s) to '%s'(%s)",
        func.__name__,
        args,
        kwargs,
        func.__name__,
        kw_out,
    )
    return kw_out
