import json
import logging
from typing import Any, Optional
from cachetools import TTLCache

logger = logging.getLogger(__name__)

_cache: TTLCache = TTLCache(maxsize=200, ttl=60)


def make_key(prefix: str, **kwargs: Any) -> str:
    return f"{prefix}:{json.dumps(kwargs, sort_keys=True, default=str)}"


def get_cached(key: str) -> Optional[Any]:
    val = _cache.get(key)
    if val is not None:
        logger.debug("cache hit: %s", key)
    return val


def set_cached(key: str, value: Any) -> None:
    _cache[key] = value


def clear() -> None:
    _cache.clear()
