import pytest
import cache as _cache_module


@pytest.fixture(autouse=True)
def clear_api_cache():
    _cache_module.clear()
    yield
    _cache_module.clear()
