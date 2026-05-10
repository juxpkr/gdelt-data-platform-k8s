import pytest
import cache


def test_set_and_get():
    cache.set_cached("k1", {"data": 42})
    assert cache.get_cached("k1") == {"data": 42}


def test_get_miss_returns_none():
    assert cache.get_cached("nonexistent") is None


def test_clear_removes_all_entries():
    cache.set_cached("a", 1)
    cache.set_cached("b", 2)
    cache.clear()
    assert cache.get_cached("a") is None
    assert cache.get_cached("b") is None


def test_make_key_is_deterministic():
    k1 = cache.make_key("signals", limit=30)
    k2 = cache.make_key("signals", limit=30)
    assert k1 == k2


def test_make_key_differs_by_param():
    k1 = cache.make_key("events", limit=50, offset=0)
    k2 = cache.make_key("events", limit=50, offset=50)
    assert k1 != k2


def test_make_key_differs_by_prefix():
    k1 = cache.make_key("stats")
    k2 = cache.make_key("batches")
    assert k1 != k2


def test_autouse_fixture_isolates_between_tests(request):
    # fixture가 테스트 간 cache를 비워줬으므로 이전 테스트 값이 보이면 안 된다
    assert cache.get_cached("k1") is None


def test_overwrite_existing_key():
    cache.set_cached("x", "first")
    cache.set_cached("x", "second")
    assert cache.get_cached("x") == "second"
