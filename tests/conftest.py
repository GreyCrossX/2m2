import inspect
import pytest
import anyio
import functools

try:  # Prefer pytest-asyncio if available
    import pytest_asyncio  # type: ignore  # noqa: F401

    pytest_plugins = ("pytest_asyncio",)
except ImportError:  # Fallback to custom async wrapper below
    pytest_plugins: tuple[str, ...] = ()


def pytest_configure(config):
    # Ensure marker is registered even without pytest-asyncio installed
    config.addinivalue_line(
        "markers",
        "asyncio: async tests (aliases to anyio when pytest-asyncio is unavailable)",
    )


def pytest_pycollect_makeitem(collector, name, obj):
    """
    Allow collection of async test functions even when pytest-asyncio is missing
    by wrapping them into a sync function executed via anyio.run.
    """
    if inspect.iscoroutinefunction(obj) and name.startswith("test_"):
        async_fn = obj

        async_sig = inspect.signature(async_fn)

        def wrapper(*args, **kwargs):
            async def runner():
                return await async_fn(*args, **kwargs)

            return anyio.run(runner)

        # Preserve original signature so pytest can inject fixtures
        functools.update_wrapper(wrapper, async_fn)
        wrapper.__signature__ = async_sig  # type: ignore[attr-defined]

        item = pytest.Function.from_parent(collector, name=name, callobj=wrapper)
        item.add_marker(pytest.mark.anyio)
        return [item]


def pytest_collection_modifyitems(items):
    for item in items:
        if "asyncio" in item.keywords and "anyio" not in item.keywords:
            item.add_marker(pytest.mark.anyio)
