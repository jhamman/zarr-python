"""
Tests for custom scheme handlers and URL router functionality.

This module tests the two-tier URL resolution system, including custom
scheme handlers and their integration with the URL router.
"""

from typing import Any

import pytest

from zarr.abc.scheme_handler import CustomSchemeHandler
from zarr.storage._scheme_registry import CustomSchemeRegistry
from zarr.storage._url_router import URLRouter, get_url_router, register_custom_scheme_handler


class MockStore:
    """Mock store for testing."""

    def __init__(self, url: str, **kwargs: Any) -> None:
        self.url = url
        self.kwargs = kwargs


class TestSchemeHandler(CustomSchemeHandler):
    """Test custom scheme handler."""

    scheme = "test"

    @classmethod
    async def from_url(cls, url: str, **kwargs: Any) -> "MockStore":  # type: ignore[override]
        return MockStore(url, **kwargs)


class TestCustomSchemeRegistry:
    """Test CustomSchemeRegistry functionality."""

    def test_register_scheme(self) -> None:
        """Test scheme registration."""
        registry = CustomSchemeRegistry()
        registry.register_scheme("test", TestSchemeHandler)

        handler = registry.get_handler("test")
        assert handler == TestSchemeHandler

    def test_register_invalid_scheme(self) -> None:
        """Test registration of invalid schemes."""
        registry = CustomSchemeRegistry()

        with pytest.raises(ValueError, match="Invalid scheme"):
            registry.register_scheme("", TestSchemeHandler)

        with pytest.raises(ValueError, match="Invalid scheme format"):
            registry.register_scheme("123invalid", TestSchemeHandler)

    def test_register_duplicate_scheme_same_handler(self) -> None:
        """Test re-registering same handler is OK."""
        registry = CustomSchemeRegistry()
        registry.register_scheme("test", TestSchemeHandler)
        registry.register_scheme("test", TestSchemeHandler)  # Should not raise

    def test_register_duplicate_scheme_different_handler(self) -> None:
        """Test registering different handler for same scheme fails."""
        registry = CustomSchemeRegistry()
        registry.register_scheme("test", TestSchemeHandler)

        class AnotherHandler(CustomSchemeHandler):
            scheme = "test"

            @classmethod
            async def from_url(cls, url: str, **kwargs: Any) -> "MockStore":  # type: ignore[override]
                return MockStore(url, **kwargs)

        with pytest.raises(ValueError, match="already registered"):
            registry.register_scheme("test", AnotherHandler)

    def test_can_handle(self) -> None:
        """Test URL scheme detection."""
        registry = CustomSchemeRegistry()
        registry.register_scheme("test", TestSchemeHandler)

        assert registry.can_handle("test://example.com")
        assert not registry.can_handle("http://example.com")
        assert not registry.can_handle("invalid-url")

    @pytest.mark.asyncio
    async def test_resolve(self) -> None:
        """Test URL resolution."""
        registry = CustomSchemeRegistry()
        registry.register_scheme("test", TestSchemeHandler)

        store = await registry.resolve("test://example.com", mode="r")
        assert isinstance(store, MockStore)
        assert store.url == "test://example.com"
        assert store.kwargs["mode"] == "r"

    @pytest.mark.asyncio
    async def test_resolve_unknown_scheme(self) -> None:
        """Test resolution of unknown scheme."""
        registry = CustomSchemeRegistry()

        with pytest.raises(ValueError, match="No handler registered"):
            await registry.resolve("unknown://example.com")


class TestURLRouter:
    """Test URLRouter functionality."""

    def test_router_initialization(self) -> None:
        """Test router initializes correctly."""
        router = URLRouter()
        assert router.custom_schemes is not None
        assert router.zep8_resolver is not None

    @pytest.mark.asyncio
    async def test_custom_scheme_routing(self) -> None:
        """Test routing to custom scheme handlers."""
        router = URLRouter()
        router.custom_schemes.register_scheme("test", TestSchemeHandler)

        store = await router.resolve("test://example.com/path", mode="r")
        assert isinstance(store, MockStore)
        assert store.url == "test://example.com/path"

    def test_register_scheme_invalid_handler(self) -> None:
        """Test registering invalid handler type."""
        router = URLRouter()

        class NotAHandler:
            pass

        with pytest.raises(TypeError, match="CustomSchemeHandler subclass"):
            router.register_scheme(NotAHandler)

    def test_path_extraction_custom_scheme(self) -> None:
        """Test path extraction from custom scheme URLs."""
        router = URLRouter()

        # Test basic path extraction
        path = router.extract_path("test://example.com/path/to/data")
        assert path == "path/to/data"

        # Test root path
        path = router.extract_path("test://example.com")
        assert path == ""

        path = router.extract_path("test://example.com/")
        assert path == ""

    def test_path_extraction_arraylake_urls(self) -> None:
        """Test path extraction for Arraylake-style URLs."""
        router = URLRouter()

        # Test URL without reference
        path = router.extract_path("al://org/repo/data/array")
        assert path == "data/array"

        # Test URL with branch reference
        path = router.extract_path("al://org/repo@branch.main/data/array")
        assert path == "data/array"

        # Test URL with tag reference
        path = router.extract_path("al://org/repo@tag.v123/experiment/results")
        assert path == "experiment/results"

        # Test URL with reference but no path
        path = router.extract_path("al://org/repo@branch.main")
        assert path == ""

        # Test URL without path at all
        path = router.extract_path("al://org/repo")
        assert path == ""


class TestGlobalRegistration:
    """Test global scheme registration functionality."""

    def test_get_global_router(self) -> None:
        """Test getting the global router instance."""
        router1 = get_url_router()
        router2 = get_url_router()
        assert router1 is router2  # Should be same instance

    def test_register_custom_scheme_handler(self) -> None:
        """Test global scheme handler registration."""
        # Clean state (router might have handlers from other tests)
        router = get_url_router()

        # Register our test handler
        register_custom_scheme_handler(TestSchemeHandler)

        # Should be able to handle test URLs
        assert router.custom_schemes.can_handle("test://example.com")

        # Should be able to get the handler
        handler = router.custom_schemes.get_handler("test")
        assert handler == TestSchemeHandler


class TestErrorHandling:
    """Test error handling in custom scheme system."""

    @pytest.mark.asyncio
    async def test_handler_exception_handling(self) -> None:
        """Test that handler exceptions are properly wrapped."""

        class FailingHandler(CustomSchemeHandler):
            scheme = "fail"

            @classmethod
            async def from_url(cls, url: str, **kwargs: Any) -> "MockStore":  # type: ignore[override]
                raise RuntimeError("Handler failed")

        registry = CustomSchemeRegistry()
        registry.register_scheme("fail", FailingHandler)

        with pytest.raises(ValueError, match="Failed to resolve URL"):
            await registry.resolve("fail://example.com")

    def test_invalid_url_handling(self) -> None:
        """Test handling of malformed URLs."""
        registry = CustomSchemeRegistry()

        # Should handle gracefully
        assert not registry.can_handle("not-a-url")
        assert not registry.can_handle("")
        assert not registry.can_handle("://missing-scheme")
