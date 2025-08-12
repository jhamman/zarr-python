"""
Integration tests for the complete custom scheme handler system.

This module tests the end-to-end integration of custom scheme handlers
with zarr-python's URL resolution system.
"""

from typing import Any

import pytest

from zarr.abc.scheme_handler import CustomSchemeHandler
from zarr.storage import register_custom_scheme_handler
from zarr.storage._common import make_store_path


class MockStore:
    """Mock store for integration testing."""

    def __init__(self, url: str, **kwargs: Any) -> None:
        self.url = url
        self.kwargs = kwargs
        # Default to writable unless specifically requested as read-only
        mode = kwargs.get("mode", "r")
        self.read_only = kwargs.get("read_only", mode == "r")

    async def _ensure_open(self) -> None:
        """Mock store open."""

    async def delete_dir(self, path: str = "") -> None:
        """Mock delete directory operation."""

    def with_read_only(self, read_only: bool) -> "MockStore":
        """Mock read-only copy."""
        new_kwargs = self.kwargs.copy()
        new_kwargs["read_only"] = read_only
        return MockStore(self.url, **new_kwargs)


class IntegrationSchemeHandler(CustomSchemeHandler):
    """Integration test scheme handler."""

    scheme = "integration"

    @classmethod
    async def from_url(cls, url: str, **kwargs: Any) -> "MockStore":  # type: ignore[override]
        # Simple mock that returns our test store
        return MockStore(url, **kwargs)


class TestIntegration:
    """Test end-to-end integration of custom scheme handlers."""

    def setup_method(self) -> None:
        """Register our test handler for each test."""
        register_custom_scheme_handler(IntegrationSchemeHandler)

    @pytest.mark.asyncio
    async def test_make_store_path_with_custom_scheme(self) -> None:
        """Test that make_store_path works with custom scheme handlers."""

        # Test URL resolution through make_store_path
        store_path = await make_store_path(
            "integration://example.com/repo/data", path="array", mode="r"
        )

        # Verify the store was created
        assert isinstance(store_path.store, MockStore)  # type: ignore[unreachable]
        assert store_path.store.url == "integration://example.com/repo/data"  # type: ignore[unreachable]
        assert store_path.store.kwargs["mode"] == "r"

        # Verify path handling - when we provide both URL path and explicit path,
        # they get combined: "repo/data" (from URL) + "array" (explicit) = "repo/data/array"
        assert store_path.path == "repo/data/array"

    @pytest.mark.asyncio
    async def test_path_extraction_integration(self) -> None:
        """Test path extraction works correctly with custom schemes."""

        # Test without path - "integration://example.com/repo" extracts "repo" as path
        store_path = await make_store_path("integration://example.com/repo", mode="r")
        assert store_path.path == "repo"  # Extracted from URL path component

        # Test with path - URL path gets extracted and combined
        store_path = await make_store_path("integration://example.com/repo/data/array", mode="r")
        assert store_path.path == "repo/data/array"  # Built-in extraction for non-Arraylake schemes

    @pytest.mark.asyncio
    async def test_mode_handling_integration(self) -> None:
        """Test that modes are properly passed through."""

        store_path = await make_store_path("integration://example.com/repo", mode="r")
        assert store_path.store.kwargs["mode"] == "r"  # type: ignore[attr-defined]
        assert store_path.store.read_only is True

    @pytest.mark.asyncio
    async def test_storage_options_integration(self) -> None:
        """Test that storage_options are properly passed through."""

        storage_options = {"custom_option": "value", "read_only": True}
        store_path = await make_store_path(
            "integration://example.com/repo", mode="r", storage_options=storage_options
        )

        assert store_path.store.kwargs["storage_options"] == storage_options  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_error_handling_integration(self) -> None:
        """Test error handling in the integration."""

        # Test with non-existent scheme (should fall back to built-in handling)
        with pytest.raises(ValueError, match="Unsupported URL scheme"):
            await make_store_path("nonexistent://example.com", mode="r")


class MockArraylakeSchemeHandler(CustomSchemeHandler):
    """Mock Arraylake scheme handler for testing."""

    scheme = "al"

    @classmethod
    async def from_url(cls, url: str, **kwargs: Any) -> "MockStore":  # type: ignore[override]
        # Mock the behavior of the real Arraylake handler
        mode = kwargs.get("mode", "r")
        if mode and ("w" in mode or "a" in mode):
            raise ValueError("Write mode not supported for Arraylake URLs")

        return MockStore(url, **kwargs)


class TestArraylakeLikeIntegration:
    """Test integration with Arraylake-like custom scheme handlers."""

    def setup_method(self) -> None:
        """Register mock Arraylake handler for each test."""
        register_custom_scheme_handler(MockArraylakeSchemeHandler)

    @pytest.mark.asyncio
    async def test_arraylake_url_resolution(self) -> None:
        """Test Arraylake-style URL resolution."""

        # Test basic URL
        store_path = await make_store_path("al://org/repo", mode="r")
        assert isinstance(store_path.store, MockStore)  # type: ignore[unreachable]
        assert store_path.store.url == "al://org/repo"  # type: ignore[unreachable]

        # Test URL with path
        store_path = await make_store_path("al://org/repo/data/array", mode="r")
        assert store_path.store.url == "al://org/repo/data/array"
        assert store_path.path == "data/array"  # Extracted by custom logic

    @pytest.mark.asyncio
    async def test_arraylake_reference_path_extraction(self) -> None:
        """Test path extraction with Arraylake reference syntax."""

        # Test URL with branch reference
        store_path = await make_store_path("al://org/repo@branch.main/data/array", mode="r")
        assert store_path.path == "data/array"

        # Test URL with tag reference
        store_path = await make_store_path("al://org/repo@tag.v123/experiment", mode="r")
        assert store_path.path == "experiment"

        # Test URL with reference but no path
        store_path = await make_store_path("al://org/repo@branch.main", mode="r")
        assert store_path.path == ""

    @pytest.mark.asyncio
    async def test_arraylake_error_handling(self) -> None:
        """Test error handling with Arraylake-like URLs."""

        # Test write mode rejection
        with pytest.raises(ValueError, match="Write mode not supported"):
            await make_store_path("al://org/repo", mode="w")
