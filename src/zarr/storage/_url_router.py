"""
URL router for two-tier resolution system.

This module implements a routing system that handles different types of URLs:
1. Custom scheme URLs (al://, gs://, etc.) - Direct URL-to-Store mapping
2. ZEP 8 URLs (pipe-chained) - Adapter-based resolution
3. Built-in scheme URLs (file://, http://, etc.) - Traditional mapping
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from zarr.storage._scheme_registry import CustomSchemeRegistry
from zarr.storage._zep8 import URLStoreResolver, is_zep8_url

if TYPE_CHECKING:
    from zarr.abc.store import Store


class URLRouter:
    """
    Router for different URL resolution strategies.

    Implements a two-tier system:
    - Tier 1: Custom scheme handlers for direct URL-to-Store mapping
    - Tier 2: ZEP 8 adapter chains and built-in scheme mapping
    """

    def __init__(self) -> None:
        self.custom_schemes = CustomSchemeRegistry()
        self.zep8_resolver = URLStoreResolver()

    async def resolve(self, url: str, **kwargs: Any) -> Store:
        """
        Resolve a URL to a Store using the appropriate strategy.

        Parameters
        ----------
        url : str
            The URL to resolve.
        **kwargs : Any
            Additional arguments for resolution.

        Returns
        -------
        Store
            The resolved store instance.

        Raises
        ------
        ValueError
            If the URL cannot be resolved.
        """
        # Fast path: Check for custom schemes first
        if self.custom_schemes.can_handle(url):
            return await self.custom_schemes.resolve(url, **kwargs)

        # Complex path: Use ZEP 8 resolution
        if is_zep8_url(url):
            return await self.zep8_resolver.resolve_url(url, **kwargs)

        # Fallback: Try built-in scheme mapping
        return await self._resolve_builtin_scheme(url, **kwargs)

    async def _resolve_builtin_scheme(self, url: str, **kwargs: Any) -> Store:
        """
        Resolve built-in scheme URLs using traditional mapping.

        Parameters
        ----------
        url : str
            The URL with a built-in scheme (file://, http://, etc.).
        **kwargs : Any
            Additional arguments.

        Returns
        -------
        Store
            The resolved store.

        Raises
        ------
        ValueError
            If the scheme is not supported.
        """
        try:
            parsed = urlparse(url)
            scheme = parsed.scheme

            if not scheme:
                # Treat as local path
                from zarr.storage import LocalStore

                return await LocalStore.open(root=url, **kwargs)

            if scheme == "file":
                from zarr.storage import LocalStore

                # Remove file: prefix
                path = url.removeprefix("file:")
                return await LocalStore.open(root=path, **kwargs)

            if scheme in ("http", "https"):
                try:
                    from zarr.storage import FsspecStore

                    # Filter out parameters that FsspecStore.from_url doesn't accept
                    fsspec_kwargs = {k: v for k, v in kwargs.items() if k not in ("mode",)}
                    return FsspecStore.from_url(url, **fsspec_kwargs)
                except ImportError as e:
                    raise ValueError(
                        "HTTP(S) URLs require fsspec. Install with: pip install fsspec"
                    ) from e

            if scheme == "s3":
                try:
                    from zarr.storage import FsspecStore

                    # Filter out parameters that FsspecStore.from_url doesn't accept
                    fsspec_kwargs = {k: v for k, v in kwargs.items() if k not in ("mode",)}
                    return FsspecStore.from_url(url, **fsspec_kwargs)
                except ImportError as e:
                    raise ValueError("S3 URLs require s3fs. Install with: pip install s3fs") from e

            if scheme in ("gs", "gcs"):
                try:
                    from zarr.storage import FsspecStore

                    # Filter out parameters that FsspecStore.from_url doesn't accept
                    fsspec_kwargs = {k: v for k, v in kwargs.items() if k not in ("mode",)}
                    return FsspecStore.from_url(url, **fsspec_kwargs)
                except ImportError as e:
                    raise ValueError(
                        "GCS URLs require gcsfs. Install with: pip install gcsfs"
                    ) from e

            if scheme == "memory":
                # For memory URLs with netloc or path info, use FsspecStore for compatibility
                parsed = urlparse(url)
                if parsed.netloc or (parsed.path and parsed.path != "/"):
                    try:
                        from zarr.storage import FsspecStore

                        # Filter out parameters that FsspecStore.from_url doesn't accept
                        fsspec_kwargs = {k: v for k, v in kwargs.items() if k not in ("mode",)}
                        return FsspecStore.from_url(url, **fsspec_kwargs)
                    except ImportError as e:
                        raise ValueError(
                            "Memory URLs with paths require fsspec. Install with: pip install fsspec"
                        ) from e
                else:
                    # For bare memory: URLs, use MemoryStore
                    from zarr.storage import MemoryStore

                    return await MemoryStore.open(**kwargs)

            # Unknown scheme
            raise ValueError(f"Unsupported URL scheme: '{scheme}'")  # noqa: TRY301

        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise ValueError(f"Failed to resolve URL '{url}': {e}") from e

    def register_scheme(self, handler_cls: type) -> None:
        """
        Register a custom scheme handler.

        Parameters
        ----------
        handler_cls : type[CustomSchemeHandler]
            The handler class to register.
        """
        from zarr.abc.scheme_handler import CustomSchemeHandler

        if not issubclass(handler_cls, CustomSchemeHandler):
            raise TypeError("Handler must be a CustomSchemeHandler subclass")

        # Get scheme from the handler class
        # Create a temporary instance to get the scheme property
        temp_instance = handler_cls()
        scheme = temp_instance.scheme

        self.custom_schemes.register_scheme(scheme, handler_cls)

    def extract_path(self, url: str) -> str:
        """
        Extract the path component from a URL.

        Parameters
        ----------
        url : str
            The URL to extract path from.

        Returns
        -------
        str
            The extracted path, or empty string if none.
        """
        # For custom scheme URLs, use specialized path extraction
        # This includes known custom schemes even if no handler is registered
        if self.custom_schemes.can_handle(url) or self._is_known_custom_scheme(url):
            return self._extract_path_from_custom_scheme_url(url)

        # For ZEP 8 URLs, use the ZEP 8 resolver
        if is_zep8_url(url):
            return self.zep8_resolver.extract_path(url)

        # For built-in schemes, extract path component
        try:
            parsed = urlparse(url)
            return parsed.path.lstrip("/") if parsed.path else ""
        except Exception:
            return ""

    def _is_known_custom_scheme(self, url: str) -> bool:
        """Check if URL uses a known custom scheme (even if no handler registered)."""
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        else:
            # Known custom schemes that need special path handling
            return parsed.scheme in ("al",)  # Add more schemes as needed

    def _extract_path_from_custom_scheme_url(self, url: str) -> str:
        """Extract the path component from custom scheme URLs."""
        from urllib.parse import urlparse

        parsed = urlparse(url)
        if not parsed.path or parsed.path == "/":
            return ""

        # Remove leading slash from path
        path = parsed.path.removeprefix("/")

        # Handle scheme-specific path parsing
        if parsed.scheme == "al":
            # For Arraylake URLs, netloc contains org and path contains /repo/data/array
            # URLs look like: al://org/repo/path or al://org/repo@ref/path

            # Reconstruct the full path: org + path (without leading slash)
            full_path = (
                parsed.netloc + "/" + path if path else parsed.netloc
            )  # e.g., "org/repo/data/array"

            if "@" in full_path:
                # URL has @reference - split and extract path after reference
                repo_part, ref_and_path = full_path.split("@", 1)
                if "/" in ref_and_path:
                    # Has path after reference: @branch.main/path/to/group
                    _, path_part = ref_and_path.split("/", 1)
                    return path_part
                else:
                    # No path after reference: @branch.main
                    return ""
            else:
                # No @reference - everything after org/repo is path
                # full_path looks like: org/repo/path/to/data
                parts = full_path.split("/")
                if len(parts) > 2:
                    # org=parts[0], repo=parts[1], path=parts[2:]
                    return "/".join(parts[2:])
                else:
                    return ""

        # For other custom schemes, return the path as-is
        return path


# Global router instance
_router = URLRouter()


def get_url_router() -> URLRouter:
    """Get the global URL router instance."""
    return _router


def register_custom_scheme_handler(handler_cls: type) -> None:
    """
    Register a custom scheme handler globally.

    Parameters
    ----------
    handler_cls : type[CustomSchemeHandler]
        The handler class to register.
    """
    _router.register_scheme(handler_cls)
