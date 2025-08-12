"""
Registry for custom scheme handlers.

This module manages registration and lookup of CustomSchemeHandler instances
for different URL schemes, enabling direct URL-to-Store resolution.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

if TYPE_CHECKING:
    from zarr.abc.scheme_handler import CustomSchemeHandler
    from zarr.abc.store import Store


class CustomSchemeRegistry:
    """
    Registry for custom URL scheme handlers.

    Manages registration and lookup of CustomSchemeHandler classes for
    different URL schemes, providing fast scheme-based routing.
    """

    def __init__(self) -> None:
        self._handlers: dict[str, type[CustomSchemeHandler]] = {}

    def register_scheme(self, scheme: str, handler_cls: type[CustomSchemeHandler]) -> None:
        """
        Register a custom scheme handler.

        Parameters
        ----------
        scheme : str
            The URL scheme to register (e.g., 'al', 'gs').
        handler_cls : type[CustomSchemeHandler]
            The handler class for this scheme.

        Raises
        ------
        ValueError
            If the scheme is invalid or already registered.
        """
        if not scheme or not isinstance(scheme, str):
            raise ValueError(f"Invalid scheme: {scheme!r}")

        # Validate scheme format (RFC 3986)
        if (not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*[a-zA-Z0-9]$", scheme) and len(scheme) > 1) or (
            len(scheme) == 1 and not re.match(r"^[a-zA-Z]$", scheme)
        ):
            raise ValueError(f"Invalid scheme format: {scheme!r}")

        if scheme in self._handlers:
            existing = self._handlers[scheme]
            if existing != handler_cls:
                raise ValueError(f"Scheme '{scheme}' is already registered to {existing.__name__}")

        self._handlers[scheme] = handler_cls

    def get_handler(self, scheme: str) -> type[CustomSchemeHandler] | None:
        """
        Get the handler for a URL scheme.

        Parameters
        ----------
        scheme : str
            The URL scheme to look up.

        Returns
        -------
        type[CustomSchemeHandler] | None
            The handler class, or None if not found.
        """
        return self._handlers.get(scheme)

    def can_handle(self, url: str) -> bool:
        """
        Check if a URL can be handled by a registered scheme handler.

        Parameters
        ----------
        url : str
            The URL to check.

        Returns
        -------
        bool
            True if a handler is registered for this URL's scheme.
        """
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        else:
            return parsed.scheme in self._handlers

    async def resolve(self, url: str, **kwargs: Any) -> Store:
        """
        Resolve a URL to a Store using registered handlers.

        Parameters
        ----------
        url : str
            The URL to resolve.
        **kwargs : Any
            Additional arguments for the handler.

        Returns
        -------
        Store
            The resolved store instance.

        Raises
        ------
        ValueError
            If no handler is found for the URL scheme.
        """

        try:
            parsed = urlparse(url)
            handler_cls = self._handlers.get(parsed.scheme)

            if handler_cls is None:
                raise ValueError(f"No handler registered for scheme '{parsed.scheme}'")  # noqa: TRY301

            # At this point handler_cls is guaranteed to not be None
            assert handler_cls is not None  # For mypy
            return await handler_cls.from_url(url, **kwargs)
        except Exception as e:
            if isinstance(e, ValueError) and "No handler registered" in str(e):
                raise
            raise ValueError(f"Failed to resolve URL '{url}': {e}") from e

    def list_schemes(self) -> list[str]:
        """
        List all registered schemes.

        Returns
        -------
        list[str]
            List of registered scheme names.
        """
        return list(self._handlers.keys())
