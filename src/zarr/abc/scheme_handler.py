"""
Abstract base class for custom scheme handlers.

Custom scheme handlers provide direct URL-to-Store mapping for specific URL schemes,
bypassing the ZEP 8 adapter chain resolution for better performance and simpler logic.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from zarr.abc.store import Store


class CustomSchemeHandler(ABC):
    """
    Abstract base class for custom URL scheme handlers.

    Custom scheme handlers enable direct mapping from URLs with specific schemes
    (like al://, gs://, etc.) to Store instances, providing an alternative to
    ZEP 8 adapter chaining for schemes that need custom logic or performance.

    Examples
    --------
    >>> class MySchemeHandler(CustomSchemeHandler):
    ...     scheme = "myscheme"
    ...
    ...     @classmethod
    ...     async def from_url(cls, url: str, **kwargs: Any) -> Store:
    ...         # Parse URL and create store
    ...         return MyStore(url, **kwargs)
    """

    @property
    @abstractmethod
    def scheme(self) -> str:
        """The URL scheme this handler supports (e.g., 'al', 'gs', 'custom')."""

    @classmethod
    @abstractmethod
    async def from_url(cls, url: str, **kwargs: Any) -> Store:
        """
        Create a store directly from a URL.

        Parameters
        ----------
        url : str
            The URL to create a store from.
        **kwargs : Any
            Additional arguments like mode, storage_options, etc.

        Returns
        -------
        Store
            A store instance for the given URL.

        Raises
        ------
        ValueError
            If the URL cannot be handled by this scheme handler.
        """
