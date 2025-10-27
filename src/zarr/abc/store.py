from __future__ import annotations

from abc import ABC, abstractmethod
from asyncio import gather
from dataclasses import dataclass
from itertools import starmap
from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Iterable
    from types import TracebackType
    from typing import Any, Self, TypeAlias

    from zarr.core.buffer import Buffer, BufferPrototype
    from zarr.core.common import JSON, ZarrFormat
    from zarr.core.metadata import ArrayMetadata, GroupMetadata

__all__ = ["ByteGetter", "ByteSetter", "HighLevelStore", "Store", "set_or_delete"]


@dataclass
class RangeByteRequest:
    """Request a specific byte range"""

    start: int
    """The start of the byte range request (inclusive)."""
    end: int
    """The end of the byte range request (exclusive)."""


@dataclass
class OffsetByteRequest:
    """Request all bytes starting from a given byte offset"""

    offset: int
    """The byte offset for the offset range request."""


@dataclass
class SuffixByteRequest:
    """Request up to the last `n` bytes"""

    suffix: int
    """The number of bytes from the suffix to request."""


ByteRequest: TypeAlias = RangeByteRequest | OffsetByteRequest | SuffixByteRequest


class Store(ABC):
    """
    Abstract base class for Zarr stores.
    """

    _read_only: bool
    _is_open: bool

    def __init__(self, *, read_only: bool = False) -> None:
        self._is_open = False
        self._read_only = read_only

    @classmethod
    async def open(cls, *args: Any, **kwargs: Any) -> Self:
        """
        Create and open the store.

        Parameters
        ----------
        *args : Any
            Positional arguments to pass to the store constructor.
        **kwargs : Any
            Keyword arguments to pass to the store constructor.

        Returns
        -------
        Store
            The opened store instance.
        """
        store = cls(*args, **kwargs)
        await store._open()
        return store

    def with_read_only(self, read_only: bool = False) -> Store:
        """
        Return a new store with a new read_only setting.

        The new store points to the same location with the specified new read_only state.
        The returned Store is not automatically opened, and this store is
        not automatically closed.

        Parameters
        ----------
        read_only
            If True, the store will be created in read-only mode. Defaults to False.

        Returns
        -------
            A new store of the same type with the new read only attribute.
        """
        raise NotImplementedError(
            f"with_read_only is not implemented for the {type(self)} store type."
        )

    def __enter__(self) -> Self:
        """Enter a context manager that will close the store upon exiting."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the store."""
        self.close()

    async def _open(self) -> None:
        """
        Open the store.

        Raises
        ------
        ValueError
            If the store is already open.
        """
        if self._is_open:
            raise ValueError("store is already open")
        self._is_open = True

    async def _ensure_open(self) -> None:
        """Open the store if it is not already open."""
        if not self._is_open:
            await self._open()

    async def is_empty(self, prefix: str) -> bool:
        """
        Check if the directory is empty.

        Parameters
        ----------
        prefix : str
            Prefix of keys to check.

        Returns
        -------
        bool
            True if the store is empty, False otherwise.
        """
        if not self.supports_listing:
            raise NotImplementedError
        if prefix != "" and not prefix.endswith("/"):
            prefix += "/"
        async for _ in self.list_prefix(prefix):
            return False
        return True

    async def clear(self) -> None:
        """
        Clear the store.

        Remove all keys and values from the store.
        """
        if not self.supports_deletes:
            raise NotImplementedError
        if not self.supports_listing:
            raise NotImplementedError
        self._check_writable()
        await self.delete_dir("")

    @property
    def read_only(self) -> bool:
        """Is the store read-only?"""
        return self._read_only

    def _check_writable(self) -> None:
        """Raise an exception if the store is not writable."""
        if self.read_only:
            raise ValueError("store was opened in read-only mode and does not support writing")

    @abstractmethod
    def __eq__(self, value: object) -> bool:
        """Equality comparison."""
        ...

    @abstractmethod
    async def get(
        self,
        key: str,
        prototype: BufferPrototype,
        byte_range: ByteRequest | None = None,
    ) -> Buffer | None:
        """Retrieve the value associated with a given key.

        Parameters
        ----------
        key : str
        prototype : BufferPrototype
            The prototype of the output buffer. Stores may support a default buffer prototype.
        byte_range : ByteRequest, optional
            ByteRequest may be one of the following. If not provided, all data associated with the key is retrieved.
            - RangeByteRequest(int, int): Request a specific range of bytes in the form (start, end). The end is exclusive. If the given range is zero-length or starts after the end of the object, an error will be returned. Additionally, if the range ends after the end of the object, the entire remainder of the object will be returned. Otherwise, the exact requested range will be returned.
            - OffsetByteRequest(int): Request all bytes starting from a given byte offset. This is equivalent to bytes={int}- as an HTTP header.
            - SuffixByteRequest(int): Request the last int bytes. Note that here, int is the size of the request, not the byte offset. This is equivalent to bytes=-{int} as an HTTP header.

        Returns
        -------
        Buffer
        """
        ...

    @abstractmethod
    async def get_partial_values(
        self,
        prototype: BufferPrototype,
        key_ranges: Iterable[tuple[str, ByteRequest | None]],
    ) -> list[Buffer | None]:
        """Retrieve possibly partial values from given key_ranges.

        Parameters
        ----------
        prototype : BufferPrototype
            The prototype of the output buffer. Stores may support a default buffer prototype.
        key_ranges : Iterable[tuple[str, tuple[int | None, int | None]]]
            Ordered set of key, range pairs, a key may occur multiple times with different ranges

        Returns
        -------
        list of values, in the order of the key_ranges, may contain null/none for missing keys
        """
        ...

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Check if a key exists in the store.

        Parameters
        ----------
        key : str

        Returns
        -------
        bool
        """
        ...

    @property
    @abstractmethod
    def supports_writes(self) -> bool:
        """Does the store support writes?"""
        ...

    @abstractmethod
    async def set(self, key: str, value: Buffer) -> None:
        """Store a (key, value) pair.

        Parameters
        ----------
        key : str
        value : Buffer
        """
        ...

    async def set_if_not_exists(self, key: str, value: Buffer) -> None:
        """
        Store a key to ``value`` if the key is not already present.

        Parameters
        ----------
        key : str
        value : Buffer
        """
        # Note for implementers: the default implementation provided here
        # is not safe for concurrent writers. There's a race condition between
        # the `exists` check and the `set` where another writer could set some
        # value at `key` or delete `key`.
        if not await self.exists(key):
            await self.set(key, value)

    async def _set_many(self, values: Iterable[tuple[str, Buffer]]) -> None:
        """
        Insert multiple (key, value) pairs into storage.
        """
        await gather(*starmap(self.set, values))

    @property
    def supports_consolidated_metadata(self) -> bool:
        """
        Does the store support consolidated metadata?.

        If it doesn't an error will be raised on requests to consolidate the metadata.
        Returning `False` can be useful for stores which implement their own
        consolidation mechanism outside of the zarr-python implementation.
        """

        return True

    @property
    @abstractmethod
    def supports_deletes(self) -> bool:
        """Does the store support deletes?"""
        ...

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Remove a key from the store

        Parameters
        ----------
        key : str
        """
        ...

    @property
    def supports_partial_writes(self) -> Literal[False]:
        """Does the store support partial writes?

        Partial writes are no longer used by Zarr, so this is always false.
        """
        return False

    @property
    @abstractmethod
    def supports_listing(self) -> bool:
        """Does the store support listing?"""
        ...

    @abstractmethod
    def list(self) -> AsyncIterator[str]:
        """Retrieve all keys in the store.

        Returns
        -------
        AsyncIterator[str]
        """
        # This method should be async, like overridden methods in child classes.
        # However, that's not straightforward:
        # https://stackoverflow.com/questions/68905848

    @abstractmethod
    def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        """
        Retrieve all keys in the store that begin with a given prefix. Keys are returned relative
        to the root of the store.

        Parameters
        ----------
        prefix : str

        Returns
        -------
        AsyncIterator[str]
        """
        # This method should be async, like overridden methods in child classes.
        # However, that's not straightforward:
        # https://stackoverflow.com/questions/68905848

    @abstractmethod
    def list_dir(self, prefix: str) -> AsyncIterator[str]:
        """
        Retrieve all keys and prefixes with a given prefix and which do not contain the character
        “/” after the given prefix.

        Parameters
        ----------
        prefix : str

        Returns
        -------
        AsyncIterator[str]
        """
        # This method should be async, like overridden methods in child classes.
        # However, that's not straightforward:
        # https://stackoverflow.com/questions/68905848

    async def delete_dir(self, prefix: str) -> None:
        """
        Remove all keys and prefixes in the store that begin with a given prefix.
        """
        if not self.supports_deletes:
            raise NotImplementedError
        if not self.supports_listing:
            raise NotImplementedError
        self._check_writable()
        if prefix != "" and not prefix.endswith("/"):
            prefix += "/"
        async for key in self.list_prefix(prefix):
            await self.delete(key)

    def close(self) -> None:
        """Close the store."""
        self._is_open = False

    async def _get_many(
        self, requests: Iterable[tuple[str, BufferPrototype, ByteRequest | None]]
    ) -> AsyncGenerator[tuple[str, Buffer | None], None]:
        """
        Retrieve a collection of objects from storage. In general this method does not guarantee
        that objects will be retrieved in the order in which they were requested, so this method
        yields tuple[str, Buffer | None] instead of just Buffer | None
        """
        for req in requests:
            yield (req[0], await self.get(*req))

    async def getsize(self, key: str) -> int:
        """
        Return the size, in bytes, of a value in a Store.

        Parameters
        ----------
        key : str

        Returns
        -------
        nbytes : int
            The size of the value (in bytes).

        Raises
        ------
        FileNotFoundError
            When the given key does not exist in the store.
        """
        # Note to implementers: this default implementation is very inefficient since
        # it requires reading the entire object. Many systems will have ways to get the
        # size of an object without reading it.
        # avoid circular import
        from zarr.core.buffer.core import default_buffer_prototype

        value = await self.get(key, prototype=default_buffer_prototype())
        if value is None:
            raise FileNotFoundError(key)
        return len(value)

    async def getsize_prefix(self, prefix: str) -> int:
        """
        Return the size, in bytes, of all values under a prefix.

        Parameters
        ----------
        prefix : str
            The prefix of the directory to measure.

        Returns
        -------
        nbytes : int
            The sum of the sizes of the values in the directory (in bytes).

        See Also
        --------
        zarr.Array.nbytes_stored
        Store.getsize

        Notes
        -----
        ``getsize_prefix`` is just provided as a potentially faster alternative to
        listing all the keys under a prefix calling [`Store.getsize`][zarr.abc.store.Store.getsize] on each.

        In general, ``prefix`` should be the path of an Array or Group in the Store.
        Implementations may differ on the behavior when some other ``prefix``
        is provided.
        """
        # TODO: Overlap listing keys with getsize calls.
        # Currently, we load the list of keys into memory and only then move
        # on to getting sizes. Ideally we would overlap those two, which should
        # improve tail latency and might reduce memory pressure (since not all keys
        # would be in memory at once).

        # avoid circular import
        from zarr.core.common import concurrent_map
        from zarr.core.config import config

        keys = [(x,) async for x in self.list_prefix(prefix)]
        limit = config.get("async.concurrency")
        sizes = await concurrent_map(keys, self.getsize, limit=limit)
        return sum(sizes)


class HighLevelStore(ABC):
    """
    Abstract base class for high-level Zarr stores.

    High-level stores provide a metadata-aware interface on top of the basic Store protocol,
    offering semantic operations for arrays, groups, chunks, and metadata management.
    """

    # Core properties
    @property
    @abstractmethod
    def store(self) -> Store:
        """The underlying Store instance."""
        ...

    @property
    @abstractmethod
    def zarr_format(self) -> ZarrFormat:
        """The Zarr format version (2 or 3), or None if not yet detected."""
        ...

    @property
    @abstractmethod
    def read_only(self) -> bool:
        """Is the store read-only?"""
        ...

    # Store capability properties
    @property
    @abstractmethod
    def supports_writes(self) -> bool:
        """Does the store support writes?"""
        ...

    @property
    @abstractmethod
    def supports_deletes(self) -> bool:
        """Does the store support deletes?"""
        ...

    @property
    @abstractmethod
    def supports_listing(self) -> bool:
        """Does the store support listing?"""
        ...

    @property
    @abstractmethod
    def supports_partial_writes(self) -> bool:
        """Does the store support partial writes?"""
        ...

    @property
    def supports_consolidated_metadata(self) -> bool:
        """
        Does the store support consolidated metadata?

        Returns
        -------
        bool
            True if the store supports consolidated metadata operations.
            Default is True. Override to return False in implementations
            that don't support consolidated metadata (e.g., database-backed
            stores with native fast metadata access).
        """
        return True

    # Store protocol methods (delegated to underlying store)
    @abstractmethod
    async def get(
        self,
        key: str,
        prototype: BufferPrototype,
        byte_range: ByteRequest | None = None,
    ) -> Buffer | None:
        """Retrieve the value associated with a given key."""
        ...

    @abstractmethod
    async def set(self, key: str, value: Buffer) -> None:
        """Store a (key, value) pair."""
        ...

    @abstractmethod
    async def set_if_not_exists(self, key: str, value: Buffer) -> None:
        """Store a key to value if the key is not already present."""
        ...

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Remove a key from the store."""
        ...

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Check if a key exists in the store."""
        ...

    @abstractmethod
    def list(self) -> AsyncIterator[str]:
        """Retrieve all keys in the store."""
        ...

    @abstractmethod
    def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        """Retrieve all keys in the store that begin with a given prefix."""
        ...

    @abstractmethod
    def list_dir(self, prefix: str) -> AsyncIterator[str]:
        """Retrieve all keys and prefixes with a given prefix."""
        ...

    @abstractmethod
    async def delete_dir(self, prefix: str) -> None:
        """Remove all keys and prefixes that begin with a given prefix."""
        ...

    @abstractmethod
    async def is_empty(self, prefix: str) -> bool:
        """Check if the directory is empty."""
        ...

    @abstractmethod
    async def get_partial_values(
        self,
        prototype: BufferPrototype,
        key_ranges: Iterable[tuple[str, ByteRequest | None]],
    ) -> list[Buffer | None]:
        """Retrieve possibly partial values from given key_ranges."""
        ...

    @abstractmethod
    async def getsize(self, key: str) -> int:
        """Return the size, in bytes, of a value in the store."""
        ...

    @abstractmethod
    async def getsize_prefix(self, prefix: str) -> int:
        """Return the size, in bytes, of all values under a prefix."""
        ...

    @abstractmethod
    def with_read_only(self, read_only: bool) -> HighLevelStore:
        """Return a new store with a new read_only setting."""
        ...

    # High-level metadata operations
    @abstractmethod
    async def detect_format(self, path: str = "") -> ZarrFormat:
        """
        Auto-detect the Zarr format at a given path.

        Parameters
        ----------
        path : str
            Path to check

        Returns
        -------
        ZarrFormat
            The detected Zarr format (2 or 3)
        """
        ...

    @abstractmethod
    async def get_metadata(self, path: str) -> ArrayMetadata | GroupMetadata:
        """
        Get metadata (array or group) and return typed object.

        Parameters
        ----------
        path : str
            Path to the node

        Returns
        -------
        ArrayMetadata | GroupMetadata
            Parsed metadata object
        """
        ...

    @abstractmethod
    async def get_array_metadata(self, path: str) -> ArrayMetadata:
        """
        Fetch array metadata with type validation.

        Parameters
        ----------
        path : str
            Path to the array

        Returns
        -------
        ArrayMetadata
            Parsed array metadata
        """
        ...

    @abstractmethod
    async def get_group_metadata(self, path: str) -> GroupMetadata:
        """
        Fetch group metadata with type validation.

        Parameters
        ----------
        path : str
            Path to the group

        Returns
        -------
        GroupMetadata
            Parsed group metadata
        """
        ...

    @abstractmethod
    async def has_consolidated_metadata(
        self, path: str, consolidated_key: str = ".zmetadata"
    ) -> bool:
        """
        Check if consolidated metadata exists at the given path.

        Parameters
        ----------
        path : str
            Path to check for consolidated metadata
        consolidated_key : str, default ".zmetadata"
            Key for v2 consolidated metadata file (ignored for v3)

        Returns
        -------
        bool
            True if consolidated metadata exists, False otherwise
        """
        ...

    @abstractmethod
    async def get_group_metadata_bytes(
        self, path: str, *, consolidated_key: str = ".zmetadata"
    ) -> dict[str, Any]:
        """
        Get raw metadata bytes for opening a group with optional consolidated metadata.

        This method reads all necessary metadata files from the store without parsing them,
        allowing the caller to construct a group with consolidated metadata support.

        Parameters
        ----------
        path : str
            Path to the group
        consolidated_key : str, default ".zmetadata"
            Key for v2 consolidated metadata file

        Returns
        -------
        dict
            Dictionary containing:
            - 'zarr_json_bytes': Buffer | None - v3 metadata
            - 'zgroup_bytes': Buffer | None - v2 group metadata
            - 'zattrs_bytes': Buffer | None - v2 attributes
            - 'consolidated_bytes': Buffer | None - v2 consolidated metadata
            - 'detected_format': Literal[2, 3] - detected zarr format
        """
        ...

    @abstractmethod
    async def open_group_metadata(
        self,
        path: str,
        *,
        use_consolidated: bool | None = None,
        consolidated_key: str = ".zmetadata",
    ) -> GroupMetadata:
        """
        Open and parse group metadata with optional consolidated metadata support.

        This method handles all the complexity of reading, parsing, and constructing
        GroupMetadata objects, including consolidated metadata if present.

        Parameters
        ----------
        path : str
            Path to the group
        use_consolidated : bool | None, default None
            Whether to use consolidated metadata:
            - True: require consolidated metadata (raise if not found)
            - False: ignore consolidated metadata even if present
            - None: use consolidated metadata if available
        consolidated_key : str, default ".zmetadata"
            Key for v2 consolidated metadata file

        Returns
        -------
        GroupMetadata
            Parsed group metadata with consolidated metadata if applicable

        Raises
        ------
        FileNotFoundError
            If the group doesn't exist
        ValueError
            If use_consolidated=True but consolidated metadata not found
        """
        ...

    @abstractmethod
    async def set_metadata(
        self,
        path: str,
        metadata: ArrayMetadata | GroupMetadata,
        *,
        ensure_parents: bool = False,
    ) -> None:
        """
        Store metadata for an array or group.

        Parameters
        ----------
        path : str
            Path to store the metadata at
        metadata : ArrayMetadata | GroupMetadata
            Metadata object to store
        ensure_parents : bool, default False
            If True, create parent groups as needed
        """
        ...

    # Node operations
    @abstractmethod
    async def get_node_type(self, path: str) -> Literal["array", "group", "nothing"]:
        """
        Determine the node type at a given path.

        Parameters
        ----------
        path : str
            Path to check

        Returns
        -------
        Literal["array", "group", "nothing"]
            The type of node at the path
        """
        ...

    @abstractmethod
    async def contains_array(self, path: str) -> bool:
        """Check if an array exists at the given path."""
        ...

    @abstractmethod
    async def contains_group(self, path: str) -> bool:
        """Check if a group exists at the given path."""
        ...

    @abstractmethod
    async def ensure_no_existing_node(
        self,
        path: str,
        *,
        zarr_format: ZarrFormat,
    ) -> None:
        """
        Ensure no existing node (array or group) exists at the given path.

        Parameters
        ----------
        path : str
            Path to check
        zarr_format : ZarrFormat
            Zarr format version

        Raises
        ------
        ContainsArrayError
            If an array exists at the path
        ContainsGroupError
            If a group exists at the path
        """
        ...

    @abstractmethod
    async def delete_node(self, path: str) -> None:
        """Delete a node (array or group) at the given path."""
        ...

    @abstractmethod
    async def delete_array(
        self,
        path: str,
        *,
        metadata: ArrayMetadata | None = None,
    ) -> None:
        """
        Delete an array and all its chunks.

        Parameters
        ----------
        path : str
            Path to the array
        metadata : ArrayMetadata, optional
            Array metadata (fetched if not provided)
        """
        ...

    @abstractmethod
    async def delete_group(
        self,
        path: str,
        *,
        metadata: GroupMetadata | None = None,
    ) -> None:
        """
        Delete a group and all its contents recursively.

        Parameters
        ----------
        path : str
            Path to the group
        metadata : GroupMetadata, optional
            Group metadata (fetched if not provided)
        """
        ...

    # Chunk operations
    @abstractmethod
    async def get_chunk(
        self,
        path: str,
        chunk_coords: tuple[int, ...],
        prototype: BufferPrototype,
        *,
        metadata: ArrayMetadata | None = None,
    ) -> Buffer | None:
        """
        Retrieve a specific chunk by coordinates.

        Parameters
        ----------
        path : str
            Path to the array
        chunk_coords : tuple[int, ...]
            Chunk coordinates
        prototype : BufferPrototype
            Buffer prototype
        metadata : ArrayMetadata, optional
            Array metadata (fetched if not provided)

        Returns
        -------
        Buffer | None
            Chunk data or None if not found
        """
        ...

    @abstractmethod
    async def set_chunk(
        self,
        path: str,
        chunk_coords: tuple[int, ...],
        value: Buffer,
        *,
        metadata: ArrayMetadata | None = None,
    ) -> None:
        """
        Store a specific chunk by coordinates.

        Parameters
        ----------
        path : str
            Path to the array
        chunk_coords : tuple[int, ...]
            Chunk coordinates
        value : Buffer
            Chunk data
        metadata : ArrayMetadata, optional
            Array metadata (fetched if not provided)
        """
        ...

    @abstractmethod
    async def delete_chunk(
        self,
        path: str,
        chunk_coords: tuple[int, ...],
        *,
        metadata: ArrayMetadata | None = None,
    ) -> None:
        """
        Delete a specific chunk by coordinates.

        Parameters
        ----------
        path : str
            Path to the array
        chunk_coords : tuple[int, ...]
            Chunk coordinates
        metadata : ArrayMetadata, optional
            Array metadata (fetched if not provided)
        """
        ...

    @abstractmethod
    async def exists_chunk(
        self,
        path: str,
        chunk_coords: tuple[int, ...],
        *,
        metadata: ArrayMetadata | None = None,
    ) -> bool:
        """
        Check if a specific chunk exists by coordinates.

        Parameters
        ----------
        path : str
            Path to the array
        chunk_coords : tuple[int, ...]
            Chunk coordinates
        metadata : ArrayMetadata, optional
            Array metadata (fetched if not provided)

        Returns
        -------
        bool
            True if chunk exists
        """
        ...

    @abstractmethod
    def list_chunks(
        self,
        path: str,
        *,
        metadata: ArrayMetadata | None = None,
    ) -> AsyncIterator[tuple[int, ...]]:
        """
        List all chunk coordinates for an array.

        Parameters
        ----------
        path : str
            Path to the array
        metadata : ArrayMetadata, optional
            Array metadata (fetched if not provided)

        Yields
        ------
        tuple[int, ...]
            Chunk coordinates
        """
        ...

    # Storage information
    @abstractmethod
    async def get_size(
        self,
        path: str,
        *,
        metadata: ArrayMetadata | GroupMetadata | None = None,
    ) -> int:
        """
        Get total size in bytes of an array or group.

        Parameters
        ----------
        path : str
            Path to the node
        metadata : ArrayMetadata | GroupMetadata, optional
            Metadata (fetched if not provided)

        Returns
        -------
        int
            Total size in bytes
        """
        ...

    @abstractmethod
    async def get_array_storage_info(
        self,
        path: str,
        *,
        metadata: ArrayMetadata | None = None,
    ) -> dict[str, Any]:
        """
        Get detailed storage information for an array.

        Parameters
        ----------
        path : str
            Path to the array
        metadata : ArrayMetadata, optional
            Array metadata (fetched if not provided)

        Returns
        -------
        dict[str, Any]
            Storage information
        """
        ...

    @abstractmethod
    async def get_group_storage_info(
        self,
        path: str,
        *,
        metadata: GroupMetadata | None = None,
    ) -> dict[str, Any]:
        """
        Get detailed storage information for a group.

        Parameters
        ----------
        path : str
            Path to the group
        metadata : GroupMetadata, optional
            Group metadata (fetched if not provided)

        Returns
        -------
        dict[str, Any]
            Storage information
        """
        ...

    @abstractmethod
    async def get_hierarchy_tree(
        self,
        path: str,
    ) -> dict[str, JSON]:
        """
        Get the hierarchy tree starting from a path.

        Parameters
        ----------
        path : str
            Root path

        Returns
        -------
        dict[str, JSON]
            Hierarchy tree structure
        """
        ...

    @abstractmethod
    def list_children(self, path: str) -> AsyncIterator[str]:
        """
        List immediate children of a group.

        Parameters
        ----------
        path : str
            Path to the group

        Yields
        ------
        str
            Child names (relative to parent)
        """
        ...

    @abstractmethod
    def list_children_with_metadata(
        self, path: str
    ) -> AsyncIterator[tuple[str, ArrayMetadata | GroupMetadata]]:
        """
        List immediate children of a group with their metadata.

        Parameters
        ----------
        path : str
            Path to the group

        Yields
        ------
        tuple[str, ArrayMetadata | GroupMetadata]
            Child name and metadata
        """
        ...

    # Magic methods
    @abstractmethod
    def __eq__(self, other: object) -> bool:
        """Equality comparison."""
        ...

    @abstractmethod
    def __hash__(self) -> int:
        """Hash for use in sets and dicts."""
        ...


@runtime_checkable
class ByteGetter(Protocol):
    async def get(
        self, prototype: BufferPrototype, byte_range: ByteRequest | None = None
    ) -> Buffer | None: ...


@runtime_checkable
class ByteSetter(Protocol):
    async def get(
        self, prototype: BufferPrototype, byte_range: ByteRequest | None = None
    ) -> Buffer | None: ...

    async def set(self, value: Buffer) -> None: ...

    async def delete(self) -> None: ...

    async def set_if_not_exists(self, default: Buffer) -> None: ...


async def set_or_delete(byte_setter: ByteSetter, value: Buffer | None) -> None:
    """Set or delete a value in a byte setter

    Parameters
    ----------
    byte_setter : ByteSetter
    value : Buffer | None

    Notes
    -----
    If value is None, the key will be deleted.
    """
    if value is None:
        await byte_setter.delete()
    else:
        await byte_setter.set(value)
