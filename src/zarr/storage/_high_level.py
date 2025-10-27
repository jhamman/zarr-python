"""
High-level store that provides metadata-aware operations for Zarr hierarchies.

This module implements the HighLevelStore class, which sits between low-level
Store (key/value) and high-level Array/Group abstractions. It centralizes metadata
fetching, JSON parsing, node detection, and format handling (v2/v3).
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from zarr.abc.store import HighLevelStore as HighLevelStoreABC
from zarr.abc.store import Store
from zarr.core.buffer import Buffer, BufferPrototype, default_buffer_prototype
from zarr.core.common import (
    ZARR_JSON,
    ZARRAY_JSON,
    ZATTRS_JSON,
    ZGROUP_JSON,
    ZarrFormat,
)
from zarr.errors import (
    ContainsArrayAndGroupError,
    ContainsArrayError,
    ContainsGroupError,
    NodeTypeValidationError,
)
from zarr.storage._utils import _join_paths, normalize_path

if TYPE_CHECKING:
    from zarr.core.group import GroupMetadata
    from zarr.core.metadata import ArrayMetadata


@dataclass
class NodeInfo:
    """Information about a node in the Zarr hierarchy."""

    path: str
    node_type: Literal["array", "group"]
    zarr_format: ZarrFormat
    metadata: ArrayMetadata | GroupMetadata


class HighLevelStore(HighLevelStoreABC):
    """
    A metadata-aware store that understands Zarr hierarchy constructs.

    This class sits between low-level Store (key/value) and high-level
    Array/Group abstractions. It centralizes:
    - Metadata fetching and JSON parsing
    - Node type detection (array/group/nothing)
    - Format version handling (v2/v3)
    - Chunk key encoding and operations
    - Hierarchy traversal

    Parameters
    ----------
    store : Store
        The underlying low-level store
    zarr_format : ZarrFormat | None, default None
        Zarr format version (2 or 3). If None, auto-detect on first access.

    Examples
    --------
    >>> from zarr.storage import LocalStore, HighLevelStore
    >>> store = await LocalStore.open("data.zarr")
    >>> hl_store = HighLevelStore(store)  # Auto-detects format
    >>> metadata = await hl_store.get_metadata("array1")
    >>> print(f"Using Zarr v{hl_store.zarr_format}")
    """

    def __init__(
        self,
        store: Store,
        *,
        zarr_format: ZarrFormat | None = None,
    ) -> None:
        from zarr.errors import MetadataValidationError

        # Validate zarr_format
        if zarr_format is not None and zarr_format not in (2, 3):
            msg = f"Invalid value for 'zarr_format'. Expected 2, 3, or None. Got '{zarr_format}'."
            raise MetadataValidationError(msg)

        self._store = store
        self._zarr_format = zarr_format
        self._format_detected = zarr_format is not None

    @property
    def store(self) -> Store:
        """Access the underlying store."""
        return self._store

    @property
    def read_only(self) -> bool:
        """Check if the store is read-only."""
        return self._store.read_only

    @property
    def supports_writes(self) -> bool:
        """Check if the underlying store supports writes."""
        return self._store.supports_writes

    @property
    def supports_deletes(self) -> bool:
        """Check if the underlying store supports deletes."""
        return self._store.supports_deletes

    @property
    def supports_listing(self) -> bool:
        """Check if the underlying store supports listing."""
        return self._store.supports_listing

    @property
    def supports_partial_writes(self) -> bool:
        """Check if the underlying store supports partial writes."""
        return self._store.supports_partial_writes

    @property
    def supports_consolidated_metadata(self) -> bool:
        """Check if the underlying store supports consolidated metadata."""
        return self._store.supports_consolidated_metadata

    @property
    def zarr_format(self) -> ZarrFormat:
        """
        Get the Zarr format version.

        If not yet detected, this property will raise an error.
        Use await hl_store.detect_format() to detect asynchronously,
        or let it auto-detect on first operation.

        Returns
        -------
        zarr_format : ZarrFormat
            The Zarr format version (2 or 3)

        Raises
        ------
        RuntimeError
            If format has not been detected yet
        """
        if not self._format_detected:
            raise RuntimeError(
                "Format not yet determined. Use await hl_store.detect_format() "
                "or specify format at initialization."
            )
        return self._zarr_format  # type: ignore[return-value]

    def with_read_only(self, read_only: bool) -> HighLevelStore:
        """
        Return a new HighLevelStore with the read_only flag set.

        This delegates to the underlying store's with_read_only method
        and wraps the result in a new HighLevelStore.

        Parameters
        ----------
        read_only : bool
            Whether the new store should be read-only

        Returns
        -------
        HighLevelStore
            A new HighLevelStore with the specified read_only setting
        """
        new_underlying_store = self._store.with_read_only(read_only)
        return HighLevelStore(new_underlying_store, zarr_format=self._zarr_format)

    # Delegate common Store methods to underlying store
    async def get(self, key: str, prototype: Any, byte_range: Any | None = None) -> Any:
        """Delegate to underlying store."""
        return await self._store.get(key, prototype, byte_range)

    async def set(self, key: str, value: Any) -> None:
        """Delegate to underlying store."""
        return await self._store.set(key, value)

    async def set_if_not_exists(self, key: str, value: Any) -> bool:
        """Delegate to underlying store."""
        return await self._store.set_if_not_exists(key, value)

    async def delete(self, key: str) -> None:
        """Delegate to underlying store."""
        return await self._store.delete(key)

    async def exists(self, key: str) -> bool:
        """Delegate to underlying store."""
        return await self._store.exists(key)

    def list_prefix(self, prefix: str) -> Any:
        """Delegate to underlying store."""
        return self._store.list_prefix(prefix)

    def list_dir(self, prefix: str) -> Any:
        """Delegate to underlying store."""
        return self._store.list_dir(prefix)

    async def delete_dir(self, prefix: str) -> None:
        """Delegate to underlying store."""
        return await self._store.delete_dir(prefix)

    async def is_empty(self, prefix: str) -> bool:
        """Delegate to underlying store."""
        return await self._store.is_empty(prefix)

    async def get_partial_values(
        self,
        prototype: Any,
        key_ranges: Any,
    ) -> Any:
        """Delegate to underlying store."""
        return await self._store.get_partial_values(prototype, key_ranges)

    async def getsize(self, key: str) -> int:
        """Return the size, in bytes, of a value in the store."""
        return await self._store.getsize(key)

    async def getsize_prefix(self, prefix: str) -> int:
        """Return the size, in bytes, of all values under a prefix."""
        return await self._store.getsize_prefix(prefix)

    def list(self) -> Any:
        """Delegate to underlying store."""
        return self._store.list()

    def __eq__(self, other: object) -> bool:
        """
        Compare HighLevelStore instances.

        Two HighLevelStores are equal if they wrap the same underlying store.
        Also supports comparison with Store instances directly.
        """
        if isinstance(other, HighLevelStore):
            return self._store == other._store
        # Allow comparison with the underlying store directly
        return self._store == other

    def __hash__(self) -> int:
        """Hash based on the underlying store."""
        return hash(self._store)

    def __repr__(self) -> str:
        """Delegate repr to underlying store for cleaner display."""
        return repr(self._store)

    def __str__(self) -> str:
        """Delegate str to underlying store for cleaner display."""
        return str(self._store)

    def _check_writable(self) -> None:
        """Delegate to underlying store."""
        return self._store._check_writable()

    async def _ensure_open(self) -> None:
        """Ensure the underlying store is open."""
        await self._store._ensure_open()

    async def _ensure_format(self) -> ZarrFormat:
        """
        Internal method to ensure format is known.

        Called by all public methods that need to know the format.
        If format not yet detected, calls detect_format() and caches result.

        Returns
        -------
        zarr_format : ZarrFormat
            The Zarr format version (2 or 3)
        """
        if not self._format_detected:
            await self.detect_format()
        return self._zarr_format  # type: ignore[return-value]

    async def detect_format(self, path: str = "") -> ZarrFormat:
        """
        Auto-detect Zarr format version.

        Checks for v3 first (zarr.json), then v2 (.zarray or .zgroup).
        Caches the result for subsequent operations.

        Parameters
        ----------
        path : str, default ""
            Path to check for metadata. If empty, checks root.

        Returns
        -------
        zarr_format : ZarrFormat
            The detected Zarr format version (2 or 3)

        Raises
        ------
        FileNotFoundError
            If no Zarr metadata is found at the specified path
        """
        await self._ensure_open()

        # Normalize path
        path = normalize_path(path)

        # Check v3 (zarr.json)
        zarr_json_key = _join_paths([path, ZARR_JSON]) if path else ZARR_JSON
        zarr_json_exists = await self._store.exists(zarr_json_key)

        # Check v2 (.zarray or .zgroup)
        zarray_key = _join_paths([path, ZARRAY_JSON]) if path else ZARRAY_JSON
        zgroup_key = _join_paths([path, ZGROUP_JSON]) if path else ZGROUP_JSON

        # Check both in parallel
        v2_exists_results = await asyncio.gather(
            self._store.exists(zarray_key),
            self._store.exists(zgroup_key),
        )

        # Check if both v2 and v3 exist
        if zarr_json_exists and any(v2_exists_results):
            import warnings

            from zarr.errors import ZarrUserWarning

            # Determine which v2 metadata file exists for accurate warning
            zarray_exists, zgroup_exists = v2_exists_results
            v2_file = ".zarray" if zarray_exists else ".zgroup"

            # Warn and favor v3
            # Use store representation for consistency with existing tests
            store_repr = str(self._store) if path == "" else path
            msg = f"Both zarr.json (Zarr format 3) and {v2_file} (Zarr format 2) metadata objects exist at {store_repr}. Zarr v3 will be used."
            warnings.warn(msg, category=ZarrUserWarning, stacklevel=2)

        if zarr_json_exists:
            self._zarr_format = 3
            self._format_detected = True
            return 3

        if any(v2_exists_results):
            self._zarr_format = 2
            self._format_detected = True
            return 2

        # If nothing found at the specified path, scan the entire store for any metadata
        async for key in self._store.list_prefix(""):
            if key.endswith(ZARR_JSON):
                self._zarr_format = 3
                self._format_detected = True
                return 3
            elif key.endswith(ZARRAY_JSON) or key.endswith(ZGROUP_JSON):
                self._zarr_format = 2
                self._format_detected = True
                return 2

        raise FileNotFoundError(f"No Zarr metadata found at path: {path!r}")

    # ===========================
    # Metadata Operations
    # ===========================

    async def _get_metadata_v2(self, path: str) -> ArrayMetadata | GroupMetadata:
        """
        Fetch and parse v2 metadata (internal method).

        Fetches .zarray, .zgroup, and .zattrs files in parallel and combines them.

        Parameters
        ----------
        path : str
            Path to the node

        Returns
        -------
        metadata : ArrayMetadata | GroupMetadata
            Parsed metadata object

        Raises
        ------
        FileNotFoundError
            If neither .zarray nor .zgroup is found
        """
        zarray_key = _join_paths([path, ZARRAY_JSON]) if path else ZARRAY_JSON
        zgroup_key = _join_paths([path, ZGROUP_JSON]) if path else ZGROUP_JSON
        zattrs_key = _join_paths([path, ZATTRS_JSON]) if path else ZATTRS_JSON

        # Fetch all three files in parallel
        zarray_bytes, zgroup_bytes, zattrs_bytes = await asyncio.gather(
            self._store.get(zarray_key, prototype=default_buffer_prototype()),
            self._store.get(zgroup_key, prototype=default_buffer_prototype()),
            self._store.get(zattrs_key, prototype=default_buffer_prototype()),
        )

        # Parse attributes
        if zattrs_bytes is None:
            zattrs = {}
        else:
            zattrs = json.loads(zattrs_bytes.to_bytes())

        # Parse main metadata
        if zarray_bytes is not None:
            zmeta = json.loads(zarray_bytes.to_bytes())
        elif zgroup_bytes is not None:
            zmeta = json.loads(zgroup_bytes.to_bytes())
        else:
            raise FileNotFoundError(path)

        # Build and return appropriate metadata type
        return self._build_metadata_v2(zmeta, zattrs)

    async def _get_metadata_v3(self, path: str) -> ArrayMetadata | GroupMetadata:
        """
        Fetch and parse v3 metadata (internal method).

        Fetches zarr.json file.

        Parameters
        ----------
        path : str
            Path to the node

        Returns
        -------
        metadata : ArrayMetadata | GroupMetadata
            Parsed metadata object

        Raises
        ------
        FileNotFoundError
            If zarr.json is not found
        """
        zarr_json_key = _join_paths([path, ZARR_JSON]) if path else ZARR_JSON

        zarr_json_bytes = await self._store.get(zarr_json_key, prototype=default_buffer_prototype())

        if zarr_json_bytes is None:
            raise FileNotFoundError(path)

        zarr_json = json.loads(zarr_json_bytes.to_bytes())
        return self._build_metadata_v3(zarr_json)

    @staticmethod
    def _build_metadata_v2(
        zarr_json: dict[str, Any], attrs_json: dict[str, Any]
    ) -> ArrayMetadata | GroupMetadata:
        """
        Convert a dict representation of Zarr V2 metadata into the corresponding metadata class.

        Parameters
        ----------
        zarr_json : dict
            The parsed .zarray or .zgroup JSON
        attrs_json : dict
            The parsed .zattrs JSON

        Returns
        -------
        metadata : ArrayMetadata | GroupMetadata
            The metadata object
        """
        from zarr.core.group import GroupMetadata
        from zarr.core.metadata import ArrayV2Metadata

        # Check if it's an array (has 'shape' key) or group
        if "shape" in zarr_json:
            return ArrayV2Metadata.from_dict(zarr_json | {"attributes": attrs_json})
        else:
            return GroupMetadata.from_dict(zarr_json | {"attributes": attrs_json})

    @staticmethod
    def _build_metadata_v3(zarr_json: dict[str, Any]) -> ArrayMetadata | GroupMetadata:
        """
        Convert a dict representation of Zarr V3 metadata into the corresponding metadata class.

        Parameters
        ----------
        zarr_json : dict
            The parsed zarr.json

        Returns
        -------
        metadata : ArrayMetadata | GroupMetadata
            The metadata object

        Raises
        ------
        ValueError
            If node_type is missing or invalid
        """
        from zarr.core.group import GroupMetadata
        from zarr.core.metadata import ArrayV3Metadata

        if "node_type" not in zarr_json:
            msg = "Required key 'node_type' is missing from the provided metadata document."
            raise ValueError(msg)

        node_type = zarr_json["node_type"]
        if node_type == "array":
            return ArrayV3Metadata.from_dict(zarr_json)
        elif node_type == "group":
            return GroupMetadata.from_dict(zarr_json)
        else:
            raise ValueError(
                f"Invalid value for 'node_type' key in metadata document: {node_type!r}"
            )

    async def get_metadata(self, path: str) -> ArrayMetadata | GroupMetadata:
        """
        Fetch and parse metadata for a node.

        This method uses the store's format (auto-detected or explicit) and
        handles v2/v3 differences transparently.

        Parameters
        ----------
        path : str
            Path to the node

        Returns
        -------
        metadata : ArrayMetadata | GroupMetadata
            Parsed metadata object (ArrayV2Metadata, ArrayV3Metadata, or GroupMetadata)

        Raises
        ------
        FileNotFoundError
            If no metadata is found at the path

        Examples
        --------
        >>> metadata = await hl_store.get_metadata("array1")
        >>> print(f"Shape: {metadata.shape}")
        """
        await self._ensure_open()
        zarr_format = await self._ensure_format()

        path = normalize_path(path)

        if zarr_format == 2:
            return await self._get_metadata_v2(path)
        else:
            return await self._get_metadata_v3(path)

    async def get_array_metadata(self, path: str) -> ArrayMetadata:
        """
        Fetch array metadata with type validation.

        Parameters
        ----------
        path : str
            Path to the array

        Returns
        -------
        metadata : ArrayMetadata
            Parsed array metadata (ArrayV2Metadata or ArrayV3Metadata)

        Raises
        ------
        FileNotFoundError
            If no metadata is found at the path
        NodeTypeValidationError
            If the node is not an array

        Examples
        --------
        >>> metadata = await hl_store.get_array_metadata("array1")
        >>> print(f"Shape: {metadata.shape}, Dtype: {metadata.dtype}")
        """
        from zarr.core.metadata import ArrayV2Metadata, ArrayV3Metadata

        metadata = await self.get_metadata(path)
        if not isinstance(metadata, (ArrayV2Metadata, ArrayV3Metadata)):
            raise NodeTypeValidationError(
                f"Expected array metadata at {path!r}, found {type(metadata).__name__}"
            )
        return metadata

    async def get_group_metadata(self, path: str) -> GroupMetadata:
        """
        Fetch group metadata with type validation.

        Parameters
        ----------
        path : str
            Path to the group

        Returns
        -------
        metadata : GroupMetadata
            Parsed group metadata

        Raises
        ------
        FileNotFoundError
            If no metadata is found at the path
        NodeTypeValidationError
            If the node is an array (not a group)

        Examples
        --------
        >>> metadata = await hl_store.get_group_metadata("group1")
        >>> print(f"Attributes: {metadata.attributes}")
        """
        from zarr.core.group import GroupMetadata

        metadata = await self.get_metadata(path)
        if not isinstance(metadata, GroupMetadata):
            raise NodeTypeValidationError(
                f"Expected group metadata at {path!r}, found {type(metadata).__name__}"
            )
        return metadata

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
        import json

        from zarr.core.common import ZARR_JSON

        await self._ensure_open()
        path = normalize_path(path)

        zarr_format = await self._ensure_format()

        if zarr_format == 2:
            # v2: Check for .zmetadata file
            consolidated_bytes = await self._store.get(
                _join_paths([path, consolidated_key]), prototype=default_buffer_prototype()
            )
            return consolidated_bytes is not None
        elif zarr_format == 3:
            # v3: Check inside zarr.json for consolidated_metadata field
            zarr_json_bytes = await self._store.get(
                _join_paths([path, ZARR_JSON]), prototype=default_buffer_prototype()
            )
            if zarr_json_bytes is not None:
                try:
                    group_metadata = json.loads(zarr_json_bytes.to_bytes())
                    return group_metadata.get("consolidated_metadata") is not None
                except (json.JSONDecodeError, AttributeError):
                    return False
            return False
        else:
            # Should not reach here since _ensure_format() always returns 2 or 3
            return False

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
        from zarr.core.common import ZARR_JSON, ZATTRS_JSON, ZGROUP_JSON

        await self._ensure_open()
        path = normalize_path(path)

        zarr_format = await self._ensure_format()

        if zarr_format == 2:
            # For v2, read .zgroup, .zattrs, and .zmetadata
            zgroup_bytes, zattrs_bytes, consolidated_bytes = await asyncio.gather(
                self._store.get(
                    _join_paths([path, ZGROUP_JSON]), prototype=default_buffer_prototype()
                ),
                self._store.get(
                    _join_paths([path, ZATTRS_JSON]), prototype=default_buffer_prototype()
                ),
                self._store.get(
                    _join_paths([path, consolidated_key]), prototype=default_buffer_prototype()
                ),
            )
            return {
                "zarr_json_bytes": None,
                "zgroup_bytes": zgroup_bytes,
                "zattrs_bytes": zattrs_bytes,
                "consolidated_bytes": consolidated_bytes,
                "detected_format": 2,
            }
        elif zarr_format == 3:
            # For v3, consolidated metadata is embedded in zarr.json
            zarr_json_bytes = await self._store.get(
                _join_paths([path, ZARR_JSON]), prototype=default_buffer_prototype()
            )
            return {
                "zarr_json_bytes": zarr_json_bytes,
                "zgroup_bytes": None,
                "zattrs_bytes": None,
                "consolidated_bytes": None,
                "detected_format": 3,
            }
        else:
            # Should not reach here since _ensure_format() always returns 2 or 3
            raise ValueError(f"Unsupported zarr_format: {zarr_format}")

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
        import json
        from collections import defaultdict

        from zarr.core.group import GroupMetadata
        from zarr.errors import NodeTypeValidationError

        # Get raw metadata bytes
        metadata_bytes = await self.get_group_metadata_bytes(
            path, consolidated_key=consolidated_key
        )
        detected_format = metadata_bytes["detected_format"]

        if detected_format == 2:
            # Parse v2 metadata
            zgroup_bytes = metadata_bytes["zgroup_bytes"]
            zattrs_bytes = metadata_bytes["zattrs_bytes"]
            consolidated_bytes = metadata_bytes["consolidated_bytes"]

            if zgroup_bytes is None:
                raise FileNotFoundError(path)

            if use_consolidated is True and consolidated_bytes is None:
                raise ValueError(f"Consolidated metadata requested but not found at '{path}'")

            # Parse JSON
            zgroup = json.loads(zgroup_bytes.to_bytes())
            zattrs = json.loads(zattrs_bytes.to_bytes()) if zattrs_bytes is not None else {}
            group_metadata_dict = {**zgroup, "attributes": zattrs}

            # Handle consolidated metadata if present and not explicitly disabled
            if consolidated_bytes is not None and use_consolidated is not False:
                v2_consolidated_metadata = json.loads(consolidated_bytes.to_bytes())
                v2_consolidated_metadata = v2_consolidated_metadata["metadata"]
                # We already read zattrs and zgroup. Should we ignore these?
                v2_consolidated_metadata.pop(".zattrs", None)
                v2_consolidated_metadata.pop(".zgroup", None)

                consolidated_metadata: defaultdict[str, dict[str, Any]] = defaultdict(dict)

                # keys like air/.zarray, air/.zattrs
                for k, v in v2_consolidated_metadata.items():
                    child_path, kind = k.rsplit("/.", 1)

                    if kind == "zarray":
                        consolidated_metadata[child_path].update(v)
                    elif kind == "zattrs":
                        consolidated_metadata[child_path]["attributes"] = v
                    elif kind == "zgroup":
                        consolidated_metadata[child_path].update(v)
                    else:
                        raise ValueError(f"Invalid file type '{kind}' at path '{child_path}")

                group_metadata_dict["consolidated_metadata"] = {
                    "metadata": dict(consolidated_metadata),
                    "kind": "inline",
                    "must_understand": False,
                }

            # Validate node type before creating GroupMetadata
            node_type = group_metadata_dict.get("node_type")
            if node_type == "array":
                raise NodeTypeValidationError(
                    f"Expected group metadata at {path!r}, found array metadata"
                )

            return GroupMetadata.from_dict(group_metadata_dict)

        elif detected_format == 3:
            # Parse v3 metadata
            zarr_json_bytes = metadata_bytes["zarr_json_bytes"]

            if zarr_json_bytes is None:
                raise FileNotFoundError(path)

            group_metadata_dict = json.loads(zarr_json_bytes.to_bytes())

            if (
                use_consolidated is True
                and group_metadata_dict.get("consolidated_metadata") is None
            ):
                raise ValueError(
                    f"Consolidated metadata requested with 'use_consolidated=True' but not found in '{path}'."
                )

            if use_consolidated is False:
                # Drop consolidated metadata if it's there
                group_metadata_dict.pop("consolidated_metadata", None)

            # Validate node type before creating GroupMetadata
            node_type = group_metadata_dict.get("node_type")
            if node_type == "array":
                raise NodeTypeValidationError(
                    f"Expected group metadata at {path!r}, found array metadata"
                )

            return GroupMetadata.from_dict(group_metadata_dict)

        else:
            raise ValueError(f"Unsupported zarr_format: {detected_format}")

    async def set_metadata(
        self,
        path: str,
        metadata: ArrayMetadata | GroupMetadata,
        *,
        ensure_parents: bool = False,
    ) -> None:
        """
        Write metadata to storage.

        Parameters
        ----------
        path : str
            Path to write metadata
        metadata : ArrayMetadata | GroupMetadata
            Metadata object to write
        ensure_parents : bool, default False
            If True, create parent groups if they don't exist

        Raises
        ------
        PermissionError
            If the store is read-only

        Examples
        --------
        >>> # Create array metadata
        >>> await hl_store.set_metadata("array1", array_metadata)
        >>>
        >>> # Create with parent groups
        >>> await hl_store.set_metadata("a/b/c/array1", metadata, ensure_parents=True)
        """
        from zarr.core.group import GroupMetadata

        await self._ensure_open()

        if self.read_only:
            raise PermissionError("Store is read-only")

        path = normalize_path(path)

        # Create parent groups if requested
        if ensure_parents and path:
            parts = path.split("/")
            for i in range(1, len(parts)):
                parent_path = "/".join(parts[:i])
                # Check if parent exists
                try:
                    await self.get_metadata(parent_path)
                except FileNotFoundError:
                    # Create parent group with same format as metadata
                    parent_metadata = GroupMetadata(zarr_format=metadata.zarr_format)
                    await self.set_metadata(parent_path, parent_metadata, ensure_parents=False)

        # Convert metadata to buffer dict
        prototype = default_buffer_prototype()
        buffer_dict = metadata.to_buffer_dict(prototype)

        # Write all buffers
        for key, buffer in buffer_dict.items():
            full_key = _join_paths([path, key]) if path else key
            await self._store.set(full_key, buffer)

    # ===========================
    # Node Detection
    # ===========================

    async def get_node_type(self, path: str) -> Literal["array", "group", "nothing"]:
        """
        Determine the type of node at the given path.

        Parameters
        ----------
        path : str
            Path to check

        Returns
        -------
        node_type : Literal["array", "group", "nothing"]
            The type of node found at the path

        Examples
        --------
        >>> node_type = await hl_store.get_node_type("array1")
        >>> if node_type == "array":
        ...     print("Found an array")
        """
        await self._ensure_open()
        zarr_format = await self._ensure_format()

        path = normalize_path(path)

        # Check for array and group
        is_array = await self.contains_array(path)
        is_group = await self.contains_group(path)

        if is_array and is_group:
            msg = (
                f"Both array and group metadata found at {path!r}. "
                "This is invalid - only one type of metadata may exist at a path."
            )
            raise ContainsArrayAndGroupError(msg)
        elif is_array:
            return "array"
        elif is_group:
            return "group"
        else:
            return "nothing"

    async def contains_array(self, path: str) -> bool:
        """
        Check if an array exists at the given path.

        Parameters
        ----------
        path : str
            Path to check

        Returns
        -------
        bool
            True if an array exists at the path

        Examples
        --------
        >>> if await hl_store.contains_array("array1"):
        ...     print("Array exists")
        """
        await self._ensure_open()
        zarr_format = await self._ensure_format()

        path = normalize_path(path)

        if zarr_format == 3:
            # Check for zarr.json with node_type: array
            zarr_json_key = _join_paths([path, ZARR_JSON]) if path else ZARR_JSON
            try:
                zarr_json_bytes = await self._store.get(
                    zarr_json_key, prototype=default_buffer_prototype()
                )
                if zarr_json_bytes is None:
                    return False
                zarr_json = json.loads(zarr_json_bytes.to_bytes())
                return zarr_json.get("node_type") == "array"
            except Exception:
                return False
        else:  # v2
            # Check for .zarray
            zarray_key = _join_paths([path, ZARRAY_JSON]) if path else ZARRAY_JSON
            return await self._store.exists(zarray_key)

    async def contains_group(self, path: str) -> bool:
        """
        Check if a group exists at the given path.

        Parameters
        ----------
        path : str
            Path to check

        Returns
        -------
        bool
            True if a group exists at the path

        Examples
        --------
        >>> if await hl_store.contains_group("group1"):
        ...     print("Group exists")
        """
        await self._ensure_open()
        zarr_format = await self._ensure_format()

        path = normalize_path(path)

        if zarr_format == 3:
            # Check for zarr.json with node_type: group
            zarr_json_key = _join_paths([path, ZARR_JSON]) if path else ZARR_JSON
            try:
                zarr_json_bytes = await self._store.get(
                    zarr_json_key, prototype=default_buffer_prototype()
                )
                if zarr_json_bytes is None:
                    return False
                zarr_json = json.loads(zarr_json_bytes.to_bytes())
                return zarr_json.get("node_type") == "group"
            except Exception:
                return False
        else:  # v2
            # Check for .zgroup
            zgroup_key = _join_paths([path, ZGROUP_JSON]) if path else ZGROUP_JSON
            return await self._store.exists(zgroup_key)

    async def ensure_no_existing_node(
        self,
        path: str,
        *,
        node_type: Literal["array", "group"] | None = None,
    ) -> None:
        """
        Validate that no node exists at the given path.

        Raises an error if a node is found, unless the node_type parameter
        allows it.

        Parameters
        ----------
        path : str
            Path to check
        node_type : Literal["array", "group"] | None, default None
            If "array", only raise error for existing arrays
            If "group", only raise error for existing groups
            If None, raise error for any existing node

        Raises
        ------
        ContainsArrayError
            If an array exists and node_type != "group"
        ContainsGroupError
            If a group exists and node_type != "array"
        ContainsArrayAndGroupError
            If both array and group exist

        Examples
        --------
        >>> # Ensure path is clear for new array
        >>> await hl_store.ensure_no_existing_node("new_array")
        >>>
        >>> # Allow overwriting group with array
        >>> await hl_store.ensure_no_existing_node("path", node_type="array")
        """
        await self._ensure_open()

        path = normalize_path(path)

        extant_node = await self.get_node_type(path)

        if extant_node == "array":
            if node_type != "group":
                msg = f"An array already exists at {path!r}"
                raise ContainsArrayError(msg)
        elif extant_node == "group":
            if node_type != "array":
                msg = f"A group already exists at {path!r}"
                raise ContainsGroupError(msg)

    # ===========================
    # Deletion Operations
    # ===========================

    async def delete_node(self, path: str) -> None:
        """
        Delete any node (array or group) and all its contents.

        Parameters
        ----------
        path : str
            Path to the node to delete

        Raises
        ------
        PermissionError
            If the store is read-only
        FileNotFoundError
            If no node exists at the path

        Examples
        --------
        >>> await hl_store.delete_node("array_or_group")
        """
        await self._ensure_open()

        if self.read_only:
            raise PermissionError("Store is read-only")

        path = normalize_path(path)

        # Determine node type and delegate
        node_type = await self.get_node_type(path)

        if node_type == "array":
            await self.delete_array(path)
        elif node_type == "group":
            await self.delete_group(path)
        else:
            raise FileNotFoundError(f"No node found at {path!r}")

    async def delete_array(
        self,
        path: str,
        *,
        delete_chunks: bool = True,
    ) -> None:
        """
        Delete an array and optionally its chunks.

        Parameters
        ----------
        path : str
            Path to the array
        delete_chunks : bool, default True
            If True, delete all chunks. If False, only delete metadata.

        Raises
        ------
        PermissionError
            If the store is read-only
        FileNotFoundError
            If no array exists at the path

        Examples
        --------
        >>> # Delete array and all chunks
        >>> await hl_store.delete_array("array1")
        >>>
        >>> # Delete only metadata, keep chunks
        >>> await hl_store.delete_array("array1", delete_chunks=False)
        """
        await self._ensure_open()
        zarr_format = await self._ensure_format()

        if self.read_only:
            raise PermissionError("Store is read-only")

        path = normalize_path(path)

        # Verify it's an array
        if not await self.contains_array(path):
            raise FileNotFoundError(f"No array found at {path!r}")

        # Delete chunks if requested
        if delete_chunks:
            try:
                metadata = await self.get_array_metadata(path)
                # List and delete all chunks
                async for coords in self.list_chunks(path, metadata=metadata):
                    await self.delete_chunk(path, coords, metadata=metadata)
            except Exception:
                # If we can't list chunks, try deleting by prefix
                chunk_prefix = f"{path}/" if path else ""
                async for key in self._store.list_prefix(chunk_prefix):
                    # Skip metadata files
                    if not any(
                        key.endswith(meta)
                        for meta in [ZARR_JSON, ZARRAY_JSON, ZATTRS_JSON, ZGROUP_JSON]
                    ):
                        await self._store.delete(key)

        # Delete metadata files
        if zarr_format == 3:
            zarr_json_key = _join_paths([path, ZARR_JSON]) if path else ZARR_JSON
            await self._store.delete(zarr_json_key)
        else:  # v2
            zarray_key = _join_paths([path, ZARRAY_JSON]) if path else ZARRAY_JSON
            zattrs_key = _join_paths([path, ZATTRS_JSON]) if path else ZATTRS_JSON
            await self._store.delete(zarray_key)
            # Try to delete .zattrs, but don't fail if it doesn't exist
            try:
                await self._store.delete(zattrs_key)
            except Exception:
                pass

    async def delete_group(
        self,
        path: str,
        *,
        recursive: bool = True,
    ) -> None:
        """
        Delete a group and optionally its children.

        Parameters
        ----------
        path : str
            Path to the group
        recursive : bool, default True
            If True, delete all children recursively.
            If False, raise an error if the group is not empty.

        Raises
        ------
        PermissionError
            If the store is read-only
        FileNotFoundError
            If no group exists at the path
        ValueError
            If recursive=False and the group contains children

        Examples
        --------
        >>> # Delete group and all children
        >>> await hl_store.delete_group("group1")
        >>>
        >>> # Delete only if empty
        >>> await hl_store.delete_group("group1", recursive=False)
        """
        await self._ensure_open()
        zarr_format = await self._ensure_format()

        if self.read_only:
            raise PermissionError("Store is read-only")

        path = normalize_path(path)

        # Verify it's a group
        if not await self.contains_group(path):
            raise FileNotFoundError(f"No group found at {path!r}")

        # Check for children
        has_children = False
        async for _ in self.list_children(path):
            has_children = True
            break

        if has_children:
            if not recursive:
                raise ValueError(
                    f"Group at {path!r} is not empty. Use recursive=True to delete it."
                )

            # Delete all children recursively
            async for child_name in self.list_children(path):
                child_path = _join_paths([path, child_name])
                await self.delete_node(child_path)

        # Delete metadata files
        if zarr_format == 3:
            zarr_json_key = _join_paths([path, ZARR_JSON]) if path else ZARR_JSON
            await self._store.delete(zarr_json_key)
        else:  # v2
            zgroup_key = _join_paths([path, ZGROUP_JSON]) if path else ZGROUP_JSON
            zattrs_key = _join_paths([path, ZATTRS_JSON]) if path else ZATTRS_JSON
            await self._store.delete(zgroup_key)
            # Try to delete .zattrs, but don't fail if it doesn't exist
            try:
                await self._store.delete(zattrs_key)
            except Exception:
                pass

    # ===========================
    # Chunk Operations
    # ===========================

    @staticmethod
    def _decode_chunk_key(chunk_key: str, metadata: ArrayMetadata) -> tuple[int, ...] | None:
        """
        Decode a chunk key back to coordinates.

        Parameters
        ----------
        chunk_key : str
            The chunk key string (e.g., "0.0" for v2 or "c/0/0" for v3)
        metadata : ArrayMetadata
            The array metadata containing encoding information

        Returns
        -------
        coords : tuple[int, ...] | None
            The decoded chunk coordinates, or None if invalid
        """
        from zarr.core.metadata import ArrayV2Metadata, ArrayV3Metadata

        try:
            if isinstance(metadata, ArrayV2Metadata):
                # V2 format: "0.0" or "0/0" depending on dimension_separator
                separator = metadata.dimension_separator
                parts = chunk_key.split(separator)
                return tuple(int(p) for p in parts)
            elif isinstance(metadata, ArrayV3Metadata):
                # V3 format: "c/0/0" with prefix
                if not chunk_key.startswith("c/"):
                    return None
                parts = chunk_key[2:].split("/")
                return tuple(int(p) for p in parts)
            else:
                return None
        except (ValueError, AttributeError):
            return None

    async def get_chunk(
        self,
        path: str,
        chunk_coords: tuple[int, ...],
        *,
        metadata: ArrayMetadata | None = None,
        prototype: BufferPrototype | None = None,
    ) -> Buffer | None:
        """
        Get raw encoded chunk bytes.

        Parameters
        ----------
        path : str
            Path to the array
        chunk_coords : tuple[int, ...]
            Chunk coordinates
        metadata : ArrayMetadata | None
            Array metadata. If None, will be fetched.
        prototype : BufferPrototype | None
            Buffer prototype. If None, uses default.

        Returns
        -------
        buffer : Buffer | None
            The chunk data, or None if chunk doesn't exist

        Examples
        --------
        >>> chunk = await hl_store.get_chunk("array1", (0, 0))
        >>> if chunk:
        ...     print(f"Chunk size: {len(chunk.to_bytes())} bytes")
        """
        await self._ensure_open()

        if metadata is None:
            metadata = await self.get_array_metadata(path)

        if prototype is None:
            prototype = default_buffer_prototype()

        # Encode chunk key
        chunk_key = metadata.encode_chunk_key(chunk_coords)
        full_key = _join_paths([path, chunk_key])

        # Get chunk from store
        return await self._store.get(full_key, prototype=prototype)

    async def set_chunk(
        self,
        path: str,
        chunk_coords: tuple[int, ...],
        data: Buffer,
        *,
        metadata: ArrayMetadata | None = None,
    ) -> None:
        """
        Set raw encoded chunk bytes.

        Parameters
        ----------
        path : str
            Path to the array
        chunk_coords : tuple[int, ...]
            Chunk coordinates
        data : Buffer
            The chunk data to write
        metadata : ArrayMetadata | None
            Array metadata. If None, will be fetched.

        Raises
        ------
        PermissionError
            If the store is read-only

        Examples
        --------
        >>> await hl_store.set_chunk("array1", (0, 0), chunk_buffer)
        """
        await self._ensure_open()

        if self.read_only:
            raise PermissionError("Store is read-only")

        if metadata is None:
            metadata = await self.get_array_metadata(path)

        # Encode chunk key
        chunk_key = metadata.encode_chunk_key(chunk_coords)
        full_key = _join_paths([path, chunk_key])

        # Set chunk in store
        await self._store.set(full_key, data)

    async def delete_chunk(
        self,
        path: str,
        chunk_coords: tuple[int, ...],
        *,
        metadata: ArrayMetadata | None = None,
    ) -> None:
        """
        Delete a specific chunk.

        Parameters
        ----------
        path : str
            Path to the array
        chunk_coords : tuple[int, ...]
            Chunk coordinates
        metadata : ArrayMetadata | None
            Array metadata. If None, will be fetched.

        Raises
        ------
        PermissionError
            If the store is read-only

        Examples
        --------
        >>> await hl_store.delete_chunk("array1", (0, 0))
        """
        await self._ensure_open()

        if self.read_only:
            raise PermissionError("Store is read-only")

        if metadata is None:
            metadata = await self.get_array_metadata(path)

        # Encode chunk key
        chunk_key = metadata.encode_chunk_key(chunk_coords)
        full_key = _join_paths([path, chunk_key])

        # Delete chunk from store
        await self._store.delete(full_key)

    async def exists_chunk(
        self,
        path: str,
        chunk_coords: tuple[int, ...],
        *,
        metadata: ArrayMetadata | None = None,
    ) -> bool:
        """
        Check if a chunk exists.

        Parameters
        ----------
        path : str
            Path to the array
        chunk_coords : tuple[int, ...]
            Chunk coordinates
        metadata : ArrayMetadata | None
            Array metadata. If None, will be fetched.

        Returns
        -------
        bool
            True if the chunk exists

        Examples
        --------
        >>> if await hl_store.exists_chunk("array1", (0, 0)):
        ...     print("Chunk exists")
        """
        await self._ensure_open()

        if metadata is None:
            metadata = await self.get_array_metadata(path)

        # Encode chunk key
        chunk_key = metadata.encode_chunk_key(chunk_coords)
        full_key = _join_paths([path, chunk_key])

        # Check if chunk exists in store
        return await self._store.exists(full_key)

    async def list_chunks(
        self,
        path: str,
        *,
        metadata: ArrayMetadata | None = None,
    ) -> AsyncIterator[tuple[int, ...]]:
        """
        Iterate over all chunk coordinates.

        Parameters
        ----------
        path : str
            Path to the array
        metadata : ArrayMetadata | None
            Array metadata. If None, will be fetched.

        Yields
        ------
        chunk_coords : tuple[int, ...]
            Coordinates of each chunk found

        Examples
        --------
        >>> async for coords in hl_store.list_chunks("array1"):
        ...     print(f"Found chunk at {coords}")
        """
        await self._ensure_open()

        if metadata is None:
            metadata = await self.get_array_metadata(path)

        # List all keys with the array path prefix
        prefix = f"{path}/" if path else ""

        async for key in self._store.list_prefix(prefix):
            # Skip metadata files
            if any(
                key.endswith(meta) for meta in [ZARR_JSON, ZARRAY_JSON, ZATTRS_JSON, ZGROUP_JSON]
            ):
                continue

            # Try to decode chunk key
            try:
                # Remove path prefix to get relative chunk key
                if path:
                    if not key.startswith(f"{path}/"):
                        continue
                    chunk_key = key[len(f"{path}/") :]
                else:
                    chunk_key = key

                # Decode chunk coordinates
                coords = self._decode_chunk_key(chunk_key, metadata)
                if coords is not None:
                    yield coords
            except Exception:
                # Not a valid chunk key, skip
                continue

    # ===========================
    # Storage Metrics
    # ===========================

    async def get_size(
        self,
        path: str,
        *,
        include_chunks: bool = True,
        include_metadata: bool = True,
    ) -> int:
        """
        Get total storage size in bytes.

        Parameters
        ----------
        path : str
            Path to the node
        include_chunks : bool, default True
            Include chunk data in size calculation
        include_metadata : bool, default True
            Include metadata files in size calculation

        Returns
        -------
        size : int
            Total size in bytes

        Examples
        --------
        >>> size = await hl_store.get_size("array1")
        >>> print(f"Array size: {size / 1024**2:.2f} MB")
        """
        await self._ensure_open()
        zarr_format = await self._ensure_format()

        path = normalize_path(path)

        total_size = 0

        # List all keys with the path prefix
        prefix = f"{path}/" if path else ""

        async for key in self._store.list_prefix(prefix):
            # Check if it's a metadata file
            is_metadata = any(
                key.endswith(meta) for meta in [ZARR_JSON, ZARRAY_JSON, ZATTRS_JSON, ZGROUP_JSON]
            )

            # Skip based on include flags
            if is_metadata and not include_metadata:
                continue
            if not is_metadata and not include_chunks:
                continue

            # Get size of this key
            try:
                buffer = await self._store.get(key, prototype=default_buffer_prototype())
                if buffer is not None:
                    total_size += len(buffer.to_bytes())
            except Exception:
                # If we can't get the key, skip it
                continue

        return total_size

    async def get_array_storage_info(
        self,
        path: str,
        *,
        metadata: ArrayMetadata | None = None,
    ) -> dict[str, Any]:
        """
        Get detailed array storage metrics.

        Parameters
        ----------
        path : str
            Path to the array
        metadata : ArrayMetadata | None
            Array metadata. If None, will be fetched.

        Returns
        -------
        info : dict[str, Any]
            Dictionary containing:
            - total_size: Total bytes
            - metadata_size: Metadata bytes
            - chunk_count: Number of chunks stored
            - chunk_size_total: Total chunk bytes
            - chunk_size_mean: Mean chunk size (if chunks exist)
            - chunk_size_min: Minimum chunk size (if chunks exist)
            - chunk_size_max: Maximum chunk size (if chunks exist)
            - expected_chunks: Expected number of chunks
            - missing_chunks: Number of missing chunks
            - storage_ratio: Ratio of stored to expected chunks

        Examples
        --------
        >>> info = await hl_store.get_array_storage_info("array1")
        >>> print(f"Storage: {info['chunk_count']}/{info['expected_chunks']} chunks")
        """
        await self._ensure_open()

        if metadata is None:
            metadata = await self.get_array_metadata(path)

        # Get metadata size
        metadata_size = await self.get_size(path, include_chunks=False, include_metadata=True)

        # Analyze chunks
        chunk_sizes: list[int] = []
        async for coords in self.list_chunks(path, metadata=metadata):
            chunk = await self.get_chunk(path, coords, metadata=metadata)
            if chunk is not None:
                chunk_sizes.append(len(chunk.to_bytes()))

        chunk_count = len(chunk_sizes)
        chunk_size_total = sum(chunk_sizes)

        # Calculate expected chunks
        import math

        expected_chunks = 1
        for dim_size, chunk_size in zip(metadata.shape, metadata.chunks, strict=False):
            expected_chunks *= math.ceil(dim_size / chunk_size)

        return {
            "total_size": metadata_size + chunk_size_total,
            "metadata_size": metadata_size,
            "chunk_count": chunk_count,
            "chunk_size_total": chunk_size_total,
            "chunk_size_mean": chunk_size_total / chunk_count if chunk_count > 0 else 0,
            "chunk_size_min": min(chunk_sizes) if chunk_sizes else 0,
            "chunk_size_max": max(chunk_sizes) if chunk_sizes else 0,
            "expected_chunks": expected_chunks,
            "missing_chunks": expected_chunks - chunk_count,
            "storage_ratio": chunk_count / expected_chunks if expected_chunks > 0 else 0,
        }

    async def get_group_storage_info(
        self,
        path: str,
        *,
        recursive: bool = True,
    ) -> dict[str, Any]:
        """
        Get detailed group storage metrics.

        Parameters
        ----------
        path : str
            Path to the group
        recursive : bool, default True
            Include children in calculation

        Returns
        -------
        info : dict[str, Any]
            Dictionary containing:
            - total_size: Total bytes
            - metadata_size: Metadata bytes
            - array_count: Number of arrays
            - group_count: Number of groups (including this one)
            - chunk_count: Total chunks
            - children: Dict of child storage info (if recursive)

        Examples
        --------
        >>> info = await hl_store.get_group_storage_info("group1")
        >>> print(f"Group contains {info['array_count']} arrays")
        """
        await self._ensure_open()

        # Get metadata size
        metadata_size = await self.get_size(path, include_chunks=False, include_metadata=True)

        total_size = metadata_size
        array_count = 0
        group_count = 1  # Include this group
        chunk_count = 0
        children_info: dict[str, Any] = {}

        if recursive:
            async for child_name in self.list_children(path):
                child_path = _join_paths([path, child_name])
                node_type = await self.get_node_type(child_path)

                if node_type == "array":
                    array_info = await self.get_array_storage_info(child_path)
                    total_size += array_info["total_size"]
                    array_count += 1
                    chunk_count += array_info["chunk_count"]
                    children_info[child_name] = array_info
                elif node_type == "group":
                    group_info = await self.get_group_storage_info(child_path, recursive=True)
                    total_size += group_info["total_size"]
                    array_count += group_info["array_count"]
                    group_count += group_info["group_count"]
                    chunk_count += group_info["chunk_count"]
                    children_info[child_name] = group_info

        return {
            "total_size": total_size,
            "metadata_size": metadata_size,
            "array_count": array_count,
            "group_count": group_count,
            "chunk_count": chunk_count,
            "children": children_info if recursive else {},
        }

    async def get_hierarchy_tree(
        self,
        path: str = "",
        *,
        max_depth: int | None = None,
        include_size: bool = True,
    ) -> dict[str, Any]:
        """
        Get nested hierarchy structure.

        Parameters
        ----------
        path : str, default ""
            Root path for the tree
        max_depth : int | None
            Maximum depth to traverse. None for unlimited.
        include_size : bool, default True
            Include size information for each node

        Returns
        -------
        tree : dict[str, Any]
            Nested dictionary representing the hierarchy

        Examples
        --------
        >>> tree = await hl_store.get_hierarchy_tree()
        >>> print(json.dumps(tree, indent=2))
        """
        await self._ensure_open()

        path = normalize_path(path)

        node_type = await self.get_node_type(path)
        if node_type == "nothing":
            raise FileNotFoundError(f"No node found at {path!r}")

        result: dict[str, Any] = {
            "name": path.split("/")[-1] if path else "root",
            "path": path,
            "type": node_type,
        }

        if include_size:
            result["size"] = await self.get_size(path)

        if node_type == "array":
            metadata = await self.get_array_metadata(path)
            result["shape"] = metadata.shape
            result["dtype"] = str(metadata.dtype)
            result["chunks"] = metadata.chunks
        elif node_type == "group":
            if max_depth is None or max_depth > 0:
                children = []
                next_depth = None if max_depth is None else max_depth - 1

                async for child_name in self.list_children(path):
                    child_path = _join_paths([path, child_name])
                    try:
                        child_tree = await self.get_hierarchy_tree(
                            child_path,
                            max_depth=next_depth,
                            include_size=include_size,
                        )
                        children.append(child_tree)
                    except Exception:
                        # Skip children we can't read
                        continue

                result["children"] = children

        return result

    # ===========================
    # Hierarchy Operations
    # ===========================

    async def list_children(self, path: str) -> AsyncIterator[str]:
        """
        List child node names.

        Filters out metadata files and returns only direct children names.

        Parameters
        ----------
        path : str
            Path to the group

        Yields
        ------
        name : str
            Name of each child node

        Examples
        --------
        >>> async for child in hl_store.list_children("group1"):
        ...     print(f"Child: {child}")
        """
        await self._ensure_open()
        zarr_format = await self._ensure_format()

        path = normalize_path(path)

        # List all keys with the path prefix
        prefix = f"{path}/" if path else ""

        # Track seen children
        seen_children: set[str] = set()

        async for key in self._store.list_dir(prefix):
            # list_dir returns paths relative to the prefix
            relative_key = key

            # Get first path component (direct child)
            parts = relative_key.split("/")
            if not parts or not parts[0]:
                continue

            child_name = parts[0]

            # Skip if we've already seen this child
            if child_name in seen_children:
                continue

            # Check if this is a valid node (has metadata)
            child_path = _join_paths([path, child_name])
            node_type = await self.get_node_type(child_path)

            if node_type != "nothing":
                seen_children.add(child_name)
                yield child_name

    async def list_children_with_metadata(self, path: str) -> AsyncIterator[tuple[str, NodeInfo]]:
        """
        List children with pre-fetched metadata.

        This is more efficient than calling list_children followed by get_metadata
        for each child, as it batches metadata fetches.

        Parameters
        ----------
        path : str
            Path to the group

        Yields
        ------
        name : str
            Name of the child node
        info : NodeInfo
            NodeInfo containing metadata and type information

        Examples
        --------
        >>> async for name, info in hl_store.list_children_with_metadata("group1"):
        ...     print(f"{name}: {info.node_type}")
        ...     if info.node_type == "array":
        ...         print(f"  Shape: {info.metadata.shape}")
        """
        from zarr.core.config import config
        from zarr.core.metadata import ArrayV2Metadata, ArrayV3Metadata

        await self._ensure_open()

        path = normalize_path(path)

        # Create tasks for fetching metadata concurrently
        async def _fetch_metadata(child_name: str) -> tuple[str, NodeInfo | None]:
            import warnings

            from zarr.errors import ZarrUserWarning

            child_path = _join_paths([path, child_name])
            try:
                metadata = await self.get_metadata(child_path)
                node_type: Literal["array", "group"] = (
                    "array" if isinstance(metadata, (ArrayV2Metadata, ArrayV3Metadata)) else "group"
                )
                return child_name, NodeInfo(
                    path=child_path,
                    node_type=node_type,
                    zarr_format=metadata.zarr_format,
                    metadata=metadata,
                )
            except FileNotFoundError:
                # Object at child_path is not recognized as a component of a Zarr hierarchy
                warnings.warn(
                    f"Object at {child_path} is not recognized as a component of a Zarr hierarchy.",
                    ZarrUserWarning,
                    stacklevel=2,
                )
                return child_name, None
            except Exception as e:
                # Other errors - warn and skip
                warnings.warn(
                    f"Error loading child node {child_path}: {e}",
                    ZarrUserWarning,
                    stacklevel=2,
                )
                return child_name, None

        # Use list_dir to get all keys (including non-Zarr objects that will trigger warnings)
        # This matches the behavior of the original _iter_members implementation
        zarr_format = await self._ensure_format()
        if zarr_format == 2:
            skip_keys = (".zattrs", ".zgroup", ".zarray", ".zmetadata")
        elif zarr_format == 3:
            skip_keys = ("zarr.json",)
        else:
            skip_keys = ()

        # Create semaphore to respect async.concurrency config
        limit = config.get("async.concurrency")
        semaphore = asyncio.Semaphore(limit)

        async def _fetch_with_semaphore(child_name: str) -> tuple[str, NodeInfo | None]:
            async with semaphore:
                return await _fetch_metadata(child_name)

        # Launch all tasks concurrently (semaphore controls actual concurrency)
        tasks = []
        async for child_name in self._store.list_dir(path):
            if child_name not in skip_keys:
                task = asyncio.create_task(_fetch_with_semaphore(child_name), name=child_name)
                tasks.append(task)

        # Yield results as they complete
        for completed_task in asyncio.as_completed(tasks):
            child_name, node_info = await completed_task
            if node_info is not None:
                yield child_name, node_info
