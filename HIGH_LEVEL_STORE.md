# HighLevelStore Design and Implementation

**Status:** Implemented ✅
**Version:** Zarr-Python 3.x
**Last Updated:** October 2025

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Core Design Principles](#core-design-principles)
4. [API Reference](#api-reference)
5. [Implementation Status](#implementation-status)
6. [Consolidated Metadata Support](#consolidated-metadata-support)
7. [Usage Examples](#usage-examples)
8. [Migration Guide](#migration-guide)

---

## Overview

### What is HighLevelStore?

HighLevelStore is a metadata-aware storage layer that sits between low-level Store (key/value operations) and high-level Array/Group abstractions. It provides a complete, format-aware API for Zarr hierarchy operations.

### Key Benefits

- **Format Abstraction**: Completely hides Zarr v2/v3 differences
- **Centralized Logic**: Single source of truth for metadata operations
- **Consolidated Metadata**: Built-in support for consolidated metadata (v2 and v3)
- **Simplified APIs**: Array/Group classes no longer need format-specific code
- **Better Testing**: Mock HighLevelStore instead of raw Store operations

### Problem It Solves

**Before HighLevelStore:**
- Format-specific code scattered across Array, Group, and storage modules
- Direct store operations (`(store_path / key).get()`) throughout codebase
- Duplicate metadata parsing logic in multiple locations
- Mixed responsibilities (Array parsing its own metadata bytes)

**After HighLevelStore:**
- All store operations go through HighLevelStore
- Format differences isolated in one place
- Array/Group work with typed metadata objects, not bytes
- Clean separation of concerns

---

## Architecture

### Layer Diagram

```
┌──────────────────────────────────────────────────────────────┐
│                  High-Level API Layer                        │
│  ┌─────────────────────┐         ┌──────────────────────┐    │
│  │  AsyncArray/Array   │         │ AsyncGroup/Group     │    │
│  │                     │         │                      │    │
│  │  - Slicing/Indexing │         │  - Hierarchy Mgmt    │    │
│  │  - Encoding/Decoding│         │  - Member Access     │    │
│  │  - Data Operations  │         │  - Attributes        │    │
│  └─────────┬───────────┘         └──────────┬───────────┘    │
└────────────┼────────────────────────────────┼────────────────┘
             │                                │
             └───────────┬────────────────────┘
                         ▼
┌──────────────────────────────────────────────────────────────┐
│              HighLevelStore Layer                            │
│                                                              │
│  Format-Aware Operations:                                    │
│  • Metadata: get/set metadata, parse consolidated metadata   │
│  • Chunks: get/set/delete chunks with format-aware keys      │
│  • Hierarchy: list children, traverse with metadata          │
│  • Node Operations: detect type, validate, delete            │
│  • Storage Metrics: size calculations, getsize_prefix        │
│                                                              │
│  Internal State:                                             │
│  • _store: Store (underlying key/value store)                │
│  • _zarr_format: ZarrFormat | None (cached format)           │
│  • _format_detected: bool (whether format is known)          │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│                  Low-Level Store Layer                       │
│                                                              │
│  Format-Agnostic Key/Value Operations:                       │
│  • async get(key) -> Buffer | None                           │
│  • async set(key, value) -> None                             │
│  • async delete(key) -> None                                 │
│  • async exists(key) -> bool                                 │
│  • async list() / list_prefix() / list_dir()                 │
│  • async getsize() / getsize_prefix()                        │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│                  Physical Storage                            │
│  • Local Filesystem                                          │
│  • S3 / Object Storage                                       │
│  • In-Memory                                                 │
│  • Zip Archives                                              │
└──────────────────────────────────────────────────────────────┘
```

### Information Flow Example: Reading Array Data

```
User: array[0:10, 0:20]
    │
    ▼
Array.__getitem__
    │ 1. Determine which chunks are needed
    │ 2. For each chunk...
    ▼
Array → HighLevelStore.get_chunk(path, coords)
                        │ 1. Uses cached zarr_format
                        │ 2. Encodes chunk key (format-specific)
                        │ 3. Fetches from store
                        ▼
                    Store.get(key) → bytes
                        │
                        ▼
                    Return Buffer
    │
    ▼
Array: Decode, combine chunks, slice to requested region
    │
    ▼
Return: ndarray to user
```

---

## Core Design Principles

### 1. Format as Store Property

**The most important design decision:** `zarr_format` is a property of the store instance, not a parameter on every method call.

**Why:**
- Stores contain either v2 OR v3 format, not both
- Simpler API (no format parameter on 20+ methods)
- Auto-detection with caching (detect once, use everywhere)
- Prevents errors (can't accidentally mix formats)

**Usage patterns:**

```python
# Auto-detection (most common)
hl_store = HighLevelStore(store)
metadata = await hl_store.get_metadata("array1")  # Auto-detects format
print(f"Using Zarr v{hl_store.zarr_format}")      # Cached result

# Explicit format (new stores)
hl_store = HighLevelStore(store, zarr_format=3)
await hl_store.set_metadata("array1", metadata)

# Manual detection
hl_store = HighLevelStore(store)
format_version = await hl_store.detect_format()
```

### 2. Store Protocol Delegation

HighLevelStore implements the Store protocol by delegating to the underlying store:

```python
async def get(self, key: str, prototype: BufferPrototype, ...) -> Buffer | None:
    return await self._store.get(key, prototype, ...)

async def set(self, key: str, value: Buffer) -> None:
    return await self._store.set(key, value)
```

This allows HighLevelStore to be used anywhere a Store is expected, while adding high-level capabilities.

### 3. Async-First

All I/O operations are async. Synchronous access through Array/Group uses the `sync()` wrapper pattern.

### 4. Clear Error Handling

- `FileNotFoundError`: Node doesn't exist
- `NodeTypeValidationError`: Wrong node type (e.g., expected group, found array)
- `ContainsArrayError`/`ContainsGroupError`: Node already exists
- `PermissionError`: Store is read-only

### 5. Type Safety

Strong typing throughout:
- `ArrayMetadata` (union of `ArrayV2Metadata` and `ArrayV3Metadata`)
- `GroupMetadata` (with optional consolidated metadata)
- `ZarrFormat` (Literal[2, 3])
- `Buffer`/`BufferPrototype`

---

## API Reference

### Initialization

```python
class HighLevelStore:
    def __init__(
        self,
        store: Store,
        *,
        zarr_format: ZarrFormat | None = None
    ) -> None:
        """
        Create a high-level store wrapper.

        Parameters
        ----------
        store : Store
            Underlying key/value store
        zarr_format : ZarrFormat | None, optional
            Format version (2 or 3). If None, auto-detect on first access.
        """
```

### Properties

```python
@property
def store(self) -> Store:
    """The underlying Store instance."""

@property
def zarr_format(self) -> ZarrFormat:
    """
    The Zarr format version (2 or 3).
    Raises RuntimeError if not yet detected.
    """

@property
def read_only(self) -> bool:
    """Is the store read-only?"""

@property
def supports_consolidated_metadata(self) -> bool:
    """
    Does the store support consolidated metadata?

    Returns True by default. Third-party HighLevelStore implementations
    can override this to return False if consolidated metadata is not
    supported or not beneficial (e.g., database-backed stores with
    native fast metadata queries).
    """
```

### Core Metadata Operations

```python
async def get_metadata(self, path: str) -> ArrayMetadata | GroupMetadata:
    """Get metadata (array or group) and return typed object."""

async def get_array_metadata(self, path: str) -> ArrayMetadata:
    """Fetch array metadata with type validation."""

async def get_group_metadata(self, path: str) -> GroupMetadata:
    """Fetch group metadata with type validation."""

async def set_metadata(
    self,
    path: str,
    metadata: ArrayMetadata | GroupMetadata,
    *,
    ensure_parents: bool = False,
) -> None:
    """Store metadata for an array or group."""
```

### Consolidated Metadata Operations

```python
async def has_consolidated_metadata(
    self, path: str, consolidated_key: str = ".zmetadata"
) -> bool:
    """Check if consolidated metadata exists at the given path."""

async def get_group_metadata_bytes(
    self, path: str, *, consolidated_key: str = ".zmetadata"
) -> dict[str, Any]:
    """
    Get raw metadata bytes for opening a group.
    Returns dict with zarr_json_bytes, zgroup_bytes, zattrs_bytes,
    consolidated_bytes, and detected_format.
    """

async def open_group_metadata(
    self,
    path: str,
    *,
    use_consolidated: bool | None = None,
    consolidated_key: str = ".zmetadata",
) -> GroupMetadata:
    """
    Open and parse group metadata with optional consolidated metadata support.

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
    """
```

### Node Detection and Format Operations

```python
async def get_node_type(self, path: str) -> Literal["array", "group", "nothing"]:
    """Determine what kind of node exists at the path."""

async def contains_array(self, path: str) -> bool:
    """Check if an array exists at the path."""

async def contains_group(self, path: str) -> bool:
    """Check if a group exists at the path."""

async def detect_format(self, path: str = "") -> ZarrFormat:
    """
    Auto-detect and cache Zarr format version.
    Checks for v3 (zarr.json) first, then v2 (.zarray or .zgroup).
    """
```

### Hierarchy Operations

```python
async def list_children(self, path: str) -> AsyncIterator[str]:
    """List direct children (names only) of a group."""

async def list_children_with_metadata(
    self, path: str
) -> AsyncIterator[tuple[str, NodeInfo]]:
    """
    List children with pre-fetched metadata.

    Yields
    ------
    tuple[str, NodeInfo]
        Child name and its metadata information (path, node_type, zarr_format, metadata)
    """
```

### Chunk Operations

```python
async def get_chunk(
    self,
    path: str,
    chunk_coords: tuple[int, ...],
    *,
    metadata: ArrayMetadata | None = None,
    prototype: BufferPrototype | None = None,
) -> Buffer | None:
    """Retrieve a chunk by coordinates."""

async def set_chunk(
    self,
    path: str,
    chunk_coords: tuple[int, ...],
    data: Buffer,
    *,
    metadata: ArrayMetadata | None = None,
) -> None:
    """Store a chunk at the given coordinates."""

async def delete_chunk(
    self,
    path: str,
    chunk_coords: tuple[int, ...],
    *,
    metadata: ArrayMetadata | None = None,
) -> None:
    """Delete a chunk."""

async def exists_chunk(
    self,
    path: str,
    chunk_coords: tuple[int, ...],
    *,
    metadata: ArrayMetadata | None = None,
) -> bool:
    """Check if a chunk exists."""

async def list_chunks(
    self,
    path: str,
    *,
    metadata: ArrayMetadata | None = None,
) -> AsyncIterator[tuple[int, ...]]:
    """List all stored chunks for an array."""
```

### Node Deletion

```python
async def delete_node(self, path: str) -> None:
    """Delete a node (array or group) and all its contents."""

async def delete_array(self, path: str, *, delete_chunks: bool = True) -> None:
    """Delete an array and optionally its chunks."""

async def delete_group(self, path: str, *, recursive: bool = True) -> None:
    """Delete a group and optionally its children."""
```

### Storage Metrics

```python
async def getsize(self, key: str) -> int:
    """Return the size, in bytes, of a value in the store."""

async def getsize_prefix(self, prefix: str) -> int:
    """Return the size, in bytes, of all values under a prefix."""
```

### Store Protocol Methods (Delegated)

HighLevelStore implements the full Store protocol by delegating to the underlying store:

```python
async def get(self, key: str, prototype: BufferPrototype, ...) -> Buffer | None
async def set(self, key: str, value: Buffer) -> None
async def set_if_not_exists(self, key: str, value: Buffer) -> None
async def delete(self, key: str) -> None
async def exists(self, key: str) -> bool
def list() -> AsyncIterator[str]
def list_prefix(prefix: str) -> AsyncIterator[str]
def list_dir(prefix: str) -> AsyncIterator[str]
```

---

## Implementation Status

### ✅ Implemented Features

1. **Core Infrastructure**
   - HighLevelStore ABC in `src/zarr/abc/store.py`
   - Concrete implementation in `src/zarr/storage/_high_level.py`
   - Format detection and caching
   - Store protocol delegation

2. **Metadata Operations**
   - `get_metadata()` - fetch and parse metadata
   - `get_array_metadata()` - with type validation
   - `get_group_metadata()` - with type validation
   - `set_metadata()` - write metadata with format handling
   - Format-specific parsing (v2 and v3)

3. **Consolidated Metadata Support**
   - `has_consolidated_metadata()` - check for consolidated metadata
   - `get_group_metadata_bytes()` - raw bytes reading
   - `open_group_metadata()` - parse with consolidated metadata
   - Handles both v2 (.zmetadata) and v3 (embedded) formats
   - Proper validation and error handling

4. **Node Operations**
   - `get_node_type()` - detect array/group/nothing
   - `contains_array()`, `contains_group()` - existence checks
   - Node type validation with proper errors

5. **Hierarchy Operations**
   - `list_children()` - list child names
   - `list_children_with_metadata()` - concurrent metadata fetching
   - Respects `async.concurrency` config for rate limiting
   - Emits warnings for unrecognized objects

6. **Chunk Operations**
   - `get_chunk()`, `set_chunk()`, `delete_chunk()` - chunk I/O
   - `exists_chunk()` - chunk existence check
   - `list_chunks()` - enumerate stored chunks
   - Format-aware chunk key encoding (v2: "0.1.2", v3: "c/0/1/2")

7. **Storage Metrics**
   - `getsize()` - size of individual keys
   - `getsize_prefix()` - size of all keys under prefix

8. **Integration with Array/Group**
   - Array and Group classes use HighLevelStore internally
   - All metadata operations go through HighLevelStore
   - No more direct store access for metadata
   - Consolidated metadata properly loaded in groups

### 🔄 Recent Refactoring

**Moved Group metadata parsing to HighLevelStore:**
- Eliminated `Group._from_bytes_v2()` and `Group._from_bytes_v3()` methods (~60 lines)
- Created `HighLevelStore.open_group_metadata()` with full parsing logic
- Simplified `AsyncGroup.open()` from ~100 lines to ~30 lines
- Group class no longer understands byte-level metadata formats

**Benefits:**
- Single source of truth for metadata parsing
- Group class focuses on domain logic, not I/O
- Better separation of concerns (HighLevelStore = storage, Group = hierarchy)
- Easier to add caching or batch operations in the future

### 📊 Code Metrics

- **Lines added**: ~1,500 (HighLevelStore implementation)
- **Lines removed**: ~390 (duplicate code in Array/Group/storage)
- **Net change**: +~1,100 lines
- **But:** Centralized, tested, reusable code vs. scattered duplicates

### ✅ Test Coverage

All functionality is tested:
- **1,869 tests passing** (group, array, consolidated metadata tests)
- Tests cover v2 and v3 formats
- Tests cover consolidated metadata scenarios
- Tests verify concurrency control
- Tests validate error handling

---

## Consolidated Metadata Support

### Overview

Consolidated metadata is a performance optimization that stores child node metadata in the parent group's metadata, avoiding separate I/O operations for each child.

**Format Support:**
- **Zarr v2**: `.zmetadata` file containing all descendant metadata
- **Zarr v3**: Embedded in `zarr.json` under `consolidated_metadata` field

### Implementation

HighLevelStore provides complete support through three methods:

#### 1. Check for Consolidated Metadata

```python
has_cm = await hl_store.has_consolidated_metadata("path/to/group")
```

Checks for:
- v2: `.zmetadata` file existence
- v3: `consolidated_metadata` field in `zarr.json`

#### 2. Read Raw Metadata Bytes

```python
metadata_bytes = await hl_store.get_group_metadata_bytes(
    "path/to/group",
    consolidated_key=".zmetadata"  # Custom v2 key (rare)
)
# Returns: {
#     "zarr_json_bytes": Buffer | None,
#     "zgroup_bytes": Buffer | None,
#     "zattrs_bytes": Buffer | None,
#     "consolidated_bytes": Buffer | None,
#     "detected_format": 2 | 3
# }
```

#### 3. Parse Group Metadata with Consolidation

```python
# Auto-detect and use consolidated metadata if available
group_metadata = await hl_store.open_group_metadata("path/to/group")

# Explicitly require consolidated metadata
group_metadata = await hl_store.open_group_metadata(
    "path/to/group",
    use_consolidated=True  # Raises if not found
)

# Ignore consolidated metadata even if present
group_metadata = await hl_store.open_group_metadata(
    "path/to/group",
    use_consolidated=False
)
```

### Integration with Group

When opening a group, the consolidated metadata behavior is:

```python
# AsyncGroup.open() uses HighLevelStore
group = await AsyncGroup.open(store, path="group1")

# If use_consolidated=None (default):
#   - Checks for consolidated metadata
#   - Uses it if found, otherwise uses regular metadata

# If use_consolidated=True:
#   - Requires consolidated metadata
#   - Raises ValueError if not found

# If use_consolidated=False:
#   - Ignores consolidated metadata
#   - Always reads metadata directly from store
```

### Consolidated Metadata in `_iter_members`

When iterating group members, consolidated metadata is respected:

```python
async for name, node in group.members():
    # If the group has consolidated metadata:
    #   - Lists children from consolidated metadata
    #   - Checks for mismatches with actual store
    #   - Emits warnings if store has extra children
    #   - Only yields children from consolidated metadata

    # If the group has no consolidated metadata:
    #   - Uses HighLevelStore.list_children_with_metadata()
    #   - Fetches metadata concurrently (respects async.concurrency)
    #   - Yields all children found in store
```

### V2 Consolidated Metadata Parsing

For Zarr v2, HighLevelStore parses the `.zmetadata` structure:

```python
# .zmetadata structure:
{
    "zarr_consolidated_format": 1,
    "metadata": {
        ".zgroup": {...},
        ".zattrs": {...},
        "child1/.zarray": {...},
        "child1/.zattrs": {...},
        "child2/.zgroup": {...},
        ...
    }
}

# HighLevelStore combines these into:
GroupMetadata(
    zarr_format=2,
    attributes={...},
    consolidated_metadata=ConsolidatedMetadata(
        metadata={
            "child1": ArrayV2Metadata(...),
            "child2": GroupMetadata(...),
        },
        kind="inline",
        must_understand=False
    )
)
```

### V3 Consolidated Metadata Parsing

For Zarr v3, consolidated metadata is embedded in `zarr.json`:

```python
# zarr.json structure:
{
    "zarr_format": 3,
    "node_type": "group",
    "attributes": {...},
    "consolidated_metadata": {
        "kind": "inline",
        "must_understand": false,
        "metadata": {
            "child1": {...},  # Full child metadata
            "child2": {...},
        }
    }
}
```

---

## Usage Examples

### Example 1: Working with Existing Store

```python
from zarr.storage import LocalStore, HighLevelStore

# Open existing store
store = await LocalStore.open("data.zarr")
hl_store = HighLevelStore(store)  # Auto-detects format

# All operations use detected format
metadata = await hl_store.get_metadata("array1")
print(f"Using Zarr v{hl_store.zarr_format}")

# Read chunk
chunk = await hl_store.get_chunk("array1", (0, 0))

# Check storage size
size = await hl_store.getsize_prefix("array1/")
print(f"Array storage: {size} bytes")
```

### Example 2: Creating New Store with Consolidated Metadata

```python
# Create new v3 store
store = await LocalStore.open("new_data.zarr", mode="w")
hl_store = HighLevelStore(store, zarr_format=3)

# Create hierarchy
await hl_store.set_metadata("group1", group_metadata)
await hl_store.set_metadata("group1/array1", array_metadata)
await hl_store.set_chunk("group1/array1", (0, 0), chunk_data)

# Later, open with consolidated metadata
group = await AsyncGroup.open(store, path="group1", use_consolidated=True)
```

### Example 3: Chunk Processing

```python
# Fetch metadata once for efficiency
metadata = await hl_store.get_array_metadata("array1")

# Process all chunks concurrently
async for coords in hl_store.list_chunks("array1", metadata=metadata):
    chunk = await hl_store.get_chunk("array1", coords, metadata=metadata)
    # Process chunk...
```

### Example 4: Hierarchy Traversal with Metadata

```python
# List children with pre-fetched metadata
async for child_name, node_info in hl_store.list_children_with_metadata("group1"):
    print(f"{child_name}: {node_info.node_type} (v{node_info.zarr_format})")

    # node_info contains:
    #   - path: full path to child
    #   - node_type: "array" or "group"
    #   - zarr_format: 2 or 3
    #   - metadata: parsed metadata object
```

### Example 5: Format Migration

```python
# Two stores, one per format
source = await LocalStore.open("data_v2.zarr")
dest = await LocalStore.open("data_v3.zarr", mode="w")

v2_hl = HighLevelStore(source, zarr_format=2)
v3_hl = HighLevelStore(dest, zarr_format=3)

# Migrate metadata
v2_metadata = await v2_hl.get_array_metadata("array1")
v3_metadata = migrate_metadata_to_v3(v2_metadata)
await v3_hl.set_metadata("array1", v3_metadata)

# Copy chunks (format-agnostic at chunk level)
async for coords in v2_hl.list_chunks("array1"):
    chunk = await v2_hl.get_chunk("array1", coords)
    if chunk:
        await v3_hl.set_chunk("array1", coords, chunk)
```

---

## Migration Guide

### For Users

**Before (direct store access):**
```python
# Had to understand format differences
if zarr_format == 2:
    zarray_bytes = await store.get(f"{path}/.zarray")
    zattrs_bytes = await store.get(f"{path}/.zattrs")
    # Parse JSON, combine...
else:
    zarr_json_bytes = await store.get(f"{path}/zarr.json")
    # Parse JSON...
```

**After (HighLevelStore):**
```python
# Format-agnostic
hl_store = HighLevelStore(store)
metadata = await hl_store.get_metadata(path)
```

### For Array/Group Developers

**Before:**
```python
# In Array class - format-specific chunk key encoding
if self.metadata.zarr_format == 2:
    key = f"{self.name}/{'.'.join(map(str, chunk_coords))}"
else:
    key = f"{self.name}/c/{'/'.join(map(str, chunk_coords))}"
chunk_bytes = await self.store.get(key)
```

**After:**
```python
# In Array class - format-agnostic
chunk_bytes = await self.hl_store.get_chunk(
    self.name, chunk_coords, metadata=self.metadata
)
```

### For Store Implementers

**No changes required!** HighLevelStore wraps any Store implementation. Just ensure your Store implements the standard Store protocol:
- `get()`, `set()`, `delete()`, `exists()`
- `list()`, `list_prefix()`, `list_dir()`
- `getsize()`, `getsize_prefix()`

### For Third-Party HighLevelStore Implementations

If you want to create a custom HighLevelStore (e.g., for a database backend with native metadata storage):

1. **Subclass HighLevelStore ABC**: `from zarr.abc.store import HighLevelStore`
2. **Implement all abstract methods**: Metadata, chunks, hierarchy operations
3. **Optional: Disable consolidated metadata support**: Override `supports_consolidated_metadata` property

```python
from zarr.abc.store import HighLevelStore

class DatabaseHighLevelStore(HighLevelStore):
    @property
    def supports_consolidated_metadata(self) -> bool:
        # Database already provides fast metadata access,
        # so consolidated metadata is not needed
        return False

    # Consolidated metadata methods can still be implemented,
    # but they won't be used when supports_consolidated_metadata=False
    async def has_consolidated_metadata(self, path: str, ...) -> bool:
        return False  # No consolidated metadata in database stores

    async def open_group_metadata(
        self,
        path: str,
        *,
        use_consolidated: bool | None = None,
        consolidated_key: str = ".zmetadata",
    ) -> GroupMetadata:
        # If user explicitly requests consolidated metadata, raise error
        if use_consolidated is True:
            raise ValueError("Consolidated metadata not supported by database store")

        # Otherwise, delegate to regular metadata method
        return await self.get_group_metadata(path)

    # Implement other required methods...
```

#### How the Capability Check Works

When opening a group, the code checks `supports_consolidated_metadata`:

```python
# In AsyncGroup.open()
hl_store = store_path.store

if not hl_store.supports_consolidated_metadata:
    # Store doesn't support consolidated metadata
    if use_consolidated:
        # User explicitly requested it - raise error
        raise ValueError(
            f"The Zarr store in use ({type(hl_store).__name__}) "
            f"doesn't support consolidated metadata."
        )
    # Auto-set to False if user didn't specify
    use_consolidated = False
```

This means:
- **`use_consolidated=True`**: Raises error if store doesn't support it
- **`use_consolidated=False`**: Works fine, ignores consolidated metadata
- **`use_consolidated=None`**: Auto-detects support and adjusts accordingly

---

## Design Rationale

### Why Format as Store Property?

Stores are **either** v2 **or** v3, not mixed. Making format a store property:
- Simplifies API (no format parameter on 20+ methods)
- Matches reality (Zarr hierarchies don't mix formats)
- Enables auto-detection with caching
- Prevents errors (can't accidentally use wrong format)

### Why Delegate Store Protocol?

HighLevelStore implements the Store protocol by delegating to the underlying store. This allows:
- Drop-in replacement: Use HighLevelStore anywhere Store is expected
- No breaking changes: Existing Store implementations work as-is
- Gradual migration: Code can use high-level or low-level APIs

### Why Separate Consolidated Metadata Methods?

Not all stores benefit from consolidated metadata:
- Database-backed stores have fast metadata queries
- Custom stores may have native metadata caching
- Third-party implementations can skip these methods

By making them optional (raise `NotImplementedError`), we allow flexibility while providing full support for standard stores.

---

## Future Enhancements

### Potential Additions

1. **Metadata Caching**: Cache frequently accessed metadata in HighLevelStore
2. **Batch Operations**: `set_chunks()` for multiple chunks in one call
3. **Partial Writes**: Efficient updates to large metadata structures
4. **Storage Analytics**: More detailed metrics (chunk size distribution, fragmentation)
5. **Format Validation**: Validate entire hierarchy structure

### Backwards Compatibility

All changes maintain backwards compatibility:
- Existing Store implementations work unchanged
- Array/Group APIs remain the same
- Internal refactoring is transparent to users

---

## Conclusion

HighLevelStore provides a clean, format-aware abstraction layer that:

1. **Simplifies higher layers**: Array/Group become format-agnostic
2. **Centralizes complexity**: v2/v3 differences in one place
3. **Improves maintainability**: Single source of truth for metadata operations
4. **Enables new features**: Chunk-level API, storage metrics, consolidated metadata
5. **Maintains flexibility**: Store backends unchanged, third-party implementations supported

**Result:** More maintainable, testable, and extensible codebase with ~390 lines of duplicate code eliminated.

---

**For more details, see:**
- Implementation: `src/zarr/storage/_high_level.py`
- ABC Definition: `src/zarr/abc/store.py`
- Tests: `tests/test_store/test_high_level.py`
- Usage in Array/Group: `src/zarr/core/array.py`, `src/zarr/core/group.py`
