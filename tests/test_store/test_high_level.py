"""
Tests for HighLevelStore - metadata-aware store operations.
"""

import pytest

from zarr.core.buffer import default_buffer_prototype
from zarr.core.common import ZarrFormat
from zarr.core.group import GroupMetadata
from zarr.core.metadata import ArrayV2Metadata, ArrayV3Metadata
from zarr.errors import (
    ContainsArrayError,
    ContainsGroupError,
    NodeTypeValidationError,
)
from zarr.storage import HighLevelStore, MemoryStore

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def memory_store() -> MemoryStore:
    """Create a fresh MemoryStore for each test."""
    return MemoryStore()


@pytest.fixture
async def v2_array_metadata() -> ArrayV2Metadata:
    """Create v2 array metadata for testing."""
    from zarr.core.dtype.npy.float import Float64

    return ArrayV2Metadata(
        shape=(10, 10),
        chunks=(5, 5),
        dtype=Float64(),
        fill_value=0.0,
        order="C",
        filters=None,
        compressor=None,
    )


@pytest.fixture
async def v3_array_metadata() -> ArrayV3Metadata:
    """Create v3 array metadata for testing."""
    from zarr.codecs.bytes import BytesCodec
    from zarr.core.chunk_grids import RegularChunkGrid
    from zarr.core.chunk_key_encodings import DefaultChunkKeyEncoding
    from zarr.core.dtype.npy.float import Float64

    return ArrayV3Metadata(
        shape=(10, 10),
        chunk_grid=RegularChunkGrid(chunk_shape=(5, 5)),
        chunk_key_encoding=DefaultChunkKeyEncoding(),
        data_type=Float64(),
        fill_value=0.0,
        codecs=(BytesCodec(),),
        attributes={},
        dimension_names=None,
    )


@pytest.fixture
async def v2_group_metadata() -> GroupMetadata:
    """Create v2 group metadata for testing."""
    return GroupMetadata(zarr_format=2, attributes={"description": "test group"})


@pytest.fixture
async def v3_group_metadata() -> GroupMetadata:
    """Create v3 group metadata for testing."""
    return GroupMetadata(zarr_format=3, attributes={"description": "test group"})


# ============================================================================
# Format Detection Tests
# ============================================================================


@pytest.mark.parametrize("zarr_format", [2, 3])
async def test_format_detection_explicit(
    memory_store: MemoryStore, zarr_format: ZarrFormat
) -> None:
    """Test explicit format specification."""
    hl_store = HighLevelStore(memory_store, zarr_format=zarr_format)

    # Format should be immediately available
    assert hl_store.zarr_format == zarr_format


async def test_format_detection_auto_v3(
    memory_store: MemoryStore, v3_array_metadata: ArrayV3Metadata
) -> None:
    """Test auto-detection of v3 format."""
    hl_store = HighLevelStore(memory_store, zarr_format=3)

    # Create v3 metadata
    await hl_store.set_metadata("array1", v3_array_metadata)

    # Create new store without format specified
    hl_store2 = HighLevelStore(memory_store)

    # Should auto-detect v3
    detected_format = await hl_store2.detect_format()
    assert detected_format == 3
    assert hl_store2.zarr_format == 3


async def test_format_detection_auto_v2(
    memory_store: MemoryStore, v2_array_metadata: ArrayV2Metadata
) -> None:
    """Test auto-detection of v2 format."""
    hl_store = HighLevelStore(memory_store, zarr_format=2)

    # Create v2 metadata
    await hl_store.set_metadata("array1", v2_array_metadata)

    # Create new store without format specified
    hl_store2 = HighLevelStore(memory_store)

    # Should auto-detect v2
    detected_format = await hl_store2.detect_format()
    assert detected_format == 2
    assert hl_store2.zarr_format == 2


async def test_format_property_error_before_detection(memory_store: MemoryStore) -> None:
    """Test that accessing zarr_format before detection raises error."""
    hl_store = HighLevelStore(memory_store)

    with pytest.raises(RuntimeError, match="Format not yet determined"):
        _ = hl_store.zarr_format


async def test_format_detection_not_found(memory_store: MemoryStore) -> None:
    """Test format detection on empty store."""
    hl_store = HighLevelStore(memory_store)

    with pytest.raises(FileNotFoundError, match="No Zarr metadata found"):
        await hl_store.detect_format()


# ============================================================================
# Metadata Operations Tests
# ============================================================================


@pytest.mark.parametrize("zarr_format", [2, 3])
async def test_set_and_get_array_metadata(
    memory_store: MemoryStore,
    zarr_format: ZarrFormat,
    v2_array_metadata: ArrayV2Metadata,
    v3_array_metadata: ArrayV3Metadata,
) -> None:
    """Test setting and getting array metadata."""
    hl_store = HighLevelStore(memory_store, zarr_format=zarr_format)
    metadata = v2_array_metadata if zarr_format == 2 else v3_array_metadata

    # Set metadata
    await hl_store.set_metadata("array1", metadata)

    # Get metadata
    retrieved = await hl_store.get_metadata("array1")

    assert retrieved.shape == (10, 10)
    assert retrieved.chunks == (5, 5)
    assert retrieved.zarr_format == zarr_format


@pytest.mark.parametrize("zarr_format", [2, 3])
async def test_set_and_get_group_metadata(
    memory_store: MemoryStore,
    zarr_format: ZarrFormat,
    v2_group_metadata: GroupMetadata,
    v3_group_metadata: GroupMetadata,
) -> None:
    """Test setting and getting group metadata."""
    hl_store = HighLevelStore(memory_store, zarr_format=zarr_format)
    metadata = v2_group_metadata if zarr_format == 2 else v3_group_metadata

    # Set metadata
    await hl_store.set_metadata("group1", metadata)

    # Get metadata
    retrieved = await hl_store.get_metadata("group1")

    assert isinstance(retrieved, GroupMetadata)
    assert retrieved.zarr_format == zarr_format
    assert retrieved.attributes["description"] == "test group"


async def test_get_array_metadata_with_validation(
    memory_store: MemoryStore, v3_array_metadata: ArrayV3Metadata
) -> None:
    """Test get_array_metadata with type validation."""
    hl_store = HighLevelStore(memory_store, zarr_format=3)

    await hl_store.set_metadata("array1", v3_array_metadata)

    # Should succeed for array
    metadata = await hl_store.get_array_metadata("array1")
    assert isinstance(metadata, ArrayV3Metadata)


async def test_get_array_metadata_validation_error(
    memory_store: MemoryStore, v3_group_metadata: GroupMetadata
) -> None:
    """Test that get_array_metadata raises error for groups."""
    hl_store = HighLevelStore(memory_store, zarr_format=3)

    await hl_store.set_metadata("group1", v3_group_metadata)

    # Should fail for group
    with pytest.raises(NodeTypeValidationError, match="Expected array metadata"):
        await hl_store.get_array_metadata("group1")


async def test_get_group_metadata_with_validation(
    memory_store: MemoryStore, v3_group_metadata: GroupMetadata
) -> None:
    """Test get_group_metadata with type validation."""
    hl_store = HighLevelStore(memory_store, zarr_format=3)

    await hl_store.set_metadata("group1", v3_group_metadata)

    # Should succeed for group
    metadata = await hl_store.get_group_metadata("group1")
    assert isinstance(metadata, GroupMetadata)


async def test_get_group_metadata_validation_error(
    memory_store: MemoryStore, v3_array_metadata: ArrayV3Metadata
) -> None:
    """Test that get_group_metadata raises error for arrays."""
    hl_store = HighLevelStore(memory_store, zarr_format=3)

    await hl_store.set_metadata("array1", v3_array_metadata)

    # Should fail for array
    with pytest.raises(NodeTypeValidationError, match="Expected group metadata"):
        await hl_store.get_group_metadata("array1")


async def test_set_metadata_with_ensure_parents(
    memory_store: MemoryStore, v3_array_metadata: ArrayV3Metadata
) -> None:
    """Test setting metadata with parent creation."""
    hl_store = HighLevelStore(memory_store, zarr_format=3)

    # Set metadata with nested path
    await hl_store.set_metadata("a/b/c/array1", v3_array_metadata, ensure_parents=True)

    # Parents should exist
    assert await hl_store.contains_group("a")
    assert await hl_store.contains_group("a/b")
    assert await hl_store.contains_group("a/b/c")
    assert await hl_store.contains_array("a/b/c/array1")


# ============================================================================
# Node Detection Tests
# ============================================================================


@pytest.mark.parametrize("zarr_format", [2, 3])
async def test_contains_array(
    memory_store: MemoryStore,
    zarr_format: ZarrFormat,
    v2_array_metadata: ArrayV2Metadata,
    v3_array_metadata: ArrayV3Metadata,
) -> None:
    """Test contains_array detection."""
    hl_store = HighLevelStore(memory_store, zarr_format=zarr_format)
    metadata = v2_array_metadata if zarr_format == 2 else v3_array_metadata

    # Should not exist initially
    assert not await hl_store.contains_array("array1")

    # Create array
    await hl_store.set_metadata("array1", metadata)

    # Should exist now
    assert await hl_store.contains_array("array1")


@pytest.mark.parametrize("zarr_format", [2, 3])
async def test_contains_group(
    memory_store: MemoryStore,
    zarr_format: ZarrFormat,
    v2_group_metadata: GroupMetadata,
    v3_group_metadata: GroupMetadata,
) -> None:
    """Test contains_group detection."""
    hl_store = HighLevelStore(memory_store, zarr_format=zarr_format)
    metadata = v2_group_metadata if zarr_format == 2 else v3_group_metadata

    # Should not exist initially
    assert not await hl_store.contains_group("group1")

    # Create group
    await hl_store.set_metadata("group1", metadata)

    # Should exist now
    assert await hl_store.contains_group("group1")


@pytest.mark.parametrize("zarr_format", [2, 3])
async def test_get_node_type(
    memory_store: MemoryStore,
    zarr_format: ZarrFormat,
    v2_array_metadata: ArrayV2Metadata,
    v3_array_metadata: ArrayV3Metadata,
    v2_group_metadata: GroupMetadata,
    v3_group_metadata: GroupMetadata,
) -> None:
    """Test get_node_type detection."""
    hl_store = HighLevelStore(memory_store, zarr_format=zarr_format)
    array_meta = v2_array_metadata if zarr_format == 2 else v3_array_metadata
    group_meta = v2_group_metadata if zarr_format == 2 else v3_group_metadata

    # Nothing exists initially
    assert await hl_store.get_node_type("array1") == "nothing"

    # Create array
    await hl_store.set_metadata("array1", array_meta)
    assert await hl_store.get_node_type("array1") == "array"

    # Create group
    await hl_store.set_metadata("group1", group_meta)
    assert await hl_store.get_node_type("group1") == "group"


async def test_ensure_no_existing_node_success(memory_store: MemoryStore) -> None:
    """Test ensure_no_existing_node when path is clear."""
    hl_store = HighLevelStore(memory_store, zarr_format=3)

    # Should not raise for non-existent path
    await hl_store.ensure_no_existing_node("array1")


async def test_ensure_no_existing_node_array_exists(
    memory_store: MemoryStore, v3_array_metadata: ArrayV3Metadata
) -> None:
    """Test ensure_no_existing_node when array exists."""
    hl_store = HighLevelStore(memory_store, zarr_format=3)

    await hl_store.set_metadata("array1", v3_array_metadata)

    # Should raise for existing array
    with pytest.raises(ContainsArrayError, match="array already exists"):
        await hl_store.ensure_no_existing_node("array1")


async def test_ensure_no_existing_node_group_exists(
    memory_store: MemoryStore, v3_group_metadata: GroupMetadata
) -> None:
    """Test ensure_no_existing_node when group exists."""
    hl_store = HighLevelStore(memory_store, zarr_format=3)

    await hl_store.set_metadata("group1", v3_group_metadata)

    # Should raise for existing group
    with pytest.raises(ContainsGroupError, match="group already exists"):
        await hl_store.ensure_no_existing_node("group1")


async def test_ensure_no_existing_node_with_node_type(
    memory_store: MemoryStore,
    v3_array_metadata: ArrayV3Metadata,
    v3_group_metadata: GroupMetadata,
) -> None:
    """Test ensure_no_existing_node with node_type parameter."""
    hl_store = HighLevelStore(memory_store, zarr_format=3)

    await hl_store.set_metadata("array1", v3_array_metadata)
    await hl_store.set_metadata("group1", v3_group_metadata)

    # Should not raise when node_type allows it
    await hl_store.ensure_no_existing_node("array1", node_type="group")
    await hl_store.ensure_no_existing_node("group1", node_type="array")


# ============================================================================
# Deletion Tests
# ============================================================================


@pytest.mark.parametrize("zarr_format", [2, 3])
async def test_delete_array(
    memory_store: MemoryStore,
    zarr_format: ZarrFormat,
    v2_array_metadata: ArrayV2Metadata,
    v3_array_metadata: ArrayV3Metadata,
) -> None:
    """Test deleting an array."""
    hl_store = HighLevelStore(memory_store, zarr_format=zarr_format)
    metadata = v2_array_metadata if zarr_format == 2 else v3_array_metadata

    # Create array
    await hl_store.set_metadata("array1", metadata)
    assert await hl_store.contains_array("array1")

    # Delete array
    await hl_store.delete_array("array1")

    # Should not exist anymore
    assert not await hl_store.contains_array("array1")


@pytest.mark.parametrize("zarr_format", [2, 3])
async def test_delete_group(
    memory_store: MemoryStore,
    zarr_format: ZarrFormat,
    v2_group_metadata: GroupMetadata,
    v3_group_metadata: GroupMetadata,
) -> None:
    """Test deleting a group."""
    hl_store = HighLevelStore(memory_store, zarr_format=zarr_format)
    metadata = v2_group_metadata if zarr_format == 2 else v3_group_metadata

    # Create group
    await hl_store.set_metadata("group1", metadata)
    assert await hl_store.contains_group("group1")

    # Delete group
    await hl_store.delete_group("group1")

    # Should not exist anymore
    assert not await hl_store.contains_group("group1")


async def test_delete_group_recursive(
    memory_store: MemoryStore,
    v3_group_metadata: GroupMetadata,
    v3_array_metadata: ArrayV3Metadata,
) -> None:
    """Test deleting a group with children recursively."""
    hl_store = HighLevelStore(memory_store, zarr_format=3)

    # Create hierarchy
    await hl_store.set_metadata("group1", v3_group_metadata)
    await hl_store.set_metadata("group1/array1", v3_array_metadata)
    await hl_store.set_metadata("group1/subgroup", v3_group_metadata)

    # Delete recursively
    await hl_store.delete_group("group1", recursive=True)

    # Nothing should exist
    assert not await hl_store.contains_group("group1")
    assert not await hl_store.contains_array("group1/array1")
    assert not await hl_store.contains_group("group1/subgroup")


async def test_delete_group_non_recursive_error(
    memory_store: MemoryStore,
    v3_group_metadata: GroupMetadata,
    v3_array_metadata: ArrayV3Metadata,
) -> None:
    """Test that deleting non-empty group without recursive raises error."""
    hl_store = HighLevelStore(memory_store, zarr_format=3)

    # Create hierarchy
    await hl_store.set_metadata("group1", v3_group_metadata)
    await hl_store.set_metadata("group1/array1", v3_array_metadata)

    # Should raise error
    with pytest.raises(ValueError, match="not empty"):
        await hl_store.delete_group("group1", recursive=False)


async def test_delete_node(
    memory_store: MemoryStore,
    v3_array_metadata: ArrayV3Metadata,
    v3_group_metadata: GroupMetadata,
) -> None:
    """Test delete_node works for both arrays and groups."""
    hl_store = HighLevelStore(memory_store, zarr_format=3)

    # Create array and group
    await hl_store.set_metadata("array1", v3_array_metadata)
    await hl_store.set_metadata("group1", v3_group_metadata)

    # Delete both using delete_node
    await hl_store.delete_node("array1")
    await hl_store.delete_node("group1")

    # Neither should exist
    assert not await hl_store.contains_array("array1")
    assert not await hl_store.contains_group("group1")


# ============================================================================
# Chunk Operations Tests
# ============================================================================


@pytest.mark.parametrize("zarr_format", [2, 3])
async def test_chunk_operations(
    memory_store: MemoryStore,
    zarr_format: ZarrFormat,
    v2_array_metadata: ArrayV2Metadata,
    v3_array_metadata: ArrayV3Metadata,
) -> None:
    """Test chunk get/set/exists/delete operations."""
    hl_store = HighLevelStore(memory_store, zarr_format=zarr_format)
    metadata = v2_array_metadata if zarr_format == 2 else v3_array_metadata

    # Create array
    await hl_store.set_metadata("array1", metadata)

    # Chunk should not exist initially
    assert not await hl_store.exists_chunk("array1", (0, 0))

    # Set chunk
    prototype = default_buffer_prototype()
    chunk_data = prototype.buffer.from_bytes(b"test chunk data")
    await hl_store.set_chunk("array1", (0, 0), chunk_data)

    # Chunk should exist now
    assert await hl_store.exists_chunk("array1", (0, 0))

    # Get chunk
    retrieved = await hl_store.get_chunk("array1", (0, 0))
    assert retrieved is not None
    assert retrieved.to_bytes() == b"test chunk data"

    # Delete chunk
    await hl_store.delete_chunk("array1", (0, 0))

    # Should not exist anymore
    assert not await hl_store.exists_chunk("array1", (0, 0))


@pytest.mark.parametrize("zarr_format", [2, 3])
async def test_list_chunks(
    memory_store: MemoryStore,
    zarr_format: ZarrFormat,
    v2_array_metadata: ArrayV2Metadata,
    v3_array_metadata: ArrayV3Metadata,
) -> None:
    """Test listing chunks."""
    hl_store = HighLevelStore(memory_store, zarr_format=zarr_format)
    metadata = v2_array_metadata if zarr_format == 2 else v3_array_metadata

    # Create array
    await hl_store.set_metadata("array1", metadata)

    # Set multiple chunks
    prototype = default_buffer_prototype()
    chunk_data = prototype.buffer.from_bytes(b"test")
    coords_list = [(0, 0), (0, 1), (1, 0), (1, 1)]

    for coords in coords_list:
        await hl_store.set_chunk("array1", coords, chunk_data)

    # List chunks
    found_coords = []
    async for coords in hl_store.list_chunks("array1"):
        found_coords.append(coords)

    # Should find all chunks
    assert set(found_coords) == set(coords_list)


# ============================================================================
# Storage Metrics Tests
# ============================================================================


async def test_get_size(memory_store: MemoryStore, v3_array_metadata: ArrayV3Metadata) -> None:
    """Test get_size calculation."""
    hl_store = HighLevelStore(memory_store, zarr_format=3)

    # Create array
    await hl_store.set_metadata("array1", v3_array_metadata)

    # Get size (metadata only)
    metadata_size = await hl_store.get_size("array1", include_chunks=False)
    assert metadata_size > 0

    # Add a chunk
    prototype = default_buffer_prototype()
    chunk_data = prototype.buffer.from_bytes(b"test chunk data")
    await hl_store.set_chunk("array1", (0, 0), chunk_data)

    # Total size should be larger
    total_size = await hl_store.get_size("array1")
    assert total_size > metadata_size


async def test_get_array_storage_info(
    memory_store: MemoryStore, v3_array_metadata: ArrayV3Metadata
) -> None:
    """Test get_array_storage_info."""
    hl_store = HighLevelStore(memory_store, zarr_format=3)

    # Create array with some chunks
    await hl_store.set_metadata("array1", v3_array_metadata)

    prototype = default_buffer_prototype()
    chunk_data = prototype.buffer.from_bytes(b"test" * 100)

    # Set 2 out of 4 expected chunks
    await hl_store.set_chunk("array1", (0, 0), chunk_data)
    await hl_store.set_chunk("array1", (0, 1), chunk_data)

    # Get storage info
    info = await hl_store.get_array_storage_info("array1")

    assert info["chunk_count"] == 2
    assert info["expected_chunks"] == 4  # (10/5) * (10/5) = 2*2 = 4
    assert info["missing_chunks"] == 2
    assert info["storage_ratio"] == 0.5
    assert info["total_size"] > 0


# ============================================================================
# Hierarchy Operations Tests
# ============================================================================


async def test_list_children(
    memory_store: MemoryStore,
    v3_group_metadata: GroupMetadata,
    v3_array_metadata: ArrayV3Metadata,
) -> None:
    """Test listing children of a group."""
    hl_store = HighLevelStore(memory_store, zarr_format=3)

    # Create hierarchy
    await hl_store.set_metadata("group1", v3_group_metadata)
    await hl_store.set_metadata("group1/array1", v3_array_metadata)
    await hl_store.set_metadata("group1/array2", v3_array_metadata)
    await hl_store.set_metadata("group1/subgroup", v3_group_metadata)

    # List children
    children = []
    async for child in hl_store.list_children("group1"):
        children.append(child)

    assert set(children) == {"array1", "array2", "subgroup"}


async def test_list_children_with_metadata(
    memory_store: MemoryStore,
    v3_group_metadata: GroupMetadata,
    v3_array_metadata: ArrayV3Metadata,
) -> None:
    """Test listing children with metadata."""
    hl_store = HighLevelStore(memory_store, zarr_format=3)

    # Create hierarchy
    await hl_store.set_metadata("group1", v3_group_metadata)
    await hl_store.set_metadata("group1/array1", v3_array_metadata)
    await hl_store.set_metadata("group1/subgroup", v3_group_metadata)

    # List children with metadata
    children_info = {}
    async for name, info in hl_store.list_children_with_metadata("group1"):
        children_info[name] = info

    assert "array1" in children_info
    assert "subgroup" in children_info
    assert children_info["array1"].node_type == "array"
    assert children_info["subgroup"].node_type == "group"


# ============================================================================
# Properties Tests
# ============================================================================


async def test_store_property(memory_store: MemoryStore) -> None:
    """Test accessing the underlying store."""
    hl_store = HighLevelStore(memory_store, zarr_format=3)

    assert hl_store.store is memory_store


async def test_read_only_property(memory_store: MemoryStore) -> None:
    """Test read_only property."""
    hl_store = HighLevelStore(memory_store, zarr_format=3)

    # MemoryStore with mode='w' should not be read-only
    assert not hl_store.read_only


async def test_read_only_store_prevents_writes(
    memory_store: MemoryStore, v3_array_metadata: ArrayV3Metadata
) -> None:
    """Test that read-only store prevents write operations."""
    # Create data first
    hl_store = HighLevelStore(memory_store, zarr_format=3)
    await hl_store.set_metadata("array1", v3_array_metadata)

    # Create read-only store
    readonly_store = MemoryStore(store_dict=memory_store._store_dict, read_only=True)
    hl_store_ro = HighLevelStore(readonly_store, zarr_format=3)

    assert hl_store_ro.read_only

    # Should raise PermissionError for write operations
    with pytest.raises(PermissionError, match="read-only"):
        await hl_store_ro.set_metadata("array2", v3_array_metadata)


# ============================================================================
# Integration Tests
# ============================================================================


async def test_end_to_end_workflow(
    memory_store: MemoryStore,
    v3_group_metadata: GroupMetadata,
    v3_array_metadata: ArrayV3Metadata,
) -> None:
    """Test a complete workflow with HighLevelStore."""
    hl_store = HighLevelStore(memory_store, zarr_format=3)

    # Create hierarchy
    await hl_store.set_metadata("root", v3_group_metadata)
    await hl_store.set_metadata("root/data", v3_group_metadata)
    await hl_store.set_metadata("root/data/array1", v3_array_metadata, ensure_parents=False)

    # Add chunks
    prototype = default_buffer_prototype()
    chunk_data = prototype.buffer.from_bytes(b"test data")
    await hl_store.set_chunk("root/data/array1", (0, 0), chunk_data)
    await hl_store.set_chunk("root/data/array1", (0, 1), chunk_data)

    # Verify structure
    assert await hl_store.contains_group("root")
    assert await hl_store.contains_group("root/data")
    assert await hl_store.contains_array("root/data/array1")

    # Verify chunks
    assert await hl_store.exists_chunk("root/data/array1", (0, 0))
    assert await hl_store.exists_chunk("root/data/array1", (0, 1))

    # Get storage info
    info = await hl_store.get_array_storage_info("root/data/array1")
    assert info["chunk_count"] == 2

    # List children
    children = []
    async for child in hl_store.list_children("root"):
        children.append(child)
    assert "data" in children

    # Clean up
    await hl_store.delete_group("root", recursive=True)
    assert not await hl_store.contains_group("root")
