"""
Tests for `zarr.create_v3_array` and `zarr.create_v3_group`: URL-pipeline-native
creation routines whose keyword arguments are the Zarr V3 metadata fields.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import numpy as np
import pytest

import zarr
import zarr.api.asynchronous
from zarr import AUTO, Array, Group, create_v3_array, create_v3_group
from zarr.codecs import BytesCodec, GzipCodec, ZstdCodec
from zarr.core.metadata.v3 import ArrayV3Metadata, RegularChunkGridMetadata
from zarr.errors import ContainsArrayError, ContainsGroupError, URLPipelineError
from zarr.storage import MemoryStore, StorePath, URLPipeline

if TYPE_CHECKING:
    from pathlib import Path

LOCATION_KINDS = ("url", "pipeline", "store", "store_path")


def _location(kind: str, tmp_path: Path, node: str) -> Any:
    """Spell the same node four ways: a `file:` URL string, a `URLPipeline`, a `Store`, a `StorePath`."""
    url = f"file:{tmp_path.as_posix()}|zarr3:{node}"
    if kind == "url":
        return url
    if kind == "pipeline":
        return URLPipeline.from_url(url)
    store = zarr.storage.LocalStore(tmp_path)
    if kind == "store":
        # a bare Store addresses its root; put the node at the root instead
        return zarr.storage.LocalStore(tmp_path / node)
    return StorePath(store, node)


def _reopen_location(kind: str, tmp_path: Path, node: str) -> Any:
    """The same location for the typed open functions, which never read a string as a pipeline."""
    location = _location(kind, tmp_path, node)
    return URLPipeline.from_url(location) if isinstance(location, str) else location


class TestCreateV3Array:
    @pytest.mark.parametrize("kind", LOCATION_KINDS)
    @pytest.mark.parametrize(
        "chunk_grid",
        [(5, 4), {"name": "regular", "configuration": {"chunk_shape": [5, 4]}}],
        ids=["shape-shorthand", "metadata-dict"],
    )
    def test_defaults(self, kind: str, chunk_grid: Any, tmp_path: Path) -> None:
        """
        Every location kind yields a V3 array whose explicit fields are stored and whose
        unspecified fields get the computed defaults: the config codec pipeline for the data
        type, the `/`-separated default chunk key encoding, and the data type's fill value.
        """
        arr = create_v3_array(
            _location(kind, tmp_path, "arr"),
            shape=(10, 8),
            data_type="int32",
            chunk_grid=chunk_grid,
        )
        assert isinstance(arr, Array)
        assert arr.metadata.zarr_format == 3
        assert arr.shape == (10, 8)
        assert arr.chunks == (5, 4)
        assert arr.dtype == np.dtype("int32")
        assert arr.metadata.codecs == (BytesCodec(endian="little"), ZstdCodec())
        assert arr.metadata.chunk_key_encoding.to_dict() == {
            "name": "default",
            "configuration": {"separator": "/"},
        }
        assert arr.metadata.fill_value == 0
        assert arr.metadata.attributes == {}
        assert arr.metadata.dimension_names is None
        # the array is really there: reopen it through the typed open function
        reopened = zarr.open_array(_reopen_location(kind, tmp_path, "arr"), mode="r")
        assert reopened.metadata == arr.metadata

    def test_explicit_metadata_fields(self, tmp_path: Path) -> None:
        """Every metadata field passed explicitly is stored verbatim; `AUTO` passed explicitly is the default."""
        arr = create_v3_array(
            f"file:{tmp_path.as_posix()}|zarr3:a",
            shape=(6,),
            data_type=np.dtype("float64"),
            chunk_grid=(3,),
            codecs=[BytesCodec(endian="big"), GzipCodec(level=3)],
            chunk_key_encoding={"name": "v2", "separator": "."},
            fill_value=AUTO,
            attributes={"units": "m"},
            dimension_names=("x",),
        )
        assert isinstance(arr.metadata, ArrayV3Metadata)
        assert arr.metadata.codecs == (BytesCodec(endian="big"), GzipCodec(level=3))
        assert arr.metadata.chunk_key_encoding.to_dict() == {
            "name": "v2",
            "configuration": {"separator": "."},
        }
        assert arr.metadata.fill_value == 0.0  # the data type's default scalar
        assert arr.attrs.asdict() == {"units": "m"}
        assert arr.metadata.dimension_names == ("x",)
        arr2 = create_v3_array(
            f"file:{tmp_path.as_posix()}|zarr3:b", shape=(6,), data_type="f8", fill_value=1.5
        )
        assert arr2.metadata.fill_value == 1.5

    def test_auto_chunk_grid(self, tmp_path: Path) -> None:
        """Without a chunk grid, chunks are guessed from the shape and data type."""
        arr = create_v3_array(
            f"file:{tmp_path.as_posix()}|zarr3:", shape=(1000, 1000), data_type="u1"
        )
        assert isinstance(arr.metadata, ArrayV3Metadata)
        assert isinstance(arr.metadata.chunk_grid, RegularChunkGridMetadata)
        assert all(0 < c <= 1000 for c in arr.chunks)

    def test_format_segment_body_is_the_node_path(self, tmp_path: Path) -> None:
        """The node path comes from the URL, and missing parent groups are created."""
        arr = create_v3_array(
            f"file:{tmp_path.as_posix()}|zarr3:grp/sub/arr", shape=(2,), data_type="i4"
        )
        assert arr.path == "grp/sub/arr"
        assert (tmp_path / "grp" / "sub" / "arr" / "zarr.json").exists()
        assert isinstance(zarr.open_url(f"file:{tmp_path.as_posix()}|zarr3:grp"), Group)

    def test_percent_escaped_pipe_in_file_root(self, tmp_path: Path) -> None:
        """`%7C` is the spelling of a literal `|` in a local path given as a `file:` URL."""
        create_v3_array(f"file:{tmp_path.as_posix()}/a%7Cb|zarr3:", shape=(2,), data_type="i4")
        assert (tmp_path / "a|b" / "zarr.json").exists()

    def test_existing_node_raises_unless_overwrite(self, tmp_path: Path) -> None:
        """Creation fails on an existing array or group; `overwrite=True` replaces it."""
        url = f"file:{tmp_path.as_posix()}|zarr3:node"
        create_v3_array(url, shape=(2,), data_type="i4")
        with pytest.raises(ContainsArrayError):
            create_v3_array(url, shape=(3,), data_type="i4")
        replaced = create_v3_array(url, shape=(3,), data_type="i4", overwrite=True)
        assert replaced.shape == (3,)
        create_v3_group(f"file:{tmp_path.as_posix()}|zarr3:g")
        with pytest.raises(ContainsGroupError):
            create_v3_array(f"file:{tmp_path.as_posix()}|zarr3:g", shape=(2,), data_type="i4")

    def test_zarr2_segment_raises(self, tmp_path: Path) -> None:
        """A `zarr2:` format segment contradicts the function."""
        with pytest.raises(ValueError, match="conflicts with the 'zarr2:' segment"):
            create_v3_array(f"file:{tmp_path.as_posix()}|zarr2:", shape=(2,), data_type="i4")

    def test_schemeless_root_raises(self, tmp_path: Path) -> None:
        """A string location is a URL pipeline, so its root needs a scheme; bare paths are rejected."""
        with pytest.raises(URLPipelineError, match="URL scheme"):
            create_v3_array(f"{tmp_path}/arr", shape=(2,), data_type="i4")

    def test_read_only_location_raises(self) -> None:
        """A location that cannot be written to is rejected before anything is attempted."""
        with pytest.raises(ValueError, match="read-only"):
            create_v3_array(MemoryStore(read_only=True), shape=(2,), data_type="i4")

    async def test_async(self, tmp_path: Path) -> None:
        arr = await zarr.api.asynchronous.create_v3_array(
            f"file:{tmp_path.as_posix()}|zarr3:", shape=(4,), data_type="i2", chunk_grid=(2,)
        )
        assert isinstance(arr, zarr.AsyncArray)
        assert arr.metadata.zarr_format == 3
        assert arr.chunks == (2,)


class TestCreateV3Group:
    @pytest.mark.parametrize("kind", LOCATION_KINDS)
    def test_locations(self, kind: str, tmp_path: Path) -> None:
        """Every location kind yields an empty V3 group that can be reopened."""
        group = create_v3_group(_location(kind, tmp_path, "grp"))
        assert isinstance(group, Group)
        assert group.metadata.zarr_format == 3
        assert group.attrs.asdict() == {}
        reopened = zarr.open_group(_reopen_location(kind, tmp_path, "grp"), mode="r")
        assert reopened.metadata == group.metadata

    def test_attributes(self, tmp_path: Path) -> None:
        group = create_v3_group(f"file:{tmp_path.as_posix()}|zarr3:", attributes={"k": [1, 2]})
        assert group.attrs.asdict() == {"k": [1, 2]}

    def test_existing_node_raises_unless_overwrite(self, tmp_path: Path) -> None:
        """Creation fails on an existing group or array; `overwrite=True` replaces it."""
        url = f"file:{tmp_path.as_posix()}|zarr3:node"
        create_v3_group(url, attributes={"a": 1})
        with pytest.raises(ContainsGroupError):
            create_v3_group(url)
        replaced = create_v3_group(url, overwrite=True)
        assert replaced.attrs.asdict() == {}
        create_v3_array(f"file:{tmp_path.as_posix()}|zarr3:arr", shape=(2,), data_type="i4")
        with pytest.raises(ContainsArrayError):
            create_v3_group(f"file:{tmp_path.as_posix()}|zarr3:arr")

    def test_zarr2_segment_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="conflicts with the 'zarr2:' segment"):
            create_v3_group(f"file:{tmp_path.as_posix()}|zarr2:")

    def test_schemeless_root_raises(self, tmp_path: Path) -> None:
        with pytest.raises(URLPipelineError, match="URL scheme"):
            create_v3_group(f"{tmp_path}/grp")

    def test_read_only_location_raises(self) -> None:
        with pytest.raises(ValueError, match="read-only"):
            create_v3_group(MemoryStore(read_only=True))

    async def test_async(self, tmp_path: Path) -> None:
        group = await zarr.api.asynchronous.create_v3_group(f"file:{tmp_path.as_posix()}|zarr3:")
        assert isinstance(group, zarr.AsyncGroup)
        assert group.metadata.zarr_format == 3
