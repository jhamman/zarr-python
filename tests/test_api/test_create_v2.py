"""
Tests for `zarr.create_v2_array` and `zarr.create_v2_group`: URL-pipeline-native
creation routines whose keyword arguments are the Zarr V2 metadata fields.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import numpy as np
import pytest
from numcodecs import Delta, VLenUTF8, Zstd

import zarr
import zarr.api.asynchronous
from zarr import Array, Group, create_v2_array, create_v2_group
from zarr.core.metadata.v2 import ArrayV2Metadata
from zarr.errors import ContainsArrayError, ContainsGroupError, URLPipelineError
from zarr.storage import StorePath, URLPipeline

if TYPE_CHECKING:
    from pathlib import Path

LOCATION_KINDS = ("url", "pipeline", "store", "store_path")


def _location(kind: str, tmp_path: Path, node: str) -> Any:
    """Spell the same node four ways: a `file:` URL string, a `URLPipeline`, a `Store`, a `StorePath`."""
    url = f"file:{tmp_path.as_posix()}|zarr2:{node}"
    if kind == "url":
        return url
    if kind == "pipeline":
        return URLPipeline.from_url(url)
    if kind == "store":
        # a bare Store addresses its root; put the node at the root instead
        return zarr.storage.LocalStore(tmp_path / node)
    return StorePath(zarr.storage.LocalStore(tmp_path), node)


def _reopen_location(kind: str, tmp_path: Path, node: str) -> Any:
    """The same location for the typed open functions, which never read a string as a pipeline."""
    location = _location(kind, tmp_path, node)
    return URLPipeline.from_url(location) if isinstance(location, str) else location


class TestCreateV2Array:
    @pytest.mark.parametrize("kind", LOCATION_KINDS)
    def test_defaults(self, kind: str, tmp_path: Path) -> None:
        """
        Every location kind yields a V2 array whose explicit fields are stored and whose
        unspecified fields get the computed defaults: the default compressor, no filters,
        the data type's fill value, the configured order and `.` as the separator.
        """
        arr = create_v2_array(
            _location(kind, tmp_path, "arr"), shape=(10, 8), dtype="int32", chunks=(5, 4)
        )
        assert isinstance(arr, Array)
        assert isinstance(arr.metadata, ArrayV2Metadata)
        assert arr.metadata.zarr_format == 2
        assert arr.shape == (10, 8)
        assert arr.chunks == (5, 4)
        assert arr.dtype == np.dtype("int32")
        assert arr.metadata.compressor == Zstd(level=0, checksum=False)
        assert arr.metadata.filters is None
        assert arr.metadata.fill_value == 0
        assert arr.metadata.order == "C"
        assert arr.metadata.dimension_separator == "."
        assert arr.metadata.attributes == {}
        reopened = zarr.open_array(_reopen_location(kind, tmp_path, "arr"), mode="r")
        assert reopened.metadata == arr.metadata

    def test_explicit_metadata_fields(self, tmp_path: Path) -> None:
        """Every field passed explicitly is stored verbatim, including `None` where V2 allows it."""
        arr = create_v2_array(
            f"file:{tmp_path.as_posix()}|zarr2:a",
            shape=(6,),
            dtype=np.dtype("int32"),
            chunks=(3,),
            fill_value=None,
            order="F",
            dimension_separator="/",
            compressor=None,
            filters=[Delta(dtype="int32")],
            attributes={"units": "m"},
        )
        assert isinstance(arr.metadata, ArrayV2Metadata)
        assert arr.metadata.fill_value is None
        assert arr.metadata.order == "F"
        assert arr.metadata.dimension_separator == "/"
        assert arr.metadata.compressor is None
        assert arr.metadata.filters == (Delta(dtype="int32"),)
        assert arr.attrs.asdict() == {"units": "m"}
        arr2 = create_v2_array(
            f"file:{tmp_path.as_posix()}|zarr2:b", shape=(6,), dtype="f8", fill_value=1.5
        )
        assert arr2.metadata.fill_value == 1.5

    def test_default_filters_follow_the_data_type(self, tmp_path: Path) -> None:
        """A variable-length string data type gets its object codec as the default filter."""
        arr = create_v2_array(f"file:{tmp_path.as_posix()}|zarr2:", shape=(4,), dtype=str)
        assert isinstance(arr.metadata, ArrayV2Metadata)
        assert arr.metadata.filters == (VLenUTF8(),)

    def test_order_follows_config(self, tmp_path: Path) -> None:
        """Without an explicit order, the configured `array.order` is used."""
        with zarr.config.set({"array.order": "F"}):
            arr = create_v2_array(f"file:{tmp_path.as_posix()}|zarr2:", shape=(4,), dtype="i4")
        assert isinstance(arr.metadata, ArrayV2Metadata)
        assert arr.metadata.order == "F"

    def test_auto_chunks(self, tmp_path: Path) -> None:
        """Without chunks, a chunk shape is guessed from the shape and data type."""
        arr = create_v2_array(f"file:{tmp_path.as_posix()}|zarr2:", shape=(1000, 1000), dtype="u1")
        assert all(0 < c <= 1000 for c in arr.chunks)

    def test_rectilinear_chunks_raise(self, tmp_path: Path) -> None:
        """Zarr format 2 has no rectilinear chunk grids."""
        with pytest.raises(ValueError, match="rectilinear"):
            create_v2_array(
                f"file:{tmp_path.as_posix()}|zarr2:",
                shape=(6,),
                dtype="i4",
                chunks=[[2, 4]],  # type: ignore[list-item]  # deliberately off-contract
            )

    def test_format_segment_body_is_the_node_path(self, tmp_path: Path) -> None:
        """The node path comes from the URL, and missing parent groups are created."""
        arr = create_v2_array(
            f"file:{tmp_path.as_posix()}|zarr2:grp/sub/arr", shape=(2,), dtype="i4"
        )
        assert arr.path == "grp/sub/arr"
        assert (tmp_path / "grp" / "sub" / "arr" / ".zarray").exists()
        assert (tmp_path / "grp" / ".zgroup").exists()

    def test_existing_node_raises_unless_overwrite(self, tmp_path: Path) -> None:
        """Creation fails on an existing array or group; `overwrite=True` replaces it."""
        url = f"file:{tmp_path.as_posix()}|zarr2:node"
        create_v2_array(url, shape=(2,), dtype="i4")
        with pytest.raises(ContainsArrayError):
            create_v2_array(url, shape=(3,), dtype="i4")
        replaced = create_v2_array(url, shape=(3,), dtype="i4", overwrite=True)
        assert replaced.shape == (3,)
        create_v2_group(f"file:{tmp_path.as_posix()}|zarr2:g")
        with pytest.raises(ContainsGroupError):
            create_v2_array(f"file:{tmp_path.as_posix()}|zarr2:g", shape=(2,), dtype="i4")

    def test_zarr3_segment_raises(self, tmp_path: Path) -> None:
        """A `zarr3:` format segment contradicts the function."""
        with pytest.raises(ValueError, match="conflicts with the 'zarr3:' segment"):
            create_v2_array(f"file:{tmp_path.as_posix()}|zarr3:", shape=(2,), dtype="i4")

    def test_schemeless_root_raises(self, tmp_path: Path) -> None:
        """A string location is a URL pipeline, so its root needs a scheme; bare paths are rejected."""
        with pytest.raises(URLPipelineError, match="URL scheme"):
            create_v2_array(f"{tmp_path}/arr", shape=(2,), dtype="i4")

    async def test_async(self, tmp_path: Path) -> None:
        arr = await zarr.api.asynchronous.create_v2_array(
            f"file:{tmp_path.as_posix()}|zarr2:", shape=(4,), dtype="i2", chunks=(2,)
        )
        assert isinstance(arr, zarr.AsyncArray)
        assert arr.metadata.zarr_format == 2
        assert arr.chunks == (2,)


class TestCreateV2Group:
    @pytest.mark.parametrize("kind", LOCATION_KINDS)
    def test_locations(self, kind: str, tmp_path: Path) -> None:
        """Every location kind yields an empty V2 group that can be reopened."""
        group = create_v2_group(_location(kind, tmp_path, "grp"))
        assert isinstance(group, Group)
        assert group.metadata.zarr_format == 2
        assert group.attrs.asdict() == {}
        reopened = zarr.open_group(_reopen_location(kind, tmp_path, "grp"), mode="r")
        assert reopened.metadata == group.metadata

    def test_attributes(self, tmp_path: Path) -> None:
        group = create_v2_group(f"file:{tmp_path.as_posix()}|zarr2:", attributes={"k": [1, 2]})
        assert group.attrs.asdict() == {"k": [1, 2]}

    def test_existing_node_raises_unless_overwrite(self, tmp_path: Path) -> None:
        """Creation fails on an existing group or array; `overwrite=True` replaces it."""
        url = f"file:{tmp_path.as_posix()}|zarr2:node"
        create_v2_group(url, attributes={"a": 1})
        with pytest.raises(ContainsGroupError):
            create_v2_group(url)
        replaced = create_v2_group(url, overwrite=True)
        assert replaced.attrs.asdict() == {}
        create_v2_array(f"file:{tmp_path.as_posix()}|zarr2:arr", shape=(2,), dtype="i4")
        with pytest.raises(ContainsArrayError):
            create_v2_group(f"file:{tmp_path.as_posix()}|zarr2:arr")

    def test_zarr3_segment_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="conflicts with the 'zarr3:' segment"):
            create_v2_group(f"file:{tmp_path.as_posix()}|zarr3:")

    async def test_async(self, tmp_path: Path) -> None:
        group = await zarr.api.asynchronous.create_v2_group(f"file:{tmp_path.as_posix()}|zarr2:")
        assert isinstance(group, zarr.AsyncGroup)
        assert group.metadata.zarr_format == 2
