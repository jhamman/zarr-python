from __future__ import annotations

import asyncio
import dataclasses
import warnings
from typing import TYPE_CHECKING, Any, Literal, NotRequired, TypedDict, cast

import numpy as np
import numpy.typing as npt
from typing_extensions import deprecated

from zarr.abc.store import Store
from zarr.core.array import (
    DEFAULT_FILL_VALUE,
    Array,
    AsyncArray,
    CompressorLike,
    create_array,
    default_compressor_v2,
    default_compressors_v3,
    default_filters_v2,
    default_serializer_v3,
    from_array,
    get_array_metadata,
)
from zarr.core.array_spec import ArrayConfigLike, parse_array_config
from zarr.core.buffer import NDArrayLike
from zarr.core.chunk_grids import guess_chunks, normalize_chunks_nd
from zarr.core.common import (
    AUTO,
    JSON,
    AccessModeLiteral,
    ChunksLike,
    DimensionNamesLike,
    MemoryOrder,
    NamedConfig,
    ShapeLike,
    ZarrFormat,
    _default_zarr_format,
    _warn_write_empty_chunks_kwarg,
    parse_order,
    parse_shapelike,
)
from zarr.core.config import config as zarr_config
from zarr.core.dtype import ZDTypeLike, get_data_type_from_native_dtype, parse_data_type
from zarr.core.dtype.common import HasItemSize
from zarr.core.group import (
    AsyncGroup,
    ConsolidatedMetadata,
    GroupMetadata,
    create_hierarchy,
)
from zarr.core.metadata import ArrayMetadataDict, ArrayV2Metadata
from zarr.core.metadata.io import save_new_metadata
from zarr.core.metadata.v3 import (
    ArrayV3Metadata,
    ChunkGridMetadata,
    RectilinearChunkGridMetadata,
    RegularChunkGridMetadata,
    create_chunk_grid_metadata,
)
from zarr.errors import (
    ArrayNotFoundError,
    GroupNotFoundError,
    NodeTypeValidationError,
    URLPipelineError,
    ZarrDeprecationWarning,
    ZarrRuntimeWarning,
    ZarrUserWarning,
)
from zarr.storage import StorePath, URLPipeline
from zarr.storage._common import make_store_path, resolve_zarr_format

if TYPE_CHECKING:
    from collections.abc import Iterable

    from zarr.abc.codec import Codec
    from zarr.abc.numcodec import Numcodec
    from zarr.core.buffer import NDArrayLikeOrScalar
    from zarr.core.chunk_key_encodings import ChunkKeyEncoding, ChunkKeyEncodingLike
    from zarr.core.metadata.v2 import CompressorLikev2
    from zarr.core.metadata.v3 import ChunkGridLike
    from zarr.storage import StoreLike
    from zarr.types import AnyArray, AnyAsyncArray

    # TODO: this type could use some more thought
    type ArrayLike = AnyAsyncArray | AnyArray | npt.NDArray[Any]
    PathLike = str

__all__ = [
    "array",
    "consolidate_metadata",
    "copy",
    "copy_all",
    "copy_store",
    "create",
    "create_array",
    "create_hierarchy",
    "create_v2_array",
    "create_v2_group",
    "create_v3_array",
    "create_v3_group",
    "empty",
    "empty_like",
    "from_array",
    "full",
    "full_like",
    "group",
    "load",
    "ones",
    "ones_like",
    "open",
    "open_array",
    "open_consolidated",
    "open_group",
    "open_like",
    "open_url",
    "save",
    "save_array",
    "save_group",
    "tree",
    "zeros",
    "zeros_like",
]


_READ_MODES: tuple[AccessModeLiteral, ...] = ("r", "r+", "a")
_CREATE_MODES: tuple[AccessModeLiteral, ...] = ("a", "w", "w-")
_OVERWRITE_MODES: tuple[AccessModeLiteral, ...] = ("w",)


def _infer_overwrite(mode: AccessModeLiteral) -> bool:
    """
    Check that an `AccessModeLiteral` is compatible with overwriting an existing Zarr node.
    """
    return mode in _OVERWRITE_MODES


def _warn_unimplemented_kwargs(kwargs: dict[str, Any]) -> None:
    """
    Emit a "not yet implemented" warning for each provided keyword argument that is not None.

    `kwargs` maps a keyword argument name to its supplied value. The `stacklevel` is chosen
    so the warning points at the caller of the public API function (the same location as an
    inline `warnings.warn(..., stacklevel=2)` would).
    """
    for name, value in kwargs.items():
        if value is not None:
            warnings.warn(f"{name} is not yet implemented", ZarrRuntimeWarning, stacklevel=3)


def _get_shape_chunks(a: ArrayLike | Any) -> tuple[tuple[int, ...] | None, tuple[int, ...] | None]:
    """Helper function to get the shape and chunks from an array-like object"""
    shape = None
    chunks = None

    if hasattr(a, "shape") and isinstance(a.shape, tuple):
        shape = a.shape

        if hasattr(a, "chunks") and isinstance(a.chunks, tuple) and (len(a.chunks) == len(a.shape)):
            chunks = a.chunks

        elif hasattr(a, "chunklen"):
            # bcolz carray
            chunks = (a.chunklen,) + a.shape[1:]

    return shape, chunks


class _LikeArgs(TypedDict):
    shape: NotRequired[tuple[int, ...]]
    chunks: NotRequired[tuple[int, ...]]
    dtype: NotRequired[np.dtype[np.generic]]
    order: NotRequired[Literal["C", "F"]]
    filters: NotRequired[tuple[Numcodec, ...] | None]
    compressor: NotRequired[CompressorLikev2]
    codecs: NotRequired[tuple[Codec, ...]]
    fill_value: NotRequired[Any]
    zarr_format: NotRequired[ZarrFormat]


def _like_args(a: ArrayLike, zarr_format: ZarrFormat | None) -> _LikeArgs:
    """
    Arguments for creating an array like `a` in `zarr_format`.

    If `a` is a zarr array, the new array has the zarr format of `a` unless `zarr_format`
    requests another. The storage settings of `a` (memory order and codecs) are specific to
    its zarr format, so they are copied only when the new array has the format of `a`.
    Otherwise the new array uses the defaults of its own format.
    """
    new: _LikeArgs = {}

    shape, chunks = _get_shape_chunks(a)
    if shape is not None:
        new["shape"] = shape
    if chunks is not None:
        new["chunks"] = chunks

    if hasattr(a, "dtype"):
        new["dtype"] = a.dtype

    if isinstance(a, AsyncArray | Array):
        new["fill_value"] = a.metadata.fill_value
        if zarr_format is None:
            zarr_format = a.metadata.zarr_format
        if a.metadata.zarr_format == zarr_format:
            if isinstance(a.metadata, ArrayV2Metadata):
                new["order"] = a.order
                new["compressor"] = a.metadata.compressor
                new["filters"] = a.metadata.filters
            else:
                # TODO: Remove type: ignore statement when type inference improves.
                # mypy cannot correctly infer the type of a.metadata here for some reason.
                new["codecs"] = a.metadata.codecs

    else:
        # TODO: set default values compressor/codecs
        # to do this, we may need to evaluate if this is a v2 or v3 array
        # new["compressor"] = "default"
        pass

    if zarr_format is not None:
        new["zarr_format"] = zarr_format

    return new


async def consolidate_metadata(
    store: StoreLike,
    path: str | None = None,
    zarr_format: ZarrFormat | None = None,
) -> AsyncGroup:
    """
    Consolidate the metadata of all nodes in a hierarchy.

    Upon completion, the metadata of the root node in the Zarr hierarchy will be
    updated to include all the metadata of child nodes. For Stores that do
    not support consolidated metadata, this operation raises a `TypeError`.

    Parameters
    ----------
    store : StoreLike
        The store-like object whose metadata you wish to consolidate. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    path : str, optional
        A path to a group in the store to consolidate at. Only children
        below that group will be consolidated.

        By default, the root node is used so all the metadata in the
        store is consolidated.
    zarr_format : {2, 3, None}, optional
        The zarr format of the hierarchy. By default the zarr format
        is inferred.

    Returns
    -------
    group: AsyncGroup
        The group, with the `consolidated_metadata` field set to include
        the metadata of each child node. If the Store doesn't support
        consolidated metadata, this function raises a `TypeError`.
        See `Store.supports_consolidated_metadata`.
    """
    store_path = await make_store_path(store, path=path)

    if not store_path.store.supports_consolidated_metadata:
        store_name = type(store_path.store).__name__
        raise TypeError(
            f"The Zarr Store in use ({store_name}) doesn't support consolidated metadata",
        )

    group = await AsyncGroup.open(store_path, zarr_format=zarr_format, use_consolidated=False)
    group.store_path.store._check_writable()

    members_metadata = {
        k: v.metadata
        async for k, v in group.members(max_depth=None, use_consolidated_for_children=False)
    }
    # While consolidating, we want to be explicit about when child groups
    # are empty by inserting an empty dict for consolidated_metadata.metadata
    for k, v in members_metadata.items():
        if isinstance(v, GroupMetadata) and v.consolidated_metadata is None:
            v = dataclasses.replace(v, consolidated_metadata=ConsolidatedMetadata(metadata={}))
            members_metadata[k] = v

    if any(m.zarr_format == 3 for m in members_metadata.values()):
        warnings.warn(
            "Consolidated metadata is currently not part in the Zarr format 3 specification. It "
            "may not be supported by other zarr implementations and may change in the future.",
            category=ZarrUserWarning,
            stacklevel=1,
        )

    ConsolidatedMetadata._flat_to_nested(members_metadata)

    consolidated_metadata = ConsolidatedMetadata(metadata=members_metadata)
    metadata = dataclasses.replace(group.metadata, consolidated_metadata=consolidated_metadata)
    group = dataclasses.replace(
        group,
        metadata=metadata,
    )
    await group._save_metadata()
    return group


async def copy(*args: Any, **kwargs: Any) -> tuple[int, int, int]:
    """
    Not implemented.
    """
    raise NotImplementedError


async def copy_all(*args: Any, **kwargs: Any) -> tuple[int, int, int]:
    """
    Not implemented.
    """
    raise NotImplementedError


async def copy_store(*args: Any, **kwargs: Any) -> tuple[int, int, int]:
    """
    Not implemented.
    """
    raise NotImplementedError


async def load(
    *,
    store: StoreLike,
    path: str | None = None,
    zarr_format: ZarrFormat | None = None,
) -> NDArrayLikeOrScalar | dict[str, NDArrayLikeOrScalar]:
    """Load data from an array or group into memory.

    Parameters
    ----------
    store : StoreLike
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    path : str or None, optional
        The path within the store from which to load.

    Returns
    -------
    out
        If the path contains an array, out will be a numpy array. If the path contains
        a group, out will be a dict-like object where keys are array names and values
        are numpy arrays.

    See Also
    --------
    save, open

    Notes
    -----
    If loading data from a group of arrays, data will not be immediately loaded into
    memory. Rather, arrays will be loaded into memory as they are requested.

    Unlike [`open`][zarr.open], which returns a lazy [`Array`][zarr.Array] or
    [`Group`][zarr.Group] backed by the store, `load` eagerly reads the data and
    returns it as an in-memory array (or a dict of arrays for a group).
    The array type is NumPy by default, but follows the configured
    buffer prototype (for example, CuPy for GPU use cases).
    Use `open` when you want to read or write data incrementally without loading it
    all into memory.
    """

    obj = await open(store=store, path=path, zarr_format=zarr_format)
    if isinstance(obj, AsyncArray):
        return await obj.getitem(Ellipsis)
    else:
        raise NotImplementedError("loading groups not yet supported")


async def open(
    *,
    store: StoreLike | None = None,
    mode: AccessModeLiteral | None = None,
    zarr_format: ZarrFormat | None = None,
    path: str | None = None,
    storage_options: dict[str, Any] | None = None,
    **kwargs: Any,  # TODO: type kwargs as valid args to open_array
) -> AnyAsyncArray | AsyncGroup:
    """Convenience function to open a group or array using file-mode-like semantics.

    Parameters
    ----------
    store : StoreLike or None, default=None
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    mode : {'r', 'r+', 'a', 'w', 'w-'}, optional
        Persistence mode: 'r' means read only (must exist); 'r+' means
        read/write (must exist); 'a' means read/write (create if doesn't
        exist); 'w' means create (overwrite if exists); 'w-' means create
        (fail if exists). On a store that cannot delete keys, 'w' raises an
        error instead of replacing an existing node.
        If the store is read-only, the default is 'r'; otherwise, it is 'a'.
    zarr_format : {2, 3, None}, optional
        The zarr format to use when saving.
    path : str or None, optional
        The path within the store to open.
    storage_options : dict
        If using an fsspec URL to create the store, these will be passed to
        the backend implementation. Ignored otherwise.
    **kwargs
        Additional parameters are passed through to `zarr.open_array` or
        `zarr.open_group`.

    Returns
    -------
    z : array or group
        Return type depends on what exists in the given store.

    See Also
    --------
    load

    Notes
    -----
    `open` returns a lazy [`Array`][zarr.Array] or [`Group`][zarr.Group] backed by
    the store, so data is read and written incrementally. Use [`load`][zarr.load]
    instead when you want the data eagerly read into an in-memory array (a
    NumPy array by default).
    """

    if mode is None:
        if isinstance(store, (Store, StorePath)) and store.read_only:
            mode = "r"
        else:
            mode = "a"
    zarr_format = resolve_zarr_format(store, zarr_format)
    store_path = await make_store_path(store, mode=mode, path=path, storage_options=storage_options)

    # TODO: the mode check below seems wrong!
    if "shape" not in kwargs and mode in (*_READ_MODES, "w"):
        # mode "w" replaces any existing node, so there is nothing to open
        if mode in _READ_MODES:
            try:
                metadata_dict = await get_array_metadata(store_path, zarr_format=zarr_format)
                # TODO: remove this cast when we fix typing for array metadata dicts
                _metadata_dict = cast("ArrayMetadataDict", metadata_dict)
                # for v2, the above would already have raised an exception if not an array
                zarr_format = _metadata_dict["zarr_format"]
                is_v3_array = zarr_format == 3 and _metadata_dict.get("node_type") == "array"
                if is_v3_array or zarr_format == 2:
                    return AsyncArray(
                        store_path=store_path,
                        metadata=_metadata_dict,
                        config=kwargs.get("config"),
                    )
            except (FileNotFoundError, NodeTypeValidationError):
                pass
        return await open_group(store=store_path, zarr_format=zarr_format, mode=mode, **kwargs)

    try:
        return await open_array(store=store_path, zarr_format=zarr_format, mode=mode, **kwargs)
    except (KeyError, NodeTypeValidationError):
        # KeyError for a missing key
        # NodeTypeValidationError for failing to parse node metadata as an array when it's
        # actually a group
        return await open_group(store=store_path, zarr_format=zarr_format, mode=mode, **kwargs)


async def _resolve_creation_location(
    location: str | URLPipeline | Store | StorePath,
    storage_options: dict[str, Any] | None,
    *,
    zarr_format: ZarrFormat,
) -> StorePath:
    """
    Resolve the location argument of the `create_v{2,3}_array` / `create_v{2,3}_group`
    functions into a writable `StorePath`. A string is a URL pipeline and must carry
    a scheme on its root; a pipeline must not select a different Zarr format.
    """
    if isinstance(location, str):
        location = URLPipeline.from_url(location)
        if not location.segments[0].scheme:
            raise URLPipelineError(
                f"{str(location)!r} has no URL scheme on its root sub-URL. A string location "
                "is a URL pipeline: spell a local path as an absolute 'file:' URL, and spell a "
                "literal '|' in it as '%7C'."
            )
    if isinstance(location, URLPipeline):
        # raises if the pipeline selects a different format
        location.resolve_zarr_format(zarr_format)
    store_path = await make_store_path(location, storage_options=storage_options)
    if store_path.read_only:
        raise ValueError(f"cannot create a node at {store_path}: the store is read-only")
    return store_path


async def create_v2_array(
    location: str | URLPipeline | Store | StorePath,
    *,
    shape: ShapeLike,
    dtype: ZDTypeLike,
    chunks: Iterable[int] | AUTO = AUTO,
    fill_value: Any | AUTO = AUTO,
    order: MemoryOrder | AUTO = AUTO,
    dimension_separator: Literal[".", "/"] = ".",
    compressor: CompressorLikev2 | AUTO = AUTO,
    filters: Iterable[Numcodec | dict[str, JSON]] | AUTO | None = AUTO,
    attributes: dict[str, JSON] | None = None,
    overwrite: bool = False,
    storage_options: dict[str, Any] | None = None,
) -> AsyncArray[ArrayV2Metadata]:
    """Create a Zarr format 2 array at a URL pipeline or store location.

    The keyword arguments are the fields of the Zarr V2 array metadata document
    (`.zarray`); this function does not abstract over Zarr formats. Fields left at
    [`AUTO`][zarr.AUTO] are computed from the shape, the data type and the
    configuration, while `None` keeps the meaning the metadata gives it.

    Parameters
    ----------
    location : str | URLPipeline | Store | StorePath
        Where to create the array. A string is always read as a
        [URL pipeline][user-guide-url-pipelines] whose root carries a URL scheme,
        e.g. `"file:/data/example.zarr|zarr2:group/array"`: the body of a trailing
        `zarr2:` segment is the path of the array within the store, and a literal
        `|` in a local path is spelled `%7C`. A `URLPipeline` is the parsed form of
        such a string. A `Store` addresses its root; a `StorePath` addresses a path
        within a store. A `zarr3:` segment raises `ValueError`.
    shape : tuple[int, ...]
        Shape of the array.
    dtype : ZDTypeLike
        Data type of the array.
    chunks : Iterable[int] | AUTO, optional
        The chunk shape, one integer per dimension. By default, guessed from the
        shape and data type. Zarr format 2 has no rectilinear chunk grids.
    fill_value : Any | AUTO, optional
        The fill value. By default, the data type's default scalar. `None` is stored
        as `null`, meaning no fill value.
    order : {'C', 'F'} | AUTO, optional
        Memory layout of chunks. By default, the configured `array.order`.
    dimension_separator : {'.', '/'}, optional
        Separator between dimension indices in chunk keys. By default `.`.
    compressor : dict[str, JSON] | Numcodec | None | AUTO, optional
        The single compressor applied after the filters. By default, the configured
        default compressor. `None` means no compressor.
    filters : Iterable[Numcodec | dict[str, JSON]] | None | AUTO, optional
        Filters applied in order before compression. By default, the data type's
        object codec for variable-length data types and none otherwise. `None`
        means no filters.
    attributes : dict[str, JSON] | None, optional
        User attributes. By default, empty.
    overwrite : bool, optional
        If True, delete any existing node at the location before creating the array.
        Otherwise an existing array or group raises `ContainsArrayError` or
        `ContainsGroupError`.
    storage_options : dict[str, Any] | None, optional
        Options for the root sub-URL of a URL pipeline (e.g. fsspec options). Not
        accepted together with a `Store` or `StorePath`.

    Returns
    -------
    AsyncArray
        The new array.
    """
    store_path = await _resolve_creation_location(location, storage_options, zarr_format=2)
    zdtype = parse_data_type(dtype, zarr_format=2)
    shape_parsed = parse_shapelike(shape)

    if chunks is AUTO:
        item_size = zdtype.item_size if isinstance(zdtype, HasItemSize) else 1
        grid = guess_chunks(shape_parsed, item_size)
    else:
        grid = normalize_chunks_nd(tuple(chunks), shape_parsed)
    if not grid.is_regular:
        raise ValueError("Zarr format 2 does not support rectilinear chunk grids.")

    metadata = ArrayV2Metadata(
        shape=shape_parsed,
        dtype=zdtype,
        chunks=grid.chunk_shape,
        fill_value=zdtype.default_scalar() if fill_value is AUTO else fill_value,
        order=parse_order(zarr_config.get("array.order")) if order is AUTO else order,
        dimension_separator=dimension_separator,
        compressor=default_compressor_v2(zdtype) if compressor is AUTO else compressor,
        filters=default_filters_v2(zdtype) if filters is AUTO else filters,
        attributes=attributes,
    )
    await save_new_metadata(store_path, metadata, overwrite=overwrite, ensure_parents=True)
    return AsyncArray(metadata=metadata, store_path=store_path)


async def create_v2_group(
    location: str | URLPipeline | Store | StorePath,
    *,
    attributes: dict[str, JSON] | None = None,
    overwrite: bool = False,
    storage_options: dict[str, Any] | None = None,
) -> AsyncGroup:
    """Create a Zarr format 2 group at a URL pipeline or store location.

    The keyword arguments are the fields of the Zarr V2 group metadata documents
    (`.zgroup` and `.zattrs`); this function does not abstract over Zarr formats.

    Parameters
    ----------
    location : str | URLPipeline | Store | StorePath
        Where to create the group. A string is always read as a
        [URL pipeline][user-guide-url-pipelines] whose root carries a URL scheme,
        e.g. `"file:/data/example.zarr|zarr2:group"`: the body of a trailing
        `zarr2:` segment is the path of the group within the store, and a literal
        `|` in a local path is spelled `%7C`. A `URLPipeline` is the parsed form of
        such a string. A `Store` addresses its root; a `StorePath` addresses a path
        within a store. A `zarr3:` segment raises `ValueError`.
    attributes : dict[str, JSON] | None, optional
        User attributes. By default, empty.
    overwrite : bool, optional
        If True, delete any existing node at the location before creating the group.
        Otherwise an existing array or group raises `ContainsArrayError` or
        `ContainsGroupError`.
    storage_options : dict[str, Any] | None, optional
        Options for the root sub-URL of a URL pipeline (e.g. fsspec options). Not
        accepted together with a `Store` or `StorePath`.

    Returns
    -------
    AsyncGroup
        The new group.
    """
    store_path = await _resolve_creation_location(location, storage_options, zarr_format=2)
    metadata = GroupMetadata(attributes={} if attributes is None else attributes, zarr_format=2)
    await save_new_metadata(store_path, metadata, overwrite=overwrite, ensure_parents=True)
    return AsyncGroup(metadata=metadata, store_path=store_path)


async def create_v3_array(
    location: str | URLPipeline | Store | StorePath,
    *,
    shape: ShapeLike,
    data_type: ZDTypeLike,
    chunk_grid: ChunkGridLike | AUTO = AUTO,
    codecs: Iterable[Codec | dict[str, JSON] | NamedConfig[str, Any] | str] | AUTO = AUTO,
    chunk_key_encoding: ChunkKeyEncodingLike | AUTO = AUTO,
    fill_value: Any | AUTO = AUTO,
    attributes: dict[str, JSON] | None = None,
    dimension_names: DimensionNamesLike = None,
    storage_transformers: Iterable[dict[str, JSON]] = (),
    overwrite: bool = False,
    storage_options: dict[str, Any] | None = None,
) -> AsyncArray[ArrayV3Metadata]:
    """Create a Zarr format 3 array at a URL pipeline or store location.

    The keyword arguments are the fields of the Zarr V3 array metadata document;
    this function does not abstract over Zarr formats. Fields left at
    [`AUTO`][zarr.AUTO] are computed from the shape, the data type and the
    configuration.

    Parameters
    ----------
    location : str | URLPipeline | Store | StorePath
        Where to create the array. A string is always read as a
        [URL pipeline][user-guide-url-pipelines] whose root carries a URL scheme,
        e.g. `"file:/data/example.zarr|zarr3:group/array"`: the body of a trailing
        `zarr3:` segment is the path of the array within the store, and a literal
        `|` in a local path is spelled `%7C`. A `URLPipeline` is the parsed form of
        such a string. A `Store` addresses its root; a `StorePath` addresses a path
        within a store. A `zarr2:` segment raises `ValueError`.
    shape : tuple[int, ...]
        Shape of the array.
    data_type : ZDTypeLike
        Data type of the array.
    chunk_grid : ChunkGridLike | AUTO, optional
        The chunk grid: a chunk shape (one integer per dimension) for a regular grid,
        or a chunk grid metadata object or document. By default, a regular grid with
        a chunk shape guessed from the shape and data type.
    codecs : Iterable[Codec | dict[str, JSON]] | AUTO, optional
        The codec chain, in order, from the first array-to-array codec to the last
        bytes-to-bytes codec. By default, the default serializer for the data type
        followed by the configured default compressors.
    chunk_key_encoding : ChunkKeyEncodingLike | AUTO, optional
        The chunk key encoding. By default, the `default` encoding with `/` as the
        separator.
    fill_value : Any | AUTO, optional
        The fill value. By default, the data type's default scalar.
    attributes : dict[str, JSON] | None, optional
        User attributes. By default, empty.
    dimension_names : Iterable[str | None] | None, optional
        Dimension names. By default, the field is omitted from the metadata.
    storage_transformers : Iterable[dict[str, JSON]], optional
        Storage transformers. By default, none.
    overwrite : bool, optional
        If True, delete any existing node at the location before creating the array.
        Otherwise an existing array or group raises `ContainsArrayError` or
        `ContainsGroupError`.
    storage_options : dict[str, Any] | None, optional
        Options for the root sub-URL of a URL pipeline (e.g. fsspec options). Not
        accepted together with a `Store` or `StorePath`.

    Returns
    -------
    AsyncArray
        The new array.
    """
    store_path = await _resolve_creation_location(location, storage_options, zarr_format=3)
    zdtype = parse_data_type(data_type, zarr_format=3)
    shape_parsed = parse_shapelike(shape)

    chunk_grid_parsed: ChunkGridMetadata | dict[str, JSON] | NamedConfig[str, Any]
    if chunk_grid is AUTO:
        item_size = zdtype.item_size if isinstance(zdtype, HasItemSize) else 1
        chunk_grid_parsed = create_chunk_grid_metadata(guess_chunks(shape_parsed, item_size))
    elif isinstance(chunk_grid, dict | RegularChunkGridMetadata | RectilinearChunkGridMetadata):
        chunk_grid_parsed = chunk_grid
    else:
        # a chunk shape; the NamedConfig TypedDict is excluded at runtime by the
        # dict check above, but mypy does not narrow it away
        chunk_shape = tuple(cast("Iterable[int]", chunk_grid))
        chunk_grid_parsed = create_chunk_grid_metadata(
            normalize_chunks_nd(chunk_shape, shape_parsed)
        )

    codecs_parsed: Iterable[Codec | dict[str, JSON] | NamedConfig[str, Any] | str]
    if codecs is AUTO:
        codecs_parsed = (default_serializer_v3(zdtype), *default_compressors_v3(zdtype))
    else:
        codecs_parsed = tuple(codecs)

    metadata = ArrayV3Metadata(
        shape=shape_parsed,
        data_type=zdtype,
        chunk_grid=chunk_grid_parsed,
        chunk_key_encoding=(
            {"name": "default", "separator": "/"}
            if chunk_key_encoding is AUTO
            else chunk_key_encoding
        ),
        fill_value=zdtype.default_scalar() if fill_value is AUTO else fill_value,
        codecs=codecs_parsed,
        attributes=attributes,
        dimension_names=dimension_names,
        storage_transformers=tuple(storage_transformers),
    )
    await save_new_metadata(store_path, metadata, overwrite=overwrite, ensure_parents=True)
    return AsyncArray(metadata=metadata, store_path=store_path)


async def create_v3_group(
    location: str | URLPipeline | Store | StorePath,
    *,
    attributes: dict[str, JSON] | None = None,
    overwrite: bool = False,
    storage_options: dict[str, Any] | None = None,
) -> AsyncGroup:
    """Create a Zarr format 3 group at a URL pipeline or store location.

    The keyword arguments are the fields of the Zarr V3 group metadata document;
    this function does not abstract over Zarr formats.

    Parameters
    ----------
    location : str | URLPipeline | Store | StorePath
        Where to create the group. A string is always read as a
        [URL pipeline][user-guide-url-pipelines] whose root carries a URL scheme,
        e.g. `"file:/data/example.zarr|zarr3:group"`: the body of a trailing
        `zarr3:` segment is the path of the group within the store, and a literal
        `|` in a local path is spelled `%7C`. A `URLPipeline` is the parsed form of
        such a string. A `Store` addresses its root; a `StorePath` addresses a path
        within a store. A `zarr2:` segment raises `ValueError`.
    attributes : dict[str, JSON] | None, optional
        User attributes. By default, empty.
    overwrite : bool, optional
        If True, delete any existing node at the location before creating the group.
        Otherwise an existing array or group raises `ContainsArrayError` or
        `ContainsGroupError`.
    storage_options : dict[str, Any] | None, optional
        Options for the root sub-URL of a URL pipeline (e.g. fsspec options). Not
        accepted together with a `Store` or `StorePath`.

    Returns
    -------
    AsyncGroup
        The new group.
    """
    store_path = await _resolve_creation_location(location, storage_options, zarr_format=3)
    metadata = GroupMetadata(attributes={} if attributes is None else attributes, zarr_format=3)
    await save_new_metadata(store_path, metadata, overwrite=overwrite, ensure_parents=True)
    return AsyncGroup(metadata=metadata, store_path=store_path)


async def open_url(
    url: str,
    *,
    mode: Literal["r", "r+"] = "r",
    storage_options: dict[str, Any] | None = None,
) -> AnyAsyncArray | AsyncGroup:
    """Open the existing group or array addressed by a URL pipeline.

    This is the only entry point that reads a string as a
    [URL pipeline][user-guide-url-pipelines]; [`open`][zarr.api.asynchronous.open] and the other
    `StoreLike`-taking functions treat strings as they always have. Unlike
    `open`, `open_url` never creates or overwrites anything: to create a node at
    a pipeline-addressed location, pass a `URLPipeline` to
    [`create_array`][zarr.api.asynchronous.create_array] or
    [`create_group`][zarr.api.asynchronous.create_group].

    The zarr format is taken from a trailing `zarr2:`/`zarr3:` segment when
    present, and inferred from the stored metadata otherwise.

    Parameters
    ----------
    url : str
        A URL pipeline such as `"s3://bucket/data.zip|zip:|zarr3:"`. A URL
        without `|` is a trivial pipeline consisting of its root alone. The
        node to open is addressed by the URL itself: the body of a trailing
        `zarr2:`/`zarr3:` segment is the path of the node within the store.
    mode : {'r', 'r+'}, optional
        `'r'` (the default) opens read-only; `'r+'` opens for reading and
        writing. The node must exist in either case.
    storage_options : dict, optional
        Options for the pipeline's root sub-URL (e.g. fsspec options).

    Returns
    -------
    z : array or group
        Return type depends on what exists at the URL. For a statically typed
        result, pass a `URLPipeline` to `open_array` or `open_group` instead.
    """
    from zarr.storage import URLPipeline

    if mode not in ("r", "r+"):
        raise ValueError(
            f"Invalid mode: {mode!r}. open_url opens existing nodes only and accepts "
            "'r' or 'r+'; to create a node at a URL pipeline, pass "
            "URLPipeline.from_url(url) to create_array or create_group."
        )
    return await open(
        store=URLPipeline.from_url(url),
        mode=mode,
        storage_options=storage_options,
    )


async def open_consolidated(
    *args: Any, use_consolidated: Literal[True] = True, **kwargs: Any
) -> AsyncGroup:
    """
    Alias for [`open_group`][zarr.api.asynchronous.open_group] with `use_consolidated=True`.
    """
    if use_consolidated is not True:
        raise TypeError(
            "'use_consolidated' must be 'True' in 'open_consolidated'. Use 'open' with "
            "'use_consolidated=False' to bypass consolidated metadata."
        )
    return await open_group(*args, use_consolidated=use_consolidated, **kwargs)


async def save(
    store: StoreLike,
    *args: NDArrayLike,
    zarr_format: ZarrFormat | None = None,
    path: str | None = None,
    storage_options: dict[str, Any] | None = None,
    **kwargs: Any,  # TODO: type kwargs as valid args to save
) -> None:
    """Convenience function to save an array or group of arrays to the local file system.

    Parameters
    ----------
    store : StoreLike
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    *args : ndarray
        NumPy arrays with data to save.
    zarr_format : {2, 3, None}, optional
        The zarr format to use when saving.
    path : str or None, optional
        The path within the group where the arrays will be saved.
    storage_options : dict
        If using an fsspec URL to create the store, these will be passed to
        the backend implementation. Ignored otherwise.
    **kwargs
        NumPy arrays with data to save.
    """

    if len(args) == 0 and len(kwargs) == 0:
        raise ValueError("at least one array must be provided")
    if len(args) == 1 and len(kwargs) == 0:
        await save_array(
            store, args[0], zarr_format=zarr_format, path=path, storage_options=storage_options
        )
    else:
        await save_group(
            store,
            *args,
            zarr_format=zarr_format,
            path=path,
            storage_options=storage_options,
            **kwargs,
        )


async def save_array(
    store: StoreLike,
    arr: NDArrayLike,
    *,
    zarr_format: ZarrFormat | None = None,
    path: str | None = None,
    storage_options: dict[str, Any] | None = None,
    **kwargs: Any,  # TODO: type kwargs as valid args to create
) -> None:
    """Convenience function to save a NumPy array to the local file system, following a
    similar API to the NumPy save() function.

    Parameters
    ----------
    store : StoreLike
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    arr : ndarray
        NumPy array with data to save.
    zarr_format : {2, 3, None}, optional
        The zarr format to use when saving. The default is `None`, which will
        use the default Zarr format defined in the global configuration object.
    path : str or None, optional
        The path within the store where the array will be saved.
    storage_options : dict
        If using an fsspec URL to create the store, these will be passed to
        the backend implementation. Ignored otherwise.
    **kwargs
        Passed through to [`create`][zarr.api.asynchronous.create], e.g., compressor.
    """
    if not isinstance(arr, NDArrayLike):
        raise TypeError("arr argument must be numpy or other NDArrayLike array")

    mode = kwargs.pop("mode", "a")
    zarr_format = resolve_zarr_format(store, zarr_format)
    store_path = await make_store_path(store, path=path, mode=mode, storage_options=storage_options)
    if zarr_format is None:
        zarr_format = _default_zarr_format()
    if np.isscalar(arr):
        arr = np.array(arr)
    shape = arr.shape
    chunks = getattr(arr, "chunks", None)  # for array-likes with chunks attribute
    overwrite = kwargs.pop("overwrite", None) or _infer_overwrite(mode)
    zarr_dtype = get_data_type_from_native_dtype(arr.dtype)
    new = await AsyncArray._create(
        store_path,
        zarr_format=zarr_format,
        shape=shape,
        dtype=zarr_dtype,
        chunks=chunks,
        overwrite=overwrite,
        **kwargs,
    )
    await new.setitem(Ellipsis, arr)


async def save_group(
    store: StoreLike,
    *args: NDArrayLike,
    zarr_format: ZarrFormat | None = None,
    path: str | None = None,
    storage_options: dict[str, Any] | None = None,
    **kwargs: NDArrayLike,
) -> None:
    """Convenience function to save several NumPy arrays to the local file system, following a
    similar API to the NumPy savez()/savez_compressed() functions.

    Parameters
    ----------
    store : StoreLike
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    *args : ndarray
        NumPy arrays with data to save.
    zarr_format : {2, 3, None}, optional
        The zarr format to use when saving.
    path : str or None, optional
        Path within the store where the group will be saved.
    storage_options : dict
        If using an fsspec URL to create the store, these will be passed to
        the backend implementation. Ignored otherwise.
    **kwargs
        NumPy arrays with data to save.
    """

    for arg in args:
        if not isinstance(arg, NDArrayLike):
            raise TypeError(
                "All arguments must be numpy or other NDArrayLike arrays (except store, path, storage_options, and zarr_format)"
            )
    for k, v in kwargs.items():
        if not isinstance(v, NDArrayLike):
            raise TypeError(f"Keyword argument '{k}' must be a numpy or other NDArrayLike array")

    if len(args) == 0 and len(kwargs) == 0:
        raise ValueError("at least one array must be provided")

    # Resolve every data type before anything is deleted, so an array that Zarr cannot
    # store raises while the existing node is still intact.
    for arr in (*args, *kwargs.values()):
        get_data_type_from_native_dtype(arr.dtype)

    zarr_format = resolve_zarr_format(store, zarr_format)
    store_path = await make_store_path(store, path=path, mode="w", storage_options=storage_options)
    if zarr_format is None:
        zarr_format = _default_zarr_format()
    # The group replaces whatever is stored under the path, now that the arguments are known
    # to be valid.
    await AsyncGroup.from_store(store_path, zarr_format=zarr_format, overwrite=True)
    aws = []
    # `store_path` already consumed `storage_options`, so passing them on again would
    # make `make_store_path` reject them as unused.
    for i, arr in enumerate(args):
        aws.append(save_array(store_path, arr, zarr_format=zarr_format, path=f"arr_{i}"))
    for k, arr in kwargs.items():
        aws.append(save_array(store_path, arr, zarr_format=zarr_format, path=k))
    await asyncio.gather(*aws)


@deprecated("Use AsyncGroup.tree instead.", category=ZarrDeprecationWarning)
async def tree(grp: AsyncGroup, expand: bool | None = None, level: int | None = None) -> Any:
    """Provide a rich display of the hierarchy.

    !!! warning "Deprecated"
        `zarr.tree()` is deprecated since v3.0.0 and will be removed in a future release.
        Use `group.tree()` instead.

    Parameters
    ----------
    grp : Group
        Zarr or h5py group.
    expand : bool, optional
        Only relevant for HTML representation. If True, tree will be fully expanded.
    level : int, optional
        Maximum depth to descend into hierarchy.

    Returns
    -------
    TreeRepr
        A pretty-printable object displaying the hierarchy.
    """
    return await grp.tree(expand=expand, level=level)


async def array(data: npt.ArrayLike | AnyArray, **kwargs: Any) -> AnyAsyncArray:
    """Create an array filled with `data`.

    Parameters
    ----------
    data : array_like
        The data to fill the array with.
    **kwargs
        Passed through to [`create`][zarr.api.asynchronous.create].

    Returns
    -------
    array : array
        The new array.
    """

    if isinstance(data, Array):
        return await from_array(data=data, **kwargs)

    # ensure data is array-like
    if not hasattr(data, "shape") or not hasattr(data, "dtype"):
        data = np.asanyarray(data)

    # setup dtype
    kw_dtype = kwargs.get("dtype")
    if kw_dtype is None and hasattr(data, "dtype"):
        kwargs["dtype"] = data.dtype
    else:
        kwargs["dtype"] = kw_dtype

    # setup shape and chunks
    data_shape, data_chunks = _get_shape_chunks(data)
    kwargs["shape"] = data_shape
    kw_chunks = kwargs.get("chunks")
    if kw_chunks is None:
        kwargs["chunks"] = data_chunks
    else:
        kwargs["chunks"] = kw_chunks

    read_only = kwargs.pop("read_only", False)
    if read_only:
        raise ValueError("read_only=True is no longer supported when creating new arrays")

    # instantiate array
    z = await create(**kwargs)

    # fill with data
    await z.setitem(Ellipsis, data)

    return z


async def group(
    *,  # Note: this is a change from v2
    store: StoreLike | None = None,
    overwrite: bool = False,
    chunk_store: StoreLike | None = None,  # not used
    cache_attrs: bool | None = None,  # not used, default changed
    synchronizer: Any | None = None,  # not used
    path: str | None = None,
    zarr_format: ZarrFormat | None = None,
    meta_array: Any | None = None,  # not used
    attributes: dict[str, JSON] | None = None,
    storage_options: dict[str, Any] | None = None,
) -> AsyncGroup:
    """Create a group.

    Parameters
    ----------
    store : StoreLike or None, default=None
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    overwrite : bool, optional
        If True, delete any pre-existing data in `store` at `path` before
        creating the group.
    chunk_store : StoreLike or None, default=None
        Separate storage for chunks. Not implemented.
    cache_attrs : bool, optional
        If True (default), user attributes will be cached for attribute read
        operations. If False, user attributes are reloaded from the store prior
        to all attribute read operations.
    synchronizer : object, optional
        Array synchronizer.
    path : str, optional
        Group path within store.
    meta_array : array-like, optional
        An array instance to use for determining arrays to create and return
        to users. Use `numpy.empty(())` by default.
    zarr_format : {2, 3, None}, optional
        The zarr format to use when saving.
    storage_options : dict
        If using an fsspec URL to create the store, these will be passed to
        the backend implementation. Ignored otherwise.

    Returns
    -------
    g : group
        The new group.
    """
    mode: AccessModeLiteral
    if overwrite:
        mode = "w"
    else:
        mode = "a"
    return await open_group(
        store=store,
        mode=mode,
        chunk_store=chunk_store,
        cache_attrs=cache_attrs,
        synchronizer=synchronizer,
        path=path,
        zarr_format=zarr_format,
        meta_array=meta_array,
        attributes=attributes,
        storage_options=storage_options,
    )


async def create_group(
    *,
    store: StoreLike,
    path: str | None = None,
    overwrite: bool = False,
    zarr_format: ZarrFormat | None = None,
    attributes: dict[str, Any] | None = None,
    storage_options: dict[str, Any] | None = None,
) -> AsyncGroup:
    """Create a group.

    Parameters
    ----------
    store : StoreLike
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    path : str, optional
        Group path within store.
    overwrite : bool, optional
        If True, pre-existing data at `path` will be deleted before
        creating the group.
    zarr_format : {2, 3, None}, optional
        The zarr format to use when saving.
        If no `zarr_format` is provided, the default format will be used.
        This default can be changed by modifying the value of `default_zarr_format`
        in [`zarr.config`][zarr.config].
    storage_options : dict
        If using an fsspec URL to create the store, these will be passed to
        the backend implementation. Ignored otherwise.

    Returns
    -------
    AsyncGroup
        The new group.
    """

    mode: Literal["a"] = "a"

    zarr_format = resolve_zarr_format(store, zarr_format)
    store_path = await make_store_path(store, path=path, mode=mode, storage_options=storage_options)
    if zarr_format is None:
        zarr_format = _default_zarr_format()

    return await AsyncGroup.from_store(
        store=store_path,
        zarr_format=zarr_format,
        overwrite=overwrite,
        attributes=attributes,
    )


async def open_group(
    store: StoreLike | None = None,
    *,  # Note: this is a change from v2
    mode: AccessModeLiteral = "a",
    cache_attrs: bool | None = None,  # not used, default changed
    synchronizer: Any = None,  # not used
    path: str | None = None,
    chunk_store: StoreLike | None = None,  # not used
    storage_options: dict[str, Any] | None = None,
    zarr_format: ZarrFormat | None = None,
    meta_array: Any | None = None,  # not used
    attributes: dict[str, JSON] | None = None,
    use_consolidated: bool | str | None = None,
) -> AsyncGroup:
    """Open a group using file-mode-like semantics.

    Parameters
    ----------
    store : StoreLike or None, default=None
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    mode : {'r', 'r+', 'a', 'w', 'w-'}, optional
        Persistence mode: 'r' means read only (must exist); 'r+' means
        read/write (must exist); 'a' means read/write (create if doesn't
        exist); 'w' means create (overwrite if exists); 'w-' means create
        (fail if exists). On a store that cannot delete keys, 'w' raises an
        error instead of replacing an existing node.
    cache_attrs : bool, optional
        If True (default), user attributes will be cached for attribute read
        operations. If False, user attributes are reloaded from the store prior
        to all attribute read operations.
    synchronizer : object, optional
        Array synchronizer.
    path : str, optional
        Group path within store.
    chunk_store : StoreLike or None, default=None
        Separate storage for chunks. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    storage_options : dict
        If using an fsspec URL to create the store, these will be passed to
        the backend implementation. Ignored otherwise.
    meta_array : array-like, optional
        An array instance to use for determining arrays to create and return
        to users. Use `numpy.empty(())` by default.
    attributes : dict
        A dictionary of JSON-serializable values with user-defined attributes.
    use_consolidated : bool or str, default None
        Whether to use consolidated metadata.

        By default, consolidated metadata is used if it's present in the
        store (in the `zarr.json` for Zarr format 3 and in the `.zmetadata` file
        for Zarr format 2).

        To explicitly require consolidated metadata, set `use_consolidated=True`,
        which will raise an exception if consolidated metadata is not found.

        To explicitly *not* use consolidated metadata, set `use_consolidated=False`,
        which will fall back to using the regular, non consolidated metadata.

        Zarr format 2 allowed configuring the key storing the consolidated metadata
        (`.zmetadata` by default). Specify the custom key as `use_consolidated`
        to load consolidated metadata from a non-default key.

    Returns
    -------
    g : group
        The new group.
    """

    _warn_unimplemented_kwargs(
        {
            "cache_attrs": cache_attrs,
            "synchronizer": synchronizer,
            "meta_array": meta_array,
            "chunk_store": chunk_store,
        }
    )

    zarr_format = resolve_zarr_format(store, zarr_format)
    store_path = await make_store_path(store, mode=mode, storage_options=storage_options, path=path)
    if attributes is None:
        attributes = {}

    try:
        if mode in _READ_MODES:
            return await AsyncGroup.open(
                store_path, zarr_format=zarr_format, use_consolidated=use_consolidated
            )
    except (KeyError, FileNotFoundError):
        pass
    if mode in _CREATE_MODES:
        overwrite = _infer_overwrite(mode)
        _zarr_format = zarr_format or _default_zarr_format()
        return await AsyncGroup.from_store(
            store_path,
            zarr_format=_zarr_format,
            overwrite=overwrite,
            attributes=attributes,
        )
    msg = f"No group found in store {store!r} at path {store_path.path!r}"
    raise GroupNotFoundError(msg)


async def create(
    shape: tuple[int, ...] | int,
    *,  # Note: this is a change from v2
    chunks: ChunksLike | None = None,
    dtype: ZDTypeLike | None = None,
    compressor: CompressorLike = "auto",
    fill_value: Any | None = DEFAULT_FILL_VALUE,
    order: MemoryOrder | None = None,
    store: StoreLike | None = None,
    synchronizer: Any | None = None,
    overwrite: bool = False,
    path: PathLike | None = None,
    chunk_store: StoreLike | None = None,
    filters: Iterable[dict[str, JSON] | Numcodec] | None = None,
    cache_metadata: bool | None = None,
    cache_attrs: bool | None = None,
    read_only: bool | None = None,
    object_codec: Codec | None = None,  # TODO: type has changed
    dimension_separator: Literal[".", "/"] | None = None,
    write_empty_chunks: bool | None = None,
    zarr_format: ZarrFormat | None = None,
    meta_array: Any | None = None,  # TODO: need type
    attributes: dict[str, JSON] | None = None,
    # v3 only
    chunk_shape: ChunksLike | None = None,
    chunk_key_encoding: (
        ChunkKeyEncoding
        | tuple[Literal["default"], Literal[".", "/"]]
        | tuple[Literal["v2"], Literal[".", "/"]]
        | None
    ) = None,
    codecs: Iterable[Codec | dict[str, JSON]] | None = None,
    dimension_names: DimensionNamesLike = None,
    storage_options: dict[str, Any] | None = None,
    config: ArrayConfigLike | None = None,
    mode: AccessModeLiteral | None = None,
    data: npt.ArrayLike | None = None,
) -> AnyAsyncArray:
    """Create an array.

    Parameters
    ----------
    shape : int or tuple of ints
        Array shape.
    chunks : ChunksLike, optional
        Chunk shape. If None (the default), it is guessed from `shape` and `dtype`. If
        False, it is set to `shape`, i.e., a single chunk for the whole array. If an
        int, the chunk size in each dimension is given by the value of `chunks`.
        `True` is not a chunk shape and raises a `ValueError`.
    dtype : str or dtype, optional
        NumPy dtype.
    compressor : Codec, optional
        Primary compressor to compress chunk data.
        Zarr format 2 only. Zarr format 3 arrays should use `codecs` instead.

        If neither `compressor` nor `filters` are provided, the default compressor
        [`zarr.codecs.ZstdCodec`][] is used.

        If `compressor` is set to `None`, no compression is used.
    fill_value : Any, optional
        Fill value for the array.
    order : {'C', 'F'}, optional
        Deprecated in favor of the `config` keyword argument.
        Pass `{'order': <value>}` to `create` instead of using this parameter.
        Memory layout to be used within each chunk.
        If not specified, the `array.order` parameter in the global config will be used.
    store : StoreLike or None, default=None
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    synchronizer : object, optional
        Array synchronizer.
    overwrite : bool, optional
        If True, delete all pre-existing data in `store` at `path` before
        creating the array.
    path : str, optional
        Path under which array is stored.
    chunk_store : StoreLike or None, default=None
        Separate storage for chunks. If not provided, `store` will be used
        for storage of both chunks and metadata.
    filters : Iterable[Codec] | Literal["auto"], optional
        Iterable of filters to apply to each chunk of the array, in order, before serializing that
        chunk to bytes.

        For Zarr format 3, a "filter" is a codec that takes an array and returns an array,
        and these values must be instances of [`zarr.abc.codec.ArrayArrayCodec`][], or a
        dict representations of [`zarr.abc.codec.ArrayArrayCodec`][].

        For Zarr format 2, a "filter" can be any numcodecs codec; you should ensure that the
        order of your filters is consistent with the behavior of each filter.

        The default value of `"auto"` instructs Zarr to use a default based on the data
        type of the array and the Zarr format specified. For all data types in Zarr V3, and most
        data types in Zarr V2, the default filters are empty. The only cases where default filters
        are not empty is when the Zarr format is 2, and the data type is a variable-length data type like
        [`zarr.dtype.VariableLengthUTF8`][] or [`zarr.dtype.VariableLengthBytes`][]. In these cases,
        the default filters contains a single element which is a codec specific to that particular data type.

        To create an array with no filters, provide an empty iterable or the value `None`.
    cache_metadata : bool, optional
        If True, array configuration metadata will be cached for the
        lifetime of the object. If False, array metadata will be reloaded
        prior to all data access and modification operations (may incur
        overhead depending on storage and data access pattern).
    cache_attrs : bool, optional
        If True (default), user attributes will be cached for attribute read
        operations. If False, user attributes are reloaded from the store prior
        to all attribute read operations.
    read_only : bool, optional
        True if array should be protected against modification.
    object_codec : Codec, optional
        A codec to encode object arrays, only needed if dtype=object.
    dimension_separator : {'.', '/'}, optional
        Separator placed between the dimensions of a chunk.
        Zarr format 2 only. Zarr format 3 arrays should use `chunk_key_encoding` instead.
    write_empty_chunks : bool, optional
        Deprecated in favor of the `config` keyword argument.
        Pass `{'write_empty_chunks': <value>}` to `create` instead of using this parameter.
        If True, all chunks will be stored regardless of their
        contents. If False, each chunk is compared to the array's fill value
        prior to storing. If a chunk is uniformly equal to the fill value, then
        that chunk is not be stored, and the store entry for that chunk's key
        is deleted.
    zarr_format : {2, 3, None}, optional
        The Zarr format to use when creating an array. The default is `None`,
        which instructs Zarr to choose the default Zarr format value defined in the
        runtime configuration.
    meta_array : array-like, optional
        Not implemented.
    attributes : dict[str, JSON], optional
        A dictionary of user attributes to store with the array.
    chunk_shape : ChunksLike, optional
        The shape of the Array's chunks (default is None).
        Zarr format 3 only. Zarr format 2 arrays should use `chunks` instead.
    chunk_key_encoding : ChunkKeyEncoding, optional
        A specification of how the chunk keys are represented in storage.
        Zarr format 3 only. Zarr format 2 arrays should use `dimension_separator` instead.
        Default is `("default", "/")`.
    codecs : Sequence of Codecs or dicts, optional
        An iterable of Codec or dict serializations of Codecs. Zarr V3 only.

        The elements of `codecs` specify the transformation from array values to stored bytes.
        Zarr format 3 only. Zarr format 2 arrays should use `filters` and `compressor` instead.

        If no codecs are provided, default codecs will be used based on the data type of the array.
        For most data types, the default codecs are the tuple `(BytesCodec(), ZstdCodec())`;
        data types that require a special [`zarr.abc.codec.ArrayBytesCodec`][], like variable-length strings or bytes,
        will use the [`zarr.abc.codec.ArrayBytesCodec`][] required for the data type instead of [`zarr.codecs.BytesCodec`][].
    dimension_names : Iterable[str | None] | None = None
        An iterable of dimension names. Zarr format 3 only.
    storage_options : dict
        If using an fsspec URL to create the store, these will be passed to
        the backend implementation. Ignored otherwise.
    config : ArrayConfigLike, optional
        Runtime configuration of the array. If provided, will override the
        default values from `zarr.config.array`.
    mode : {'r', 'r+', 'a', 'w', 'w-'}, optional
        Legacy way to control overwriting, kept for compatibility with Zarr-Python 2.
        Prefer `overwrite`. The access mode used to open `store`; the default, `None`,
        is `'a'`.

        - `'a'` and `'r+'` create the array and fail if a node exists at `path`,
          unless `overwrite` is `True`.
        - `'w'` replaces anything stored under `path` (the whole store if `path` is
          not set), even if `overwrite` is `False`. On a store that cannot delete
          keys, `'w'` raises an error instead of replacing an existing node.
        - `'w-'` fails if anything is stored under `path`, even if `overwrite` is
          `True`.
        - `'r'` always fails.

        If `store` is a `StorePath`, `mode` is not validated against it: `'w'` still
        sets `overwrite`, and the other modes have no effect.
    data : array-like, optional
        Values written into the new array after it is created. Unlike the `data`
        parameter of `create_array`, it does not set the shape or data type of the
        array. To create an array from existing data, use `create_array(data=...)`.
        A Zarr array is not supported as `data`.

    Returns
    -------
    z : array
        The array.
    """
    _warn_unimplemented_kwargs(
        {
            "synchronizer": synchronizer,
            "chunk_store": chunk_store,
            "cache_metadata": cache_metadata,
            "cache_attrs": cache_attrs,
            "object_codec": object_codec,
            "read_only": read_only,
            "meta_array": meta_array,
        }
    )

    if write_empty_chunks is not None:
        _warn_write_empty_chunks_kwarg()

    if mode is None:
        mode = "a"
    overwrite = overwrite or _infer_overwrite(mode)
    zarr_format = resolve_zarr_format(store, zarr_format)
    store_path = await make_store_path(store, path=path, mode=mode, storage_options=storage_options)
    if zarr_format is None:
        zarr_format = _default_zarr_format()

    config_parsed = parse_array_config(config)

    if write_empty_chunks is not None:
        if config is not None:
            msg = (
                "Both write_empty_chunks and config keyword arguments are set. "
                "This is redundant. When both are set, write_empty_chunks will be used instead "
                "of the value in config."
            )
            warnings.warn(ZarrUserWarning(msg), stacklevel=1)
        config_parsed = dataclasses.replace(config_parsed, write_empty_chunks=write_empty_chunks)

    return await AsyncArray._create(
        store_path,
        shape=shape,
        chunks=chunks,
        # Legacy v2 behavior: an unspecified dtype defaults to float64.
        dtype="float64" if dtype is None else dtype,
        compressor=compressor,
        fill_value=fill_value,
        overwrite=overwrite,
        filters=filters,
        dimension_separator=dimension_separator,
        order=order,
        zarr_format=zarr_format,
        chunk_shape=chunk_shape,
        chunk_key_encoding=chunk_key_encoding,
        codecs=codecs,
        dimension_names=dimension_names,
        attributes=attributes,
        config=config_parsed,
        data=data,
    )


async def empty(shape: tuple[int, ...], **kwargs: Any) -> AnyAsyncArray:
    """Create an empty array with the specified shape. The contents will be filled with the
    specified fill value or zeros if no fill value is provided.

    Parameters
    ----------
    shape : int or tuple of int
        Shape of the empty array.
    **kwargs
        Keyword arguments passed to [`create`][zarr.api.asynchronous.create].

    Notes
    -----
    The contents of an empty Zarr array are not defined. On attempting to
    retrieve data from an empty Zarr array, any values may be returned,
    and these are not guaranteed to be stable from one access to the next.
    """
    return await create(shape=shape, **kwargs)


async def empty_like(
    a: ArrayLike, *, zarr_format: ZarrFormat | None = None, **kwargs: Any
) -> AnyAsyncArray:
    """Create an empty array like `a`. The contents will be filled with the
    array's fill value or zeros if no fill value is provided.

    Parameters
    ----------
    a : array-like
        The array to create an empty array like.
    zarr_format : {2, 3, None}, optional
        The zarr format of the new array. If `None` (default), the zarr format of `a` if it
        is a zarr array, otherwise the default zarr format.
    **kwargs
        Keyword arguments passed to [`create`][zarr.api.asynchronous.create].

    Returns
    -------
    Array
        The new array.

    Notes
    -----
    The contents of an empty Zarr array are not defined. On attempting to
    retrieve data from an empty Zarr array, any values may be returned,
    and these are not guaranteed to be stable from one access to the next.
    """
    like_kwargs = _like_args(a, zarr_format) | kwargs
    return await empty(**like_kwargs)  # type: ignore[arg-type]


# TODO: add type annotations for fill_value and kwargs
async def full(shape: tuple[int, ...], fill_value: Any, **kwargs: Any) -> AnyAsyncArray:
    """Create an array, with `fill_value` being used as the default value for
    uninitialized portions of the array.

    Parameters
    ----------
    shape : int or tuple of int
        Shape of the empty array.
    fill_value : scalar
        Fill value.
    **kwargs
        Keyword arguments passed to [`create`][zarr.api.asynchronous.create].

    Returns
    -------
    Array
        The new array.
    """
    return await create(shape=shape, fill_value=fill_value, **kwargs)


# TODO: add type annotations for kwargs
async def full_like(
    a: ArrayLike, *, zarr_format: ZarrFormat | None = None, **kwargs: Any
) -> AnyAsyncArray:
    """Create a filled array like `a`.

    Parameters
    ----------
    a : array-like
        The array to create an empty array like.
    zarr_format : {2, 3, None}, optional
        The zarr format of the new array. If `None` (default), the zarr format of `a` if it
        is a zarr array, otherwise the default zarr format.
    **kwargs
        Keyword arguments passed to [`zarr.api.asynchronous.create`][].

    Returns
    -------
    Array
        The new array.
    """
    like_kwargs = _like_args(a, zarr_format) | kwargs
    return await full(**like_kwargs)  # type: ignore[arg-type]


async def ones(shape: tuple[int, ...], **kwargs: Any) -> AnyAsyncArray:
    """Create an array, with one being used as the default value for
    uninitialized portions of the array.

    Parameters
    ----------
    shape : int or tuple of int
        Shape of the empty array.
    **kwargs
        Keyword arguments passed to [`zarr.api.asynchronous.create`][].

    Returns
    -------
    Array
        The new array.
    """
    return await create(shape=shape, fill_value=1, **kwargs)


async def ones_like(
    a: ArrayLike, *, zarr_format: ZarrFormat | None = None, **kwargs: Any
) -> AnyAsyncArray:
    """Create an array of ones like `a`.

    Parameters
    ----------
    a : array-like
        The array to create an empty array like.
    zarr_format : {2, 3, None}, optional
        The zarr format of the new array. If `None` (default), the zarr format of `a` if it
        is a zarr array, otherwise the default zarr format.
    **kwargs
        Keyword arguments passed to [`zarr.api.asynchronous.create`][].

    Returns
    -------
    Array
        The new array.
    """
    like_args = _like_args(a, zarr_format)
    # `ones` supplies its own fill_value, so drop any inherited from `a`.
    like_args.pop("fill_value", None)
    like_kwargs = like_args | kwargs
    return await ones(**like_kwargs)  # type: ignore[arg-type]


async def open_array(
    *,  # note: this is a change from v2
    store: StoreLike | None = None,
    zarr_format: ZarrFormat | None = None,
    path: PathLike = "",
    storage_options: dict[str, Any] | None = None,
    **kwargs: Any,  # TODO: type kwargs as valid args to save
) -> AnyAsyncArray:
    """Open an array using file-mode-like semantics.

    Parameters
    ----------
    store : StoreLike
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    zarr_format : {2, 3, None}, optional
        The zarr format to use when saving.
    path : str, optional
        Path in store to array.
    storage_options : dict
        If using an fsspec URL to create the store, these will be passed to
        the backend implementation. Ignored otherwise.
    **kwargs
        Any keyword arguments to pass to [`create`][zarr.api.asynchronous.create].

    Returns
    -------
    AsyncArray
        The opened array.
    """

    mode = kwargs.pop("mode", None)
    zarr_format = resolve_zarr_format(store, zarr_format)
    store_path = await make_store_path(store, path=path, mode=mode, storage_options=storage_options)

    if "write_empty_chunks" in kwargs:
        _warn_write_empty_chunks_kwarg()

    # mode "w" replaces any existing array, so there is nothing to open
    if mode != "w":
        try:
            return await AsyncArray.open(store_path, zarr_format=zarr_format)
        except FileNotFoundError as err:
            if store_path.read_only or mode not in _CREATE_MODES:
                msg = f"No array found in store {store_path.store} at path {store_path.path}"
                raise ArrayNotFoundError(msg) from err
    return await create(
        store=store_path,
        zarr_format=zarr_format or _default_zarr_format(),
        overwrite=_infer_overwrite(mode),
        **kwargs,
    )


async def open_like(
    a: ArrayLike, path: str, *, zarr_format: ZarrFormat | None = None, **kwargs: Any
) -> AnyAsyncArray:
    """Open a persistent array like `a`.

    Parameters
    ----------
    a : Array
        The shape and data-type of a define these same attributes of the returned array.
    path : str
        The path to the new array.
    zarr_format : {2, 3, None}, optional
        The zarr format of the array to open or create. If `None` (default), an existing
        array of either format is opened, and a missing one is created in the default zarr
        format. The zarr format of `a` is not inherited.
    **kwargs
        Additional keyword arguments passed to `open_array`.
        If `mode` is omitted or `None`, it defaults to `"a"`. Pass `mode="r"` when
        opening an existing array from a read-only store.

    Returns
    -------
    AsyncArray
        The opened array.
    """
    # The zarr format of `a` is not inherited: `open_array` would then look for an existing
    # array in only that format.
    like_args = _like_args(a, zarr_format or _default_zarr_format())
    like_args.pop("zarr_format")
    like_kwargs = like_args | kwargs
    if like_kwargs.get("mode") is None:
        like_kwargs["mode"] = "a"
    return await open_array(path=path, zarr_format=zarr_format, **like_kwargs)  # type: ignore[arg-type]


async def zeros(shape: tuple[int, ...], **kwargs: Any) -> AnyAsyncArray:
    """Create an array, with zero being used as the default value for
    uninitialized portions of the array.

    Parameters
    ----------
    shape : int or tuple of int
        Shape of the empty array.
    **kwargs
        Keyword arguments passed to [`zarr.api.asynchronous.create`][].

    Returns
    -------
    Array
        The new array.
    """
    return await create(shape=shape, fill_value=0, **kwargs)


async def zeros_like(
    a: ArrayLike, *, zarr_format: ZarrFormat | None = None, **kwargs: Any
) -> AnyAsyncArray:
    """Create an array of zeros like `a`.

    Parameters
    ----------
    a : array-like
        The array to create an empty array like.
    zarr_format : {2, 3, None}, optional
        The zarr format of the new array. If `None` (default), the zarr format of `a` if it
        is a zarr array, otherwise the default zarr format.
    **kwargs
        Keyword arguments passed to [`create`][zarr.api.asynchronous.create].

    Returns
    -------
    Array
        The new array.
    """
    like_args = _like_args(a, zarr_format)
    # `zeros` supplies its own fill_value, so drop any inherited from `a`.
    like_args.pop("fill_value", None)
    like_kwargs = like_args | kwargs
    return await zeros(**like_kwargs)  # type: ignore[arg-type]
