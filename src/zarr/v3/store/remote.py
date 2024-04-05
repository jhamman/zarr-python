from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union, List

from zarr.v3.abc.store import Store
from zarr.v3.store.core import _dereference_path
from zarr.v3.common import BytesLike


if TYPE_CHECKING:
    from upath import UPath
    from fsspec.asyn import AsyncFileSystem


# TODOs:
# * cache fsspec objects after instantiation


class RemoteStore(Store):
    supports_writes: bool = True
    supports_partial_writes: bool = False
    supports_listing: bool = True

    root: UPath

    def __init__(self, url: Union[UPath, str], **storage_options: Dict[str, Any]):
        from upath import UPath
        import fsspec

        if isinstance(url, str):
            self.root = UPath(url, **storage_options)
        else:
            assert len(storage_options) == 0, (
                "If constructed with a UPath object, no additional "
                + "storage_options are allowed."
            )
            self.root = url.rstrip("/")
        # test instantiate file system
        fs, _ = fsspec.core.url_to_fs(str(self.root), asynchronous=True, **self.root._kwargs)
        assert fs.__class__.async_impl, "FileSystem needs to support async operations."

    def __str__(self) -> str:
        return str(self.root)

    def __repr__(self) -> str:
        return f"RemoteStore({repr(str(self))})"

    def _make_fs(self) -> Tuple[AsyncFileSystem, str]:
        import fsspec

        storage_options = self.root._kwargs.copy()
        storage_options.pop("_url", None)
        fs, root = fsspec.core.url_to_fs(str(self.root), asynchronous=True, **self.root._kwargs)
        assert fs.__class__.async_impl, "FileSystem needs to support async operations."
        return fs, root

    async def get(
        self, key: str, byte_range: Optional[Tuple[int, Optional[int]]] = None
    ) -> Optional[BytesLike]:
        assert isinstance(key, str)
        fs, root = self._make_fs()
        path = _dereference_path(root, key)

        try:
            value = await (
                fs._cat_file(path, start=byte_range[0], end=byte_range[1])
                if byte_range
                else fs._cat_file(path)
            )
        except (FileNotFoundError, IsADirectoryError, NotADirectoryError):
            return None

        return value

    async def set(
        self, key: str, value: BytesLike, byte_range: Optional[Tuple[int, int]] = None
    ) -> None:
        assert isinstance(key, str)
        fs, root = self._make_fs()
        path = _dereference_path(root, key)

        # write data
        if byte_range:
            with fs._open(path, "r+b") as f:
                f.seek(byte_range[0])
                f.write(value)
        else:
            await fs._pipe_file(path, value)

    async def delete(self, key: str) -> None:
        fs, root = self._make_fs()
        path = _dereference_path(root, key)
        if await fs._exists(path):
            await fs._rm(path)

    async def exists(self, key: str) -> bool:
        fs, root = self._make_fs()
        path = _dereference_path(root, key)
        return await fs._exists(path)

    async def list(self) -> List[str]:
        fs, root = self._make_fs()
        keys = await fs._glob(root + "/*")
        print(keys)
        return [k.replace(root + "/", "") for k in keys]

    async def list_prefix(self, prefix: str) -> List[str]:
        return [key for key in self._store_dict if key.startswith(prefix)]

    async def list_dir(self, prefix: str) -> List[str]:
        if prefix == "":
            return list({key.split("/", maxsplit=1)[0] for key in self._store_dict})
        else:
            return list(
                {
                    key.strip(prefix + "/").split("/")[0]
                    for key in self._store_dict
                    if (key.startswith(prefix + "/") and key != prefix)
                }
            )

    async def set_partial_values(self, key_start_values: List[Tuple[str, int, bytes]]) -> None:
        raise NotImplementedError

    async def get_partial_values(
        self, key_ranges: List[Tuple[str, Tuple[int, int]]]
    ) -> List[bytes]:
        return [await self.get(key, byte_range=byte_range) for key, byte_range in key_ranges]
