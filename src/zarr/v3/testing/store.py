import pytest

from zarr.v3.abc.store import Store


class StoreTests:
    store_cls: type[Store]

    @pytest.fixture(scope="function")
    def store(self) -> Store:
        return self.store_cls()

    def test_store_type(self, store: Store) -> None:
        assert isinstance(store, Store)
        assert isinstance(store, self.store_cls)

    def test_store_repr(self, store: Store) -> None:
        assert repr(store)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("key", ["c/0", "foo/c/0.0", "foo/0/0"])
    @pytest.mark.parametrize("data", [b"\x01\x02\x03\x04", b""])
    async def test_set_get_bytes_roundtrip(self, store: Store, key: str, data: bytes) -> None:
        await store.set(key, data)
        assert await store.get(key) == data

    # @pytest.mark.parametrize("key, data, error, error_msg", [
    #     ("", b"\x01\x02\x03\x04", ValueError, "invalid key")
    # ])
    # async def test_set_raises(self, store, key: Any, data: Any) -> None:
    #     with pytest.raises(TypeError):
    #         await store.set(key, data)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("key", ["foo/c/0"])
    @pytest.mark.parametrize("data", [b"\x01\x02\x03\x04", b""])
    async def test_get_partial_values(self, store: Store, key: str, data: bytes) -> None:
        # put all of the data
        await store.set(key, data)
        # read back just part of it
        vals = await store.get_partial_values([(key, (0, 2))])
        assert vals == [data[0:2]]

        # read back multiple parts of it at once
        vals = await store.get_partial_values([(key, (0, 2)), (key, (2, 4))])
        assert vals == [data[0:2], data[2:4]]
