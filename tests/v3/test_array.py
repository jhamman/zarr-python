import pickle

import numpy as np
import pytest

from zarr.array import Array, AsyncArray
from zarr.common import ZarrFormat
from zarr.group import Group
from zarr.store import LocalStore, MemoryStore


@pytest.mark.parametrize("store", ("local", "memory"), indirect=["store"])
@pytest.mark.parametrize("zarr_format", (2, 3))
def test_array_name_properties_no_group(
    store: LocalStore | MemoryStore, zarr_format: ZarrFormat
) -> None:
    arr = Array.create(store=store, shape=(100,), chunks=(10,), zarr_format=zarr_format, dtype="i4")
    assert arr.path == ""
    assert arr.name is None
    assert arr.basename is None


@pytest.mark.parametrize("store", ("local", "memory"), indirect=["store"])
@pytest.mark.parametrize("zarr_format", (2, 3))
def test_array_name_properties_with_group(
    store: LocalStore | MemoryStore, zarr_format: ZarrFormat
) -> None:
    root = Group.create(store=store, zarr_format=zarr_format)
    foo = root.create_array("foo", shape=(100,), chunks=(10,), dtype="i4")
    assert foo.path == "foo"
    assert foo.name == "/foo"
    assert foo.basename == "foo"

    bar = root.create_group("bar")
    spam = bar.create_array("spam", shape=(100,), chunks=(10,), dtype="i4")

    assert spam.path == "bar/spam"
    assert spam.name == "/bar/spam"
    assert spam.basename == "spam"


@pytest.mark.parametrize("store", ("local",), indirect=["store"])
@pytest.mark.parametrize("zarr_format", (2, 3))
async def test_serizalizable_async_array(
    store: LocalStore | MemoryStore, zarr_format: ZarrFormat
) -> None:
    expected = await AsyncArray.create(
        store=store, shape=(100,), chunks=(10,), zarr_format=zarr_format, dtype="i4"
    )
    # await expected.setitems(list(range(100)))

    p = pickle.dumps(expected)
    actual = pickle.loads(p)

    assert actual == expected
    # np.testing.assert_array_equal(await actual.getitem(slice(None)), await expected.getitem(slice(None)))
    # TODO: uncomment the parts of this test that will be impacted by the config/prototype changes in flight


@pytest.mark.parametrize("store", ("local",), indirect=["store"])
@pytest.mark.parametrize("zarr_format", (2, 3))
def test_serizalizable_sync_array(store: LocalStore, zarr_format: ZarrFormat) -> None:
    expected = Array.create(
        store=store, shape=(100,), chunks=(10,), zarr_format=zarr_format, dtype="i4"
    )
    expected[:] = list(range(100))

    p = pickle.dumps(expected)
    actual = pickle.loads(p)

    assert actual == expected
    np.testing.assert_array_equal(actual[:], expected[:])
