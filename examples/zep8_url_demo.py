# /// script
# dependencies = [
#     "zarr",
#     "numpy",
#     "fsspec",
# ]
# ///
"""
ZEP 8 URL Syntax Demo

This script demonstrates ZEP 8 URL syntax for chained store access in zarr-python.
ZEP 8 URLs allow you to chain different storage adapters using pipe (|) syntax.

Examples of ZEP 8 URLs:
    - "memory:" - Simple in-memory store
    - "file:/path/data.zip|zip:" - ZIP file access
    - "s3://bucket/data.zip|zip:|zarr3:" - Cloud ZIP with zarr3 format
    - "file:/path/repo|icechunk:branch:main" - Icechunk repository (if available)

For comprehensive Icechunk integration examples, see the icechunk repository tests.
"""

import tempfile
from pathlib import Path

import numpy as np

import zarr
from zarr.storage import ZipStore
from zarr.storage._zep8 import URLParser, is_zep8_url


def demo_basic_zep8() -> None:
    """Demonstrate basic ZEP 8 URL syntax features."""
    print("=== Basic ZEP 8 URL Demo ===")

    print("📝 Testing basic ZEP 8 URL formats")

    # Memory store
    print("\n1. Memory store:")
    memory_url = "memory:"
    root = zarr.open_group(memory_url, mode="w")
    arr = root.create_array("test_data", shape=(10,), dtype="f4")
    arr[:] = np.random.random(10)
    print(f"✅ Created array via {memory_url}")
    print(f"   Data shape: {arr.shape}, dtype: {arr.dtype}")

    # File store
    print("\n2. File store:")
    with tempfile.TemporaryDirectory() as tmpdir:
        file_url = f"file:{tmpdir}/test.zarr"
        root2 = zarr.open_group(file_url, mode="w")
        arr2 = root2.create_array("persistent_data", shape=(20,), dtype="i4")
        arr2[:] = range(20)
        print(f"✅ Created array via {file_url}")
        print(f"   Data: {list(arr2[:5])}... (first 5 elements)")


def demo_zip_chaining() -> None:
    """Demonstrate ZIP file chaining with ZEP 8 URLs."""
    print("\n=== ZIP Chaining Demo ===")

    print("📝 Creating ZIP file with zarr data, then accessing via ZEP 8 URL")

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / "data.zip"

        # Step 1: Create ZIP file with zarr data
        print(f"Creating ZIP file at: {zip_path}")
        with ZipStore(str(zip_path), mode="w") as zip_store:
            root = zarr.open_group(zip_store, mode="w")

            # Create sample datasets
            temps = root.create_array("temperatures", shape=(365,), dtype="f4")
            temp_data = (
                20 + 10 * np.sin(np.arange(365) * 2 * np.pi / 365) + np.random.normal(0, 2, 365)
            )
            temps[:] = temp_data
            temps.attrs["units"] = "celsius"
            temps.attrs["description"] = "Daily temperature readings"

            metadata = root.create_group("metadata")
            info = metadata.create_array("info", shape=(1,), dtype="U50")
            info[0] = "Weather data from ZIP demo"

            print("✅ Created temperature data in ZIP file")
            print(f"   Temperature range: {temps[:].min():.1f}°C to {temps[:].max():.1f}°C")

        # Step 2: Access via ZEP 8 URL syntax
        print("\nAccessing ZIP data via ZEP 8 URL")
        zip_url = f"file:{zip_path}|zip:"
        root_read = zarr.open_group(zip_url, mode="r")

        temps_read = root_read["temperatures"]
        info_read = root_read["metadata/info"]

        print(f"✅ Successfully read via URL: {zip_url}")
        print(f"   Temperature units: {temps_read.attrs['units']}")
        print(f"   Description: {temps_read.attrs['description']}")
        print(f"   Metadata: {info_read[0]}")
        print(f"   Data integrity: {np.array_equal(temp_data, temps_read[:])}")


def demo_url_parsing() -> None:
    """Demonstrate ZEP 8 URL parsing and validation."""
    print("\n=== URL Parsing Demo ===")

    parser = URLParser()

    test_urls = [
        "memory:",
        "file:/tmp/data.zarr",
        "file:/tmp/data.zip|zip:",
        "s3://bucket/data.zip|zip:|zarr3:",
        "memory:|icechunk:branch:main",  # This would be rejected by icechunk adapter
        "/regular/file/path",  # Not a ZEP 8 URL
    ]

    print("📝 Testing URL parsing:")

    for url in test_urls:
        is_zep8 = is_zep8_url(url)
        print(f"\n  URL: {url}")
        print(f"  ZEP 8: {is_zep8}")

        if is_zep8:
            try:
                segments = parser.parse(url)
                print(f"  Segments: {len(segments)}")
                for i, seg in enumerate(segments):
                    scheme_part = f"scheme={seg.scheme}" if seg.scheme else ""
                    adapter_part = f"adapter={seg.adapter}" if seg.adapter else ""
                    path_part = f"path='{seg.path}'" if seg.path else ""
                    parts = [p for p in [scheme_part, adapter_part, path_part] if p]
                    print(f"    {i}: {', '.join(parts)}")
            except Exception as e:
                print(f"    Parse error: {e}")


def demo_error_cases() -> None:
    """Demonstrate common error cases and their handling."""
    print("\n=== Error Handling Demo ===")

    print("🚫 Testing error cases:")

    # Test 1: Invalid URL format
    print("\n1. Invalid URL formats:")
    invalid_urls = [
        "|invalid:start",  # Starts with pipe
        "memory:|",  # Ends with pipe
        "memory:||zip:",  # Double pipe
        "",  # Empty URL
    ]

    for url in invalid_urls:
        try:
            zarr.open_group(url, mode="r")
            print(f"❌ Should have failed: {url}")
        except Exception as e:
            print(f"✅ Correctly rejected: {url} -> {type(e).__name__}")

    # Test 2: Unknown adapters
    print("\n2. Unknown adapters:")
    try:
        zarr.open_group("memory:|unknown_adapter:", mode="r")
        print("❌ Should have failed: unknown adapter")
    except Exception as e:
        print(f"✅ Correctly rejected unknown adapter -> {type(e).__name__}")

    # Test 3: Fallback behavior
    print("\n3. Fallback to regular stores:")
    regular_urls = ["memory:", f"file:{tempfile.mkdtemp()}/fallback.zarr"]

    for url in regular_urls:
        try:
            root = zarr.open_group(url, mode="w")
            arr = root.create_array("data", shape=(5,), dtype="i4")
            arr[:] = [1, 2, 3, 4, 5]
            print(f"✅ Fallback works: {url}")
        except Exception as e:
            print(f"❌ Fallback failed: {url} -> {e}")


if __name__ == "__main__":
    print("ZEP 8 URL Syntax Demo")
    print("=" * 30)

    demo_basic_zep8()
    demo_zip_chaining()
    demo_url_parsing()
    demo_error_cases()

    print("\n" + "=" * 30)
    print("Demo completed!")
    print("\nZEP 8 URL syntax enables flexible chaining of storage adapters.")
    print("For adapter-specific examples (like Icechunk), see the respective")
    print("package repositories and their test suites.")
