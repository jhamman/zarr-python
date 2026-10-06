Added `zarr.create_v3_array`, `zarr.create_v3_group`, `zarr.create_v2_array` and
`zarr.create_v2_group`: creation functions that are native to URL pipelines and to one Zarr
format each. Their keyword arguments are that format's metadata fields (for V3 `shape`,
`data_type`, `chunk_grid`, `codecs`, `chunk_key_encoding`, `fill_value`, `attributes`,
`dimension_names`, `storage_transformers`; for V2 `shape`, `dtype`, `chunks`, `fill_value`,
`order`, `dimension_separator`, `compressor`, `filters`, `attributes`), with no translation
layer between formats. Fields left at the new `zarr.AUTO` sentinel are computed from the
shape, the data type and the configuration, while `None` keeps the meaning the metadata
gives it (for example no compressor). A string location is always a URL pipeline such as
`"file:/data/example.zarr|zarr3:group/array"`, so its root must carry a scheme and a literal
`|` in a local path is spelled `%7C`; a `URLPipeline`, a `Store` or a `StorePath` is accepted
as well. Creation fails if a node already exists at the location unless `overwrite=True`.
