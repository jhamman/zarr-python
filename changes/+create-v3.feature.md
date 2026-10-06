Added `zarr.create_v3_array` and `zarr.create_v3_group`, creation functions that are native
to URL pipelines and to Zarr format 3. Their keyword arguments are the Zarr V3 metadata
fields (`shape`, `data_type`, `chunk_grid`, `codecs`, `chunk_key_encoding`, `fill_value`,
`attributes`, `dimension_names`, `storage_transformers`), with no translation layer between
formats. Fields left at the new `zarr.AUTO` sentinel are computed from the shape, the data
type and the configuration. A string location is always a URL pipeline such as
`"file:/data/example.zarr|zarr3:group/array"`, so its root must carry a scheme and a literal
`|` in a local path is spelled `%7C`; a `URLPipeline`, a `Store` or a `StorePath` is accepted
as well. Creation fails if a node already exists at the location unless `overwrite=True`.
