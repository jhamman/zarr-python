"""
Parsing and resolution of URL pipelines (https://github.com/jbms/url-pipeline).

Importing this module is cheap and has no side effects: third-party adapters
(registered through the `zarr.url_adapters` entry-point group) are loaded
only when a pipeline URL naming their scheme is actually resolved.

A string is never interpreted as a pipeline on its own: the user wraps it
in a [`URLPipeline`][zarr.storage.URLPipeline] (or calls `zarr.open_url`),
and that object is what the `StoreLike` machinery resolves. Plain string
store specifications, including local paths that happen to contain `|`,
keep their pre-existing meaning. Percent-escapes are decoded in zarr's
native `file:` and `memory:` roots, so `%7C` spells a literal `|` in a
local path; adapter segments receive their bodies verbatim.

As the specification requires, the root sub-URL carries a URL scheme: a
local path is spelled as an absolute `file:` URL.
"""

from __future__ import annotations

import dataclasses
import re
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote

from zarr.abc.url_pipeline import (
    AdapterResolution,
    PipelineContext,
    PipelineSegment,
)
from zarr.errors import URLPipelineError
from zarr.registry import get_url_adapter, list_url_adapter_schemes
from zarr.storage._memory import ManagedMemoryStore
from zarr.storage._utils import _join_paths, normalize_path

if TYPE_CHECKING:
    from zarr.abc.store import Store
    from zarr.core.common import AccessModeLiteral, ZarrFormat

__all__ = ["URLPipeline"]

# Format segments are consumed by zarr-python itself rather than by a
# registered adapter: they select the zarr format of the node the pipeline
# addresses, and their body is a path to that node.
_FORMAT_SCHEMES: dict[str, ZarrFormat] = {"zarr2": 2, "zarr3": 3}

# Adapter scheme per RFC 3986 plus "." to permit vendor-prefixed
# nonstandard schemes (e.g. "earthmover.myscheme").
_SCHEME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.\-]*$")

# Root scheme at the very start of the sub-URL.
_ROOT_SCHEME_RE = re.compile(r"([a-zA-Z][a-zA-Z0-9+.\-]*):")

# Schemes zarr resolves natively at the pipeline root. These are never
# dispatched to a registered root adapter, so an installed package cannot
# intercept zarr's own local-path and in-memory routing.
_NATIVE_ROOT_SCHEMES = frozenset({"file", "memory"})

# An absolute Windows drive path (C:\... or C:/...), which counts as an
# absolute path in a `file:` pipeline root.
_WINDOWS_DRIVE_RE = re.compile(r"[A-Za-z]:[/\\]")


def _root_scheme(root_sub_url: str) -> str:
    """
    Detect the scheme of the root sub-URL, or `""` when it has none.

    Scheme extraction happens on the raw string, so nothing `urlparse`
    would strip or reject can desync the detected scheme from the body. A
    single letter followed by `:` is a scheme on every platform: a Windows
    drive path is spelled `file:/C:/...` in a pipeline, never bare.
    """
    match = _ROOT_SCHEME_RE.match(root_sub_url)
    if match is None:
        return ""
    return match.group(1).lower()


def _split_query(sub_url: str) -> tuple[str, str | None]:
    """Split a sub-URL on the first `?`. Fragments are not supported."""
    if "#" in sub_url:
        raise URLPipelineError(
            f"URL pipeline sub-URLs do not support fragments: {sub_url!r}. "
            "Percent-encode '#' as '%23' if it is part of the path."
        )
    body, sep, query = sub_url.partition("?")
    return body, query if sep else None


def parse_pipeline(url: str) -> tuple[PipelineSegment, ...]:
    """
    Parse a URL pipeline into its `|`-delimited segments.

    The first segment is the *root* sub-URL. Subsequent segments are *adapter* sub-URLs of the form
    `scheme:body` where the trailing colon is optional when the body is
    empty (`zip` is equivalent to `zip:`).

    The root must carry a scheme; a bare local path is rejected, since the
    specification has no schemeless sub-URLs and `|` could not be escaped
    in one.

    Segment text is preserved verbatim (no case or percent-encoding
    normalization) except that schemes are lowercased.
    """
    parts = url.split("|")
    if any(not part for part in parts):
        raise URLPipelineError(f"URL pipeline contains an empty sub-URL: {url!r}")

    segments: list[PipelineSegment] = []
    for index, part in enumerate(parts):
        if index == 0:
            scheme = _root_scheme(part)
            if not scheme:
                raise URLPipelineError(
                    f"the root sub-URL {part!r} of the URL pipeline {url!r} has no URL scheme. "
                    "Spell a local path as an absolute 'file:' URL, and a literal '|' in "
                    "it as '%7C'."
                )
            body_and_scheme, query = _split_query(part)
            body = body_and_scheme[len(scheme) + 1 :]
            segments.append(PipelineSegment(scheme=scheme, body=body, query=query, raw=part))
        else:
            body_and_scheme, query = _split_query(part)
            scheme, _, body = body_and_scheme.partition(":")
            scheme = scheme.lower()
            if not _SCHEME_RE.match(scheme):
                raise URLPipelineError(
                    f"invalid adapter scheme {scheme!r} in pipeline segment {part!r}"
                )
            segments.append(PipelineSegment(scheme=scheme, body=body, query=query, raw=part))
    return tuple(segments)


@dataclasses.dataclass(frozen=True)
class URLPipeline:
    """
    A parsed URL pipeline, ready to be passed wherever a `StoreLike` is accepted.

    Construct one with [`from_url`][zarr.storage.URLPipeline.from_url] and open it
    through any `StoreLike`-taking function, or resolve it directly with
    [`resolve`][zarr.storage.URLPipeline.resolve]. Because a
    `URLPipeline` is an explicit object, no plain string store specification is
    ever interpreted as a pipeline: `zarr.open("data|x.zarr")` still addresses a
    local directory of that name, while
    `zarr.open(URLPipeline.from_url("s3://bucket/data.zip|zip:|zarr3:"))` (or the
    equivalent `zarr.open_url(...)`) resolves the pipeline through registered
    adapters.

    A pipeline is a value: instances are immutable, hashable, and compare equal
    when they hold the same segments. Nothing is resolved until the pipeline is
    opened; resolution needs the access mode and storage options of the open
    call.

    Attributes
    ----------
    segments : tuple[PipelineSegment, ...]
        Every `|`-delimited sub-URL of the pipeline, in order: the root, any
        adapter segments, and an optional trailing format segment.

    Raises
    ------
    URLPipelineError
        If there are no segments, if a format segment (`zarr2:`/`zarr3:`)
        appears anywhere but last, or if a format segment carries a query.
    """

    segments: tuple[PipelineSegment, ...]

    def __post_init__(self) -> None:
        if not self.segments:
            raise URLPipelineError("a URL pipeline needs at least one segment")
        for segment in self.segments[1:-1]:
            if segment.scheme in _FORMAT_SCHEMES:
                raise URLPipelineError(
                    f"the format segment {segment.raw!r} must be the last segment "
                    f"of the URL pipeline {str(self)!r}"
                )
        format_segment = self._format_segment
        if format_segment is not None and format_segment.query is not None:
            raise URLPipelineError(
                f"'{format_segment.scheme}:' segments do not accept a query: {format_segment.raw!r}"
            )

    @classmethod
    def from_url(cls, url: str) -> URLPipeline:
        """
        Parse a URL pipeline string.

        A URL without a `|` is a trivial pipeline consisting of its root alone.

        Raises
        ------
        URLPipelineError
            If the string cannot be parsed or the resulting pipeline is invalid
            (see the class docstring).
        """
        return cls(parse_pipeline(url))

    @property
    def _format_segment(self) -> PipelineSegment | None:
        last = self.segments[-1]
        if len(self.segments) > 1 and last.scheme in _FORMAT_SCHEMES:
            return last
        return None

    @property
    def store_segments(self) -> tuple[PipelineSegment, ...]:
        """
        The segments that address a store: the root and any adapter segments,
        i.e. `segments` without a trailing format segment.
        """
        if self._format_segment is None:
            return self.segments
        return self.segments[:-1]

    @property
    def zarr_format(self) -> ZarrFormat | None:
        """The format selected by a trailing `zarr2:`/`zarr3:` segment, or None."""
        format_segment = self._format_segment
        if format_segment is None:
            return None
        return _FORMAT_SCHEMES[format_segment.scheme]

    @property
    def path(self) -> str:
        """
        The node path given as the body of a trailing format segment,
        normalized. Empty when there is no format segment or its body is empty.
        """
        format_segment = self._format_segment
        if format_segment is None:
            return ""
        return normalize_path(format_segment.body)

    def resolve_zarr_format(self, zarr_format: ZarrFormat | None) -> ZarrFormat | None:
        """
        Combine the format selected by this pipeline with a caller-supplied one.

        Parameters
        ----------
        zarr_format : ZarrFormat | None
            The format requested by the caller, or None when unspecified.

        Returns
        -------
        ZarrFormat | None
            The pipeline's format when the caller did not specify one, otherwise
            the caller's format. None when neither is set.

        Raises
        ------
        ValueError
            If the caller's format differs from the one selected by the pipeline.
        """
        pipeline_format = self.zarr_format
        if pipeline_format is None:
            return zarr_format
        if zarr_format is not None and zarr_format != pipeline_format:
            raise ValueError(
                f"zarr_format={zarr_format} conflicts with the 'zarr{pipeline_format}:' "
                f"segment of the URL pipeline {str(self)!r}; pass zarr_format=None to use "
                "the pipeline's format"
            )
        return pipeline_format

    async def resolve(
        self,
        *,
        mode: AccessModeLiteral | None = None,
        storage_options: dict[str, Any] | None = None,
    ) -> AdapterResolution:
        """
        Resolve the pipeline into a store and a residual path.

        The residual path joins the path returned by the adapters with the
        node path of a trailing format segment (`...|zarr3:path/to/node`).
        The format itself is not part of the resolution; read it from
        `zarr_format`.

        Parameters
        ----------
        mode : AccessModeLiteral | None
            The caller's access mode. `"r"` requires a read-only store; the
            resolver enforces this on whatever the final adapter returns.
        storage_options : dict | None
            Options forwarded to the root sub-URL's store (and visible to
            adapters via the context).

        Raises
        ------
        URLPipelineError
            If a segment names a scheme with no registered adapter, names an
            adapter entry point that fails to import, or the root sub-URL
            cannot be resolved.
        TypeError
            If `storage_options` are passed to a root that does not accept
            them (`file:` and `memory:`), as for non-pipeline stores.
        OSError
            Errors from opening the root resource (e.g. a missing local
            directory in mode `"r"`) propagate unchanged, as for non-pipeline
            stores.
        """
        resolution = await _resolve(self.store_segments, mode=mode, storage_options=storage_options)
        if self.path:
            resolution = dataclasses.replace(
                resolution, path=_join_paths([normalize_path(resolution.path), self.path])
            )
        return resolution

    def __str__(self) -> str:
        return "|".join(segment.raw for segment in self.segments)


def _root_routes_to_adapter(scheme: str) -> bool:
    """
    Whether a root sub-URL with this scheme is dispatched to a registered
    root adapter. Schemes zarr resolves natively (`file:`, `memory:`) are
    excluded; the registry check inspects entry-point names only — no
    adapter code is imported here.
    """
    return scheme not in _NATIVE_ROOT_SCHEMES and scheme in list_url_adapter_schemes()


async def _resolve(
    segments: tuple[PipelineSegment, ...],
    *,
    mode: AccessModeLiteral | None,
    storage_options: dict[str, Any] | None,
) -> AdapterResolution:
    if len(segments) == 1 and not _root_routes_to_adapter(segments[0].scheme):
        return AdapterResolution(
            store=await _resolve_root(segments[0], mode=mode, storage_options=storage_options)
        )

    *preceding, last = segments
    adapter_cls = get_url_adapter(last.scheme)

    context = PipelineContext(
        preceding=tuple(preceding),
        mode=mode,
        storage_options=storage_options,
    )
    resolution = await adapter_cls.open_pipeline_segment(last, context)
    if mode == "r" and not resolution.store.read_only:
        # The caller required read-only; enforce it rather than trusting
        # the adapter to have honored context.read_only.
        try:
            read_only_store = resolution.store.with_read_only(True)
            await read_only_store._ensure_open()
        except NotImplementedError as exc:
            resolution.store.close()
            raise URLPipelineError(
                f"adapter {last.scheme!r} returned a writable store for mode 'r', "
                "and the store does not support read-only conversion via "
                ".with_read_only()"
            ) from exc
        except Exception:
            resolution.store.close()
            raise
        resolution = dataclasses.replace(resolution, store=read_only_store)
    return resolution


async def _resolve_root(
    segment: PipelineSegment,
    *,
    mode: AccessModeLiteral | None,
    storage_options: dict[str, Any] | None,
) -> Store:
    """
    Resolve the root sub-URL of a pipeline into a store.

    `memory:` and `file:` roots are resolved here with the URL pipeline
    spec's semantics (spelling equivalences, mandatory absolute `file:`
    paths, percent-escapes decoded); every other scheme (e.g. fsspec URLs)
    delegates to the existing `StoreLike` machinery unchanged.
    """
    if segment.scheme == "memory":
        return _resolve_memory_root(segment, mode=mode, storage_options=storage_options)
    from zarr.storage._common import make_store  # circular import

    if segment.scheme == "file":
        # Per the spec: file://localhost/p and file:///p are equivalent to
        # file:/p; other authorities are unsupported; relative paths are
        # forbidden; file: URLs carry no query.
        if segment.query is not None:
            raise URLPipelineError(f"'file:' pipeline roots do not accept a query: {segment.raw!r}")
        body = unquote(segment.body)
        if body.startswith("//"):
            authority, sep, rest = body[2:].partition("/")
            if authority not in ("", "localhost"):
                raise URLPipelineError(
                    f"unsupported authority {authority!r} in 'file:' pipeline "
                    f"root {segment.raw!r}; only an empty authority or "
                    "'localhost' is allowed"
                )
            body = f"/{rest}" if sep else ""
        # a file URL spells a Windows drive path as /C:/...; strip the
        # leading slash so the local-path machinery sees the drive
        if body.startswith("/") and _WINDOWS_DRIVE_RE.match(body[1:]):
            body = body[1:]
        if not (body.startswith("/") or _WINDOWS_DRIVE_RE.match(body)):
            raise URLPipelineError(
                f"'file:' pipeline roots must carry an absolute path: {segment.raw!r}"
            )
        return await make_store(f"file:{body}", mode=mode, storage_options=storage_options)

    try:
        return await make_store(segment.raw, mode=mode, storage_options=storage_options)
    except ValueError as exc:
        raise URLPipelineError(
            f"could not resolve the pipeline root {segment.raw!r}: {exc}"
        ) from exc


def _resolve_memory_root(
    segment: PipelineSegment,
    *,
    mode: AccessModeLiteral | None,
    storage_options: dict[str, Any] | None,
) -> Store:
    """
    Resolve a `memory:` pipeline root per the spec's spelling equivalences:
    `memory:` ≡ `memory:/` ≡ `memory://`, and `memory:a` ≡ `memory:/a` ≡
    `memory://a`. The first path component names the managed store; the
    remainder is a path within it.
    """
    if segment.query is not None:
        raise URLPipelineError(f"'memory:' pipeline roots do not accept a query: {segment.raw!r}")
    if storage_options:
        raise TypeError(
            "'storage_options' was provided but unused. "
            "'storage_options' is only used when the store is passed as an FSSpec URI string.",
        )
    name, _, path = unquote(segment.body).lstrip("/").partition("/")
    return ManagedMemoryStore(name=name, path=path, read_only=mode == "r")
