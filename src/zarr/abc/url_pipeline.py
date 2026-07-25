"""
Abstract base class and data model for URL pipeline adapters.

A URL pipeline is a `|`-separated chain of sub-URLs, read outer-to-inner,
as specified by https://github.com/jbms/url-pipeline. The first sub-URL (the
*root*) locates a resource using a conventional URL, and each subsequent
sub-URL names an *adapter* that reinterprets everything to its left:

    s3://bucket/data.zip|zip:path/inside|zarr3:

Third-party packages provide adapters by subclassing
[`URLPipelineAdapter`][zarr.abc.url_pipeline.URLPipelineAdapter] and
registering the class under the `zarr.url_adapters` entry-point group,
using the URL scheme as the entry-point name.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from zarr.abc.store import Store
    from zarr.core.common import AccessModeLiteral, ZarrFormat

__all__ = [
    "AdapterResolution",
    "PipelineContext",
    "PipelineSegment",
    "URLPipelineAdapter",
]


@dataclass(frozen=True)
class PipelineSegment:
    """
    One `|`-delimited sub-URL of a URL pipeline.

    Attributes
    ----------
    scheme : str
        The lowercased URL scheme. Empty string only for a schemeless root
        (a bare local path).
    body : str
        The text after `scheme:` and before any `?`. Interpretation is
        scheme-defined; it is **not** URL-normalized, so case-significant
        content (e.g. icechunk snapshot IDs) is preserved.
    query : str | None
        The raw query string after `?`, or None. Interpretation is
        scheme-defined.
    raw : str
        The exact original sub-URL text, preserved for lossless
        reconstruction of the pipeline.
    """

    scheme: str
    body: str
    query: str | None
    raw: str

    def __str__(self) -> str:
        return self.raw


@dataclass(frozen=True)
class AdapterResolution:
    """
    The result of resolving a URL pipeline (or a prefix of one).

    Attributes
    ----------
    store : Store
        The resolved store.
    path : str
        Residual path *within* the store that the pipeline addresses
        (e.g. `"path/to/node"` for `...|icechunk://tag.v1/path/to/node`).
        Empty string when the pipeline addresses the store root.
    zarr_format : ZarrFormat | None
        Zarr format selected by a format segment (`zarr2:`/`zarr3:`),
        or None if unspecified.
    """

    store: Store
    path: str = ""
    zarr_format: ZarrFormat | None = None


@dataclass(frozen=True)
class PipelineContext:
    """
    Context handed to a [`URLPipelineAdapter`][zarr.abc.url_pipeline.URLPipelineAdapter]
    describing the pipeline to the left of its segment.

    Attributes
    ----------
    preceding : tuple[PipelineSegment, ...]
        The parsed sub-URLs to the left of the adapter's segment, outer to
        inner. Empty when the adapter's segment is the pipeline root.
    mode : AccessModeLiteral | None
        The access mode requested by the caller (e.g. `zarr.open(mode=...)`),
        or None when unspecified. Adapters for read-only resources should
        raise for explicit write modes (`"w"`, `"w-"`, `"a"`, `"r+"`)
        and open read-only for `None` and `"r"`.
    read_only : bool
        True when `mode == "r"`. Adapters must construct their store
        read-only when this is set.
    storage_options : dict[str, Any] | None
        Options passed by the caller. By convention these configure the
        *root* sub-URL (e.g. fsspec options), but adapters may consume
        adapter-specific keys.
    """

    preceding: tuple[PipelineSegment, ...]
    mode: AccessModeLiteral | None
    read_only: bool
    storage_options: dict[str, Any] | None
    _resolver: Callable[[tuple[PipelineSegment, ...]], Awaitable[AdapterResolution]] = field(
        repr=False
    )

    @property
    def preceding_url(self) -> str:
        """The pipeline to the left of this segment, reconstructed exactly."""
        return "|".join(segment.raw for segment in self.preceding)

    async def resolve_preceding(self) -> AdapterResolution:
        """
        Resolve the preceding pipeline into a store.

        This is the entry point for *wrapper* adapters (e.g. `zip:`) that
        operate on the resource produced by the segments to their left.
        Adapters backed by their own I/O machinery (e.g. `icechunk:`)
        should use [`preceding_url`][zarr.abc.url_pipeline.PipelineContext.preceding_url]
        instead and never materialize the intermediate store.
        """
        return await self._resolver(self.preceding)


class URLPipelineAdapter(ABC):
    """
    Handler for one URL pipeline scheme.

    Subclasses implement a single classmethod,
    [`open_pipeline_segment`][zarr.abc.url_pipeline.URLPipelineAdapter.open_pipeline_segment],
    and are registered under the `zarr.url_adapters` entry-point group with
    the URL scheme as the entry-point name:

        [project.entry-points."zarr.url_adapters"]
        myscheme = "mypackage.zarr_adapter:MyAdapter"

    An adapter is used in two positions:

    - as an *adapter segment*: `s3://bucket/repo|icechunk://tag.v1` — the
      context carries the preceding sub-URLs;
    - as a *root scheme*: `gh://org/repo` — `context.preceding` is empty.
    """

    @classmethod
    @abstractmethod
    async def open_pipeline_segment(
        cls, segment: PipelineSegment, context: PipelineContext
    ) -> AdapterResolution:
        """
        Resolve `segment` (in the context of the pipeline to its left)
        into a store, an optional residual path within that store, and an
        optional zarr format.

        The returned store must already be open and must honor
        `context.read_only`.
        """
        ...
