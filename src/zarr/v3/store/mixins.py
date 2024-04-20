import json
import numbers
from enum import Enum
from typing import Any, Optional

import numpy as np


def json_default(o: Any) -> Any:
    # See json.JSONEncoder.default docstring for explanation
    # This is necessary to encode numpy dtype
    if isinstance(o, numbers.Integral):
        return int(o)
    if isinstance(o, numbers.Real):
        return float(o)
    if isinstance(o, np.dtype):
        if o.fields is None:
            return o.str
        else:
            return o.descr
    if isinstance(o, Enum):
        return o.name
    # this serializes numcodecs compressors
    # todo: implement to_dict for codecs
    elif hasattr(o, "get_config"):
        return o.get_config()
    raise TypeError


class JsonMetadataStoreMixin:
    async def get_metadata(self, key: str) -> Optional[dict[str, Any]]:
        data = await self.get(key)
        if data is None:
            return None
        return json.loads(data)

    async def set_metadata(self, key: str, metadata: dict[str, Any]) -> None:
        data = json.dumps(metadata, default=json_default).encode("utf-8")
        await self.set(key, data)
