from __future__ import annotations

import re
from dataclasses import dataclass

_REMOTE_PATTERN = re.compile(
    r"^casty://(?P<system>[^@/]+)@(?P<host>[^:]+):(?P<port>\d+)/(?P<path>.+)$"
)
_LOCAL_PATTERN = re.compile(
    r"^casty://(?P<system>[^@/]+)/(?P<path>.+)$"
)


@dataclass(frozen=True)
class ActorAddress:
    system: str
    path: str
    host: str | None = None
    port: int | None = None

    @property
    def is_local(self) -> bool:
        return self.host is None

    def to_uri(self) -> str:
        # Strip leading slash from path for URI representation
        uri_path = self.path.lstrip("/")
        if self.host is not None and self.port is not None:
            return f"casty://{self.system}@{self.host}:{self.port}/{uri_path}"
        return f"casty://{self.system}/{uri_path}"

    @staticmethod
    def from_uri(uri: str) -> ActorAddress:
        remote_match = _REMOTE_PATTERN.match(uri)
        if remote_match:
            return ActorAddress(
                system=remote_match.group("system"),
                path=f"/{remote_match.group('path')}",
                host=remote_match.group("host"),
                port=int(remote_match.group("port")),
            )

        local_match = _LOCAL_PATTERN.match(uri)
        if local_match:
            return ActorAddress(
                system=local_match.group("system"),
                path=f"/{local_match.group('path')}",
            )

        msg = f"Invalid ActorAddress URI: must start with 'casty://', got: {uri}"
        raise ValueError(msg)
