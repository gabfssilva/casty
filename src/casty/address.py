"""Actor address representation with URI-based parsing.

Provides ``ActorAddress``, a frozen dataclass that identifies an actor within
a Casty system using a URI scheme: ``casty://system@host:port/path`` (remote)
or ``casty://system/path`` (local).
"""

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
    """Immutable address identifying an actor within a Casty system.

    Uses a URI scheme: ``casty://system@host:port/path`` for remote actors
    and ``casty://system/path`` for local actors.

    Parameters
    ----------
    system : str
        Name of the actor system.
    path : str
        Slash-prefixed path of the actor within the system.
    host : str or None
        Hostname for remote addresses, ``None`` for local.
    port : int or None
        TCP port for remote addresses, ``None`` for local.

    Examples
    --------
    >>> local = ActorAddress(system="my-system", path="/user/counter")
    >>> local.is_local
    True
    >>> local.to_uri()
    'casty://my-system/user/counter'

    >>> remote = ActorAddress(system="cluster", path="/user/worker", host="10.0.0.1", port=25520)
    >>> remote.to_uri()
    'casty://cluster@10.0.0.1:25520/user/worker'
    """

    system: str
    path: str
    host: str | None = None
    port: int | None = None

    @property
    def is_local(self) -> bool:
        """Return ``True`` if this address has no host (i.e. is local).

        Returns
        -------
        bool

        Examples
        --------
        >>> ActorAddress(system="sys", path="/a").is_local
        True
        >>> ActorAddress(system="sys", path="/a", host="h", port=1).is_local
        False
        """
        return self.host is None

    def to_uri(self) -> str:
        """Serialize the address to a ``casty://`` URI string.

        Returns
        -------
        str
            URI representation of the address.

        Examples
        --------
        >>> addr = ActorAddress(system="sys", path="/user/greeter", host="localhost", port=9000)
        >>> addr.to_uri()
        'casty://sys@localhost:9000/user/greeter'
        """
        # Strip leading slash from path for URI representation
        uri_path = self.path.lstrip("/")
        if self.host is not None and self.port is not None:
            return f"casty://{self.system}@{self.host}:{self.port}/{uri_path}"
        return f"casty://{self.system}/{uri_path}"

    @staticmethod
    def from_uri(uri: str) -> ActorAddress:
        """Parse a ``casty://`` URI into an ``ActorAddress``.

        Parameters
        ----------
        uri : str
            A URI of the form ``casty://system@host:port/path`` (remote)
            or ``casty://system/path`` (local).

        Returns
        -------
        ActorAddress

        Raises
        ------
        ValueError
            If *uri* does not match either pattern.

        Examples
        --------
        >>> addr = ActorAddress.from_uri("casty://sys@127.0.0.1:8000/user/actor")
        >>> addr.host
        '127.0.0.1'
        >>> addr.path
        '/user/actor'
        """
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
