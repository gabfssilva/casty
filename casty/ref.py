from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from casty import LocalRef
    from casty.cluster import RemoteRef

type ActorRef[M] = LocalRef[M] | RemoteRef[M]
