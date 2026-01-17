from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from casty import LocalRef
    from casty.cluster import ClusteredRef

type ActorRef[M] = LocalRef[M] | ClusteredRef[M]
