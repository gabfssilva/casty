"""`Askable`, the base of a message that is answered.

Its annotations are evaluated as the class is built, not kept as strings: the schema reads `reply_to: Ref[R]` from it,
and the `get_type_hints` of early 3.12 releases cannot evaluate a string that names a type parameter.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from casty import Ref

    def nobody() -> Ref[object]: ...
else:
    # The extension has no stub, and `casty` is still importing this module.
    from casty._casty import Ref, nobody

# A `Ref[object]` takes any answer, so it is the default of every `Askable`.
_NOBODY = nobody()


@dataclass(frozen=True)
class Askable[R]:
    """A message answered with an `R`, which is what `Ref.ask` and `Context.ask` send.

    A subclass names the type of its answer, `class Withdraw(Askable[bool])`, and the actor that receives it answers
    with `msg.reply_to.tell(...)`. `ask` gives the message it sends a `reply_to` of its own. Sent with `tell`, a
    message answers whoever its `reply_to` names: nobody, unless it was built with one, as
    `Withdraw(30, reply_to=ctx.self)`, whose answer comes to the actor as a message, with no deadline. A message
    handed on as it was received keeps its `reply_to`, so whoever it is handed to may answer.

    Attributes
    ----------
    reply_to
        Where the answer goes. Keyword-only, not part of `repr`, and not compared. Without one, the ref that reaches
        nobody, which drops what it is told.
    """

    reply_to: Ref[R] = field(default=_NOBODY, kw_only=True, repr=False, compare=False)

    if TYPE_CHECKING:
        # For the checkers only: `ask` matches this against `_Asked[M, R]`, which checks at once that the message is
        # one the actor takes and that its answer is an `R`.
        def _asked(self) -> tuple[Self, R]: ...
