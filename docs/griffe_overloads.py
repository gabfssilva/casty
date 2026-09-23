"""Griffe extension that keeps the functions a stub declares only as `@overload`s.

Griffe attaches overloads to the implementation that follows them, and a stub has none, so without this
`casty.actor`, `State.update`, `Context.become` and the `ref` of `ActorSystem` and `Client` would be missing from the
reference. The function it makes stands for the overloads: their parameters, and the docstrings they carry, in order.
"""

from __future__ import annotations

import ast

from griffe import (
    Class,
    Docstring,
    Extension,
    Function,
    Inspector,
    Module,
    Object,
    ObjectNode,
    Parameter,
    Parameters,
    Visitor,
)


class StubOverloads(Extension):
    """Make a function of each name a module or class declares only through overloads."""

    def on_members(
        self, *, node: ast.AST | ObjectNode, obj: Object, agent: Visitor | Inspector, **kwargs: object
    ) -> None:
        if not isinstance(obj, Module | Class):
            return
        for name, overloads in obj.overloads.items():
            if overloads and name not in obj.members:
                # The overloads stay where they are, for the merge of this stub to attach them to an implementation of
                # the same name in the module.
                obj.set_member(name, _declared(name, overloads, obj))


def _declared(name: str, overloads: list[Function], parent: Module | Class) -> Function:
    first = overloads[0]
    parameters: dict[str, Parameter] = {}
    for overload in overloads:
        for parameter in overload.parameters:
            parameters.setdefault(
                parameter.name,
                Parameter(
                    parameter.name,
                    annotation=parameter.annotation,
                    kind=parameter.kind,
                    default=parameter.default,
                ),
            )
    returns = {str(overload.returns) for overload in overloads}
    function = Function(
        name,
        lineno=first.lineno,
        endlineno=overloads[-1].endlineno,
        parameters=Parameters(*parameters.values()),
        returns=first.returns if len(returns) == 1 else None,
        type_parameters=first.type_parameters,
        parent=parent,
        runtime=first.runtime,
        analysis="static",
    )
    function.labels |= first.labels
    function.overloads = overloads
    documented = [overload.docstring for overload in overloads if overload.docstring is not None]
    if documented:
        function.docstring = Docstring(
            "\n\n".join(docstring.value for docstring in documented),
            lineno=documented[0].lineno,
            endlineno=documented[-1].endlineno,
            parent=function,
            parser=documented[0].parser,
            parser_options=documented[0].parser_options,
        )
    return function
