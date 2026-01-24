from __future__ import annotations

import ast
import inspect
import textwrap
from types import SimpleNamespace
from typing import Any, Callable, Coroutine


class AssignmentFinder(ast.NodeVisitor):
    """Finds all variables that are assigned to in the function body."""

    def __init__(self):
        self.assigned: set[str] = set()

    def visit_Assign(self, node: ast.Assign) -> None:
        for target in node.targets:
            if isinstance(target, ast.Name):
                self.assigned.add(target.id)
        self.generic_visit(node)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        if isinstance(node.target, ast.Name):
            self.assigned.add(node.target.id)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if isinstance(node.target, ast.Name) and node.value is not None:
            self.assigned.add(node.target.id)
        self.generic_visit(node)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        if isinstance(node.target, ast.Name):
            self.assigned.add(node.target.id)
        self.generic_visit(node)


def find_assigned_params(func: Callable, positional_params: list[str]) -> list[str]:
    """Returns only the positional params that are assigned to in the function body."""
    source = inspect.getsource(func)
    source = textwrap.dedent(source)
    tree = ast.parse(source)

    func_def = tree.body[0]
    while not isinstance(func_def, ast.AsyncFunctionDef):
        if isinstance(func_def, ast.FunctionDef):
            for node in ast.walk(func_def):
                if isinstance(node, ast.AsyncFunctionDef):
                    func_def = node
                    break
            break
        func_def = func_def.body[0] if hasattr(func_def, 'body') else func_def

    finder = AssignmentFinder()
    for stmt in func_def.body:
        finder.visit(stmt)

    positional_set = set(positional_params)
    return [p for p in positional_params if p in finder.assigned and p in positional_set]


class StateTransformer(ast.NodeTransformer):
    """Transforms state variable references to use _state namespace.

    Transforms:
        count = count + 1
        print(count)
    Into:
        _state.count = _state.count + 1
        print(_state.count)
    """

    def __init__(self, state_vars: set[str]):
        self.state_vars = state_vars

    def visit_Name(self, node: ast.Name) -> ast.AST:
        if node.id in self.state_vars:
            return ast.Attribute(
                value=ast.Name(id='_state', ctx=ast.Load()),
                attr=node.id,
                ctx=node.ctx,
            )
        return node

    def visit_Assign(self, node: ast.Assign) -> ast.AST:
        if (len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id in self.state_vars):
            var_name = node.targets[0].id
            new_value = self.visit(node.value)
            return ast.Assign(
                targets=[ast.Attribute(
                    value=ast.Name(id='_state', ctx=ast.Load()),
                    attr=var_name,
                    ctx=ast.Store(),
                )],
                value=new_value,
            )
        return self.generic_visit(node)

    def visit_AugAssign(self, node: ast.AugAssign) -> ast.AST:
        if (isinstance(node.target, ast.Name)
            and node.target.id in self.state_vars):
            var_name = node.target.id
            return ast.AugAssign(
                target=ast.Attribute(
                    value=ast.Name(id='_state', ctx=ast.Load()),
                    attr=var_name,
                    ctx=ast.Store(),
                ),
                op=node.op,
                value=self.visit(node.value),
            )
        return self.generic_visit(node)


def get_positional_params(func: Callable) -> list[str]:
    sig = inspect.signature(func)
    positional = []

    for name, param in sig.parameters.items():
        if param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD
        ):
            positional.append(name)
        elif param.kind == inspect.Parameter.KEYWORD_ONLY:
            break

    return positional


def get_defaults_for_params(func: Callable, param_names: list[str]) -> list[Any]:
    """Get default values for specific params, preserving order."""
    sig = inspect.signature(func)
    defaults = []
    param_set = set(param_names)

    for name in param_names:
        param = sig.parameters.get(name)
        if param and param.default is not inspect.Parameter.empty:
            defaults.append(param.default)

    return defaults


def transform_actor_function(
    func: Callable[..., Coroutine[Any, Any, None]],
    state_vars: list[str],
) -> Callable[..., Coroutine[Any, Any, None]]:
    if not state_vars:
        return func

    source = inspect.getsource(func)
    source = textwrap.dedent(source)

    tree = ast.parse(source)

    func_def = tree.body[0]
    while isinstance(func_def, ast.AsyncFunctionDef) is False:
        if isinstance(func_def, ast.FunctionDef):
            for node in ast.walk(func_def):
                if isinstance(node, ast.AsyncFunctionDef):
                    func_def = node
                    break
            break
        func_def = func_def.body[0] if hasattr(func_def, 'body') else func_def

    for decorator in func_def.decorator_list[:]:
        func_def.decorator_list.remove(decorator)

    # Replace positional params with single _state param
    new_args = []
    state_vars_set = set(state_vars)
    inserted_state = False
    for arg in func_def.args.args:
        if arg.arg in state_vars_set:
            if not inserted_state:
                new_args.append(ast.arg(arg='_state', annotation=None))
                inserted_state = True
        else:
            new_args.append(arg)
    func_def.args.args = new_args

    # Also handle posonlyargs if any
    new_posonlyargs = []
    for arg in func_def.args.posonlyargs:
        if arg.arg in state_vars_set:
            if not inserted_state:
                new_posonlyargs.append(ast.arg(arg='_state', annotation=None))
                inserted_state = True
        else:
            new_posonlyargs.append(arg)
    func_def.args.posonlyargs = new_posonlyargs

    # Clear defaults for state params (they're handled by the namespace)
    num_state_params = len([a for a in state_vars if a in state_vars_set])
    if func_def.args.defaults:
        func_def.args.defaults = func_def.args.defaults[num_state_params:]

    transformer = StateTransformer(state_vars_set)
    new_tree = transformer.visit(tree)
    ast.fix_missing_locations(new_tree)

    code = compile(new_tree, filename=inspect.getfile(func), mode='exec')

    namespace = func.__globals__

    # Inject closure variables into globals so the exec'd function can access them
    if func.__closure__ and func.__code__.co_freevars:
        for name, cell in zip(func.__code__.co_freevars, func.__closure__):
            namespace[name] = cell.cell_contents

    local_ns: dict[str, Any] = {}
    exec(code, namespace, local_ns)
    new_func = local_ns[func.__name__]
    new_func.__original__ = func
    new_func.__transformed__ = True
    new_func.__state_vars__ = state_vars

    return new_func


def make_state_namespace(params: list[str], values: list[Any]) -> SimpleNamespace:
    return SimpleNamespace(**dict(zip(params, values)))
