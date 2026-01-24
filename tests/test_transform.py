"""Tests for AST transformation of state variables."""

import ast
import pytest
from types import SimpleNamespace

from casty.transform import (
    StateTransformer,
    get_positional_params,
    find_assigned_params,
    transform_actor_function,
    make_state_namespace,
)


def test_get_positional_params():
    async def example(a: int, b: str, *, mailbox, ctx):
        pass

    params = get_positional_params(example)
    assert params == ['a', 'b']


def test_get_positional_params_no_positional():
    async def example(*, mailbox, ctx):
        pass

    params = get_positional_params(example)
    assert params == []


def test_find_assigned_params():
    async def example(a: int, b: str, c: str, *, mailbox):
        a = a + 1
        b += "x"
        print(c)

    positional = get_positional_params(example)
    assigned = find_assigned_params(example, positional)
    assert assigned == ['a', 'b']


def test_find_assigned_params_none_assigned():
    async def example(a: int, b: str, *, mailbox):
        print(a, b)

    positional = get_positional_params(example)
    assigned = find_assigned_params(example, positional)
    assert assigned == []


def test_state_transformer_simple_assign():
    code = "count = count + 1"
    tree = ast.parse(code)

    transformer = StateTransformer({'count'})
    new_tree = transformer.visit(tree)
    ast.fix_missing_locations(new_tree)

    result = ast.unparse(new_tree)
    assert result == "_state.count = _state.count + 1"


def test_state_transformer_aug_assign():
    code = "count += 1"
    tree = ast.parse(code)

    transformer = StateTransformer({'count'})
    new_tree = transformer.visit(tree)

    result = ast.unparse(new_tree)
    assert result == "_state.count += 1"


def test_state_transformer_multiple_vars():
    code = """
count = count + 1
limit = limit - 1
name = "foo"
"""
    tree = ast.parse(code)

    transformer = StateTransformer({'count', 'limit'})
    new_tree = transformer.visit(tree)
    ast.fix_missing_locations(new_tree)

    result = ast.unparse(new_tree)
    assert "_state.count = _state.count + 1" in result
    assert "_state.limit = _state.limit - 1" in result
    assert "name = 'foo'" in result


def test_state_transformer_preserves_non_state():
    code = "x = 10"
    tree = ast.parse(code)

    transformer = StateTransformer({'count'})
    new_tree = transformer.visit(tree)

    result = ast.unparse(new_tree)
    assert result == "x = 10"


def test_state_transformer_nested_expression():
    code = "count = (count * 2) + other"
    tree = ast.parse(code)

    transformer = StateTransformer({'count'})
    new_tree = transformer.visit(tree)
    ast.fix_missing_locations(new_tree)

    result = ast.unparse(new_tree)
    assert result == "_state.count = _state.count * 2 + other"


def test_transform_actor_function():
    async def counter(count: int, *, mailbox):
        count = count + 1
        return count

    transformed = transform_actor_function(counter, ['count'])

    assert hasattr(transformed, '__transformed__')
    assert transformed.__transformed__ is True
    assert transformed.__state_vars__ == ['count']


@pytest.mark.asyncio
async def test_transformed_function_executes():
    async def counter(count: int, *, mailbox) -> None:
        count = count + 1
        count += 10

    transformed = transform_actor_function(counter, ['count'])

    state = SimpleNamespace(count=0)
    await transformed(state, mailbox=None)

    assert state.count == 11


@pytest.mark.asyncio
async def test_transformed_function_multiple_states():
    async def game(score: int, lives: int, *, mailbox) -> None:
        score = score + 100
        lives = lives - 1

    transformed = transform_actor_function(game, ['score', 'lives'])

    state = SimpleNamespace(score=0, lives=3)
    await transformed(state, mailbox=None)

    assert state.score == 100
    assert state.lives == 2


@pytest.mark.asyncio
async def test_state_read_in_condition():
    """Test that state reads in conditions are transformed."""
    async def bounded(count: int, limit: int, *, mailbox) -> None:
        if count < limit:
            count = count + 1

    transformed = transform_actor_function(bounded, ['count', 'limit'])

    state = SimpleNamespace(count=5, limit=10)
    await transformed(state, mailbox=None)

    assert state.count == 6
    assert state.limit == 10  # unchanged


@pytest.mark.asyncio
async def test_state_in_loop():
    """Test state updates in a loop."""
    async def summer(total: int, *, mailbox) -> None:
        for i in range(5):
            total = total + i

    transformed = transform_actor_function(summer, ['total'])

    state = SimpleNamespace(total=0)
    await transformed(state, mailbox=None)

    assert state.total == 10  # 0+1+2+3+4


def test_make_state_namespace():
    ns = make_state_namespace(['a', 'b', 'c'], [1, 2, 3])
    assert ns.a == 1
    assert ns.b == 2
    assert ns.c == 3
