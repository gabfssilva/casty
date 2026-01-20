def test_core_imports():
    from casty import actor, Behavior, Mailbox, Context, ActorRef, LocalActorRef, Envelope

    assert callable(actor)
    assert Behavior is not None
    assert Mailbox is not None
    assert Context is not None
    assert LocalActorRef is not None
    assert Envelope is not None


def test_import_message():
    from casty import message
    assert callable(message)


def test_import_state():
    from casty import State
    state = State(0)
    assert state.value == 0


def test_import_replication():
    from casty import replicated, Routing, ReplicationConfig
    assert callable(replicated)
    assert Routing.LEADER.value == "leader"
    assert ReplicationConfig is not None
