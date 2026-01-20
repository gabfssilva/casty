def test_core_imports():
    from casty import actor, Behavior, Mailbox, Context, ActorRef, LocalActorRef, Envelope

    assert callable(actor)
    assert Behavior is not None
    assert Mailbox is not None
    assert Context is not None
    assert LocalActorRef is not None
    assert Envelope is not None
