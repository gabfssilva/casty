from casty.remote.protocol import RemoteEnvelope


def test_envelope_lookup_has_ensure():
    envelope = RemoteEnvelope(
        type="lookup",
        name="test/actor",
        correlation_id="abc123",
        ensure=True,
    )
    assert envelope.ensure is True

    d = envelope.to_dict()
    assert d.get("ensure") is True

    restored = RemoteEnvelope.from_dict(d)
    assert restored.ensure is True


def test_envelope_lookup_ensure_defaults_false():
    envelope = RemoteEnvelope(
        type="lookup",
        name="test/actor",
        correlation_id="abc123",
    )
    assert envelope.ensure is False
