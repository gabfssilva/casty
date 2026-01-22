from casty.remote.messages import Lookup, LookupResult


def test_lookup_has_ensure_field():
    lookup = Lookup(name="test")
    assert hasattr(lookup, "ensure")
    assert lookup.ensure is False


def test_lookup_has_timeout_field():
    lookup = Lookup(name="test")
    assert hasattr(lookup, "timeout")
    assert lookup.timeout is None


def test_lookup_with_ensure_true():
    lookup = Lookup(name="test", ensure=True)
    assert lookup.ensure is True


def test_lookup_with_timeout():
    lookup = Lookup(name="test", timeout=5.0)
    assert lookup.timeout == 5.0


def test_lookup_result_has_error_field():
    result = LookupResult(ref=None, error="behavior not found")
    assert result.error == "behavior not found"

    result_ok = LookupResult(ref=None)
    assert result_ok.error is None
