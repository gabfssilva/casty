from casty.cluster.consistency import resolve_replication, resolve_consistency


class TestResolveReplication:
    def test_int_value(self):
        assert resolve_replication(3, total_nodes=5) == 3

    def test_one(self):
        assert resolve_replication(1, total_nodes=5) == 1

    def test_quorum(self):
        assert resolve_replication('quorum', total_nodes=5) == 3
        assert resolve_replication('quorum', total_nodes=4) == 3
        assert resolve_replication('quorum', total_nodes=3) == 2

    def test_all(self):
        assert resolve_replication('all', total_nodes=5) == 5

    def test_exceeds_total_capped(self):
        assert resolve_replication(10, total_nodes=3) == 3


class TestResolveConsistency:
    def test_async(self):
        assert resolve_consistency('async', replicas=3) == 0

    def test_one(self):
        assert resolve_consistency('one', replicas=3) == 1
        assert resolve_consistency(1, replicas=3) == 1

    def test_quorum(self):
        assert resolve_consistency('quorum', replicas=3) == 2
        assert resolve_consistency('quorum', replicas=5) == 3

    def test_all(self):
        assert resolve_consistency('all', replicas=3) == 3

    def test_int_value(self):
        assert resolve_consistency(2, replicas=3) == 2

    def test_exceeds_replicas_capped(self):
        assert resolve_consistency(10, replicas=3) == 3
