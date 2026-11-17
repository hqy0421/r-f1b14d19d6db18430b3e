import pytest

from dbt.tests.util import run_dbt, relation_from_name


# The cdc strategy models read their change batches from plain feed tables so
# that each test can replay arbitrary (out-of-order, repeated, conflicting)
# upstream CDC batches between runs. Every test gets its own model and feed
# table because all tests in this file share one DuckDB database and schema.


def _cdc_model(feed):
    return (
        """
{{ config(
    materialized='incremental',
    incremental_strategy='cdc',
    unique_key='id',
) }}
select * from %s
"""
        % feed
    )


_cdc_downstream = """
select id, name, val from {{ ref('cdc_basic') }} order by id
"""

_cdc_custom = """
{{ config(
    materialized='incremental',
    incremental_strategy='cdc',
    unique_key=['k1', 'k2'],
    cdc_version_column='ver',
    cdc_operation_column='op',
    cdc_upsert_values=['u', 'c'],
    cdc_delete_values=['d'],
) }}
select * from cdc_feed_custom
"""

_cdc_schema = """
{{ config(
    materialized='incremental',
    incremental_strategy='cdc',
    unique_key='id',
    on_schema_change='append_new_columns',
) }}
select * from cdc_feed_schema
"""

_cdc_hooked = """
{{ config(
    materialized='incremental',
    incremental_strategy='cdc',
    unique_key='id',
    post_hook="select 1 from cdc_hook_killswitch",
) }}
select * from cdc_feed_hooked
"""

_cdc_no_key = """
{{ config(
    materialized='incremental',
    incremental_strategy='cdc',
) }}
select * from cdc_feed_no_key
"""


def _feed(project, name, rows):
    """(Re)create a CDC feed table with the default meta columns."""
    project.run_sql(f"drop table if exists {name}")
    project.run_sql(
        f"""
        create table {name} as
        select * from (
            values {rows}
        ) as t(id, _cdc_version, _cdc_op, name, val)
        """
    )


def _rows(project, relation, order_by="id"):
    return project.run_sql(f"select * from {relation} order by {order_by}", fetch="all")


def _state_rows(project, relation, order_by="1"):
    return project.run_sql(f"select * from {relation} order by {order_by}", fetch="all")


class TestIncrementalCDC:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "cdc_basic.sql": _cdc_model("cdc_feed_basic"),
            "cdc_downstream.sql": _cdc_downstream,
            "cdc_replay.sql": _cdc_model("cdc_feed_replay"),
            "cdc_tombstone.sql": _cdc_model("cdc_feed_tombstone"),
            "cdc_conflict.sql": _cdc_model("cdc_feed_conflict"),
            "cdc_deletes.sql": _cdc_model("cdc_feed_deletes"),
            "cdc_refresh.sql": _cdc_model("cdc_feed_refresh"),
            "cdc_schema.sql": _cdc_schema,
            "cdc_hooked.sql": _cdc_hooked,
            "cdc_custom.sql": _cdc_custom,
            "cdc_err.sql": _cdc_model("cdc_feed_err"),
            "cdc_no_key.sql": _cdc_no_key,
            "cdc_collide.sql": _cdc_model("cdc_feed_collide"),
        }

    def test_out_of_order_batch_and_version_wins(self, project):
        """Highest version per key wins, out-of-order and duplicate replays are absorbed."""
        _feed(
            project,
            "cdc_feed_basic",
            """
            (1, 3, 'upsert', 'alice-v3', 30),
            (1, 1, 'upsert', 'alice-v1', 10),
            (1, 2, 'upsert', 'alice-v2', 20),
            (2, 1, 'upsert', 'bob-v1', 100),
            (3, 5, 'delete', 'ghost', 0)
            """,
        )
        run_dbt(["run", "--select", "cdc_basic", "cdc_downstream"])

        relation = relation_from_name(project.adapter, "cdc_basic")
        state = relation_from_name(project.adapter, "cdc_basic__dbt_cdc_state")

        # business table holds only business columns, one row per key at max version
        columns = [row[0] for row in project.run_sql(f"describe {relation}", fetch="all")]
        assert columns == ["id", "name", "val"]
        assert _rows(project, relation) == [(1, "alice-v3", 30), (2, "bob-v1", 100)]

        # the state table tracks every key, including the never-present deleted one
        state_columns = [row[0] for row in project.run_sql(f"describe {state}", fetch="all")]
        assert state_columns == ["id", "_dbt_cdc_version", "_dbt_cdc_op", "_dbt_cdc_hash"]
        assert [(r[0], r[1], r[2]) for r in _state_rows(project, state)] == [
            (1, 3, "upsert"),
            (2, 1, "upsert"),
            (3, 5, "delete"),
        ]

        # ref() reads the plain business columns
        downstream = relation_from_name(project.adapter, "cdc_downstream")
        assert _rows(project, downstream) == [(1, "alice-v3", 30), (2, "bob-v1", 100)]

        # second batch: stale update ignored, delete applied, delete resurrected by
        # a newer version, identical duplicate rows deduplicated
        _feed(
            project,
            "cdc_feed_basic",
            """
            (1, 2, 'upsert', 'alice-STALE', 999),
            (2, 4, 'delete', 'bob', 0),
            (3, 6, 'upsert', 'ghost-back', 60),
            (4, 1, 'upsert', 'dan', 1),
            (4, 1, 'upsert', 'dan', 1)
            """,
        )
        run_dbt(["run", "--select", "cdc_basic"])

        assert _rows(project, relation) == [
            (1, "alice-v3", 30),
            (3, "ghost-back", 60),
            (4, "dan", 1),
        ]
        assert [(r[0], r[1], r[2]) for r in _state_rows(project, state)] == [
            (1, 3, "upsert"),
            (2, 4, "delete"),
            (3, 6, "upsert"),
            (4, 1, "upsert"),
        ]

    def test_replay_is_idempotent(self, project):
        """Replaying the exact same batch is a no-op."""
        rows = """
            (1, 3, 'upsert', 'alice-v3', 30),
            (2, 4, 'delete', 'bob', 0)
        """
        _feed(project, "cdc_feed_replay", rows)
        run_dbt(["run", "--select", "cdc_replay"])
        relation = relation_from_name(project.adapter, "cdc_replay")
        state = relation_from_name(project.adapter, "cdc_replay__dbt_cdc_state")
        before = _rows(project, relation)
        state_before = _state_rows(project, state)

        run_dbt(["run", "--select", "cdc_replay"])
        run_dbt(["run", "--select", "cdc_replay"])

        assert _rows(project, relation) == before
        assert _state_rows(project, state) == state_before

    def test_stale_update_does_not_resurrect_deleted_key(self, project):
        """A delete tombstone is remembered across runs; older updates stay ignored."""
        _feed(project, "cdc_feed_tombstone", "(1, 5, 'delete', 'alice', 0)")
        run_dbt(["run", "--select", "cdc_tombstone"])
        relation = relation_from_name(project.adapter, "cdc_tombstone")
        assert _rows(project, relation) == []

        # an older upsert replayed in a later run must not bring the key back
        _feed(project, "cdc_feed_tombstone", "(1, 3, 'upsert', 'alice-old', 3)")
        run_dbt(["run", "--select", "cdc_tombstone"])
        assert _rows(project, relation) == []

        # a newer upsert resurrects the key
        _feed(project, "cdc_feed_tombstone", "(1, 6, 'upsert', 'alice-new', 6)")
        run_dbt(["run", "--select", "cdc_tombstone"])
        assert _rows(project, relation) == [(1, "alice-new", 6)]

    def test_same_version_conflicting_content_errors(self, project):
        """Same key + same version + different content fails, against state or in-batch."""
        _feed(project, "cdc_feed_conflict", "(1, 5, 'upsert', 'x', 1)")
        run_dbt(["run", "--select", "cdc_conflict"])
        relation = relation_from_name(project.adapter, "cdc_conflict")

        # conflicts with the version already recorded in the state table
        _feed(project, "cdc_feed_conflict", "(1, 5, 'upsert', 'y', 2)")
        result = run_dbt(["run", "--select", "cdc_conflict"], expect_pass=False)
        assert "conflicts with a different change already applied" in str(
            result.results[0].message
        )
        # the failed run left both tables untouched
        assert _rows(project, relation) == [(1, "x", 1)]
        state = relation_from_name(project.adapter, "cdc_conflict__dbt_cdc_state")
        assert [(r[0], r[1], r[2]) for r in _state_rows(project, state)] == [(1, 5, "upsert")]

        # conflicting rows inside a single batch
        _feed(project, "cdc_feed_conflict", "(2, 7, 'upsert', 'a', 1), (2, 7, 'upsert', 'b', 2)")
        result = run_dbt(["run", "--select", "cdc_conflict"], expect_pass=False)
        assert "conflicting contents at version 7" in str(result.results[0].message)

    def test_delete_only_batches(self, project):
        """Batches containing only deletes work, including on first load."""
        _feed(project, "cdc_feed_deletes", "(1, 1, 'delete', 'a', 0), (2, 2, 'delete', 'b', 0)")
        run_dbt(["run", "--select", "cdc_deletes"])

        relation = relation_from_name(project.adapter, "cdc_deletes")
        state = relation_from_name(project.adapter, "cdc_deletes__dbt_cdc_state")
        assert _rows(project, relation) == []
        assert [(r[0], r[1], r[2]) for r in _state_rows(project, state)] == [
            (1, 1, "delete"),
            (2, 2, "delete"),
        ]

        # an upsert below the tombstone version is ignored, a newer one lands
        _feed(
            project,
            "cdc_feed_deletes",
            "(1, 3, 'upsert', 'a-back', 10), (2, 1, 'upsert', 'b-stale', 5)",
        )
        run_dbt(["run", "--select", "cdc_deletes"])
        assert _rows(project, relation) == [(1, "a-back", 10)]

    def test_full_refresh_rebuilds_business_and_state(self, project):
        """Full-refresh re-applies the same rules from scratch and resets the state."""
        _feed(project, "cdc_feed_refresh", "(1, 9, 'upsert', 'old', 9)")
        run_dbt(["run", "--select", "cdc_refresh"])

        _feed(
            project,
            "cdc_feed_refresh",
            "(1, 1, 'upsert', 'fresh', 1), (2, 2, 'delete', 'gone', 0)",
        )
        run_dbt(["run", "--select", "cdc_refresh", "--full-refresh"])

        relation = relation_from_name(project.adapter, "cdc_refresh")
        state = relation_from_name(project.adapter, "cdc_refresh__dbt_cdc_state")
        assert _rows(project, relation) == [(1, "fresh", 1)]
        assert [(r[0], r[1], r[2]) for r in _state_rows(project, state)] == [
            (1, 1, "upsert"),
            (2, 2, "delete"),
        ]

        # versions from before the full-refresh no longer suppress anything
        _feed(project, "cdc_feed_refresh", "(1, 2, 'upsert', 'fresher', 2)")
        run_dbt(["run", "--select", "cdc_refresh"])
        assert _rows(project, relation) == [(1, "fresher", 2)]

    def test_meta_columns_never_reach_business_table(self, project):
        """on_schema_change must not add the version/operation columns to the target."""
        _feed(project, "cdc_feed_schema", "(1, 1, 'upsert', 'a', 1)")
        run_dbt(["run", "--select", "cdc_schema"])
        relation = relation_from_name(project.adapter, "cdc_schema")
        columns = [row[0] for row in project.run_sql(f"describe {relation}", fetch="all")]
        assert columns == ["id", "name", "val"]

        # a new business column is appended; the meta columns are not
        project.run_sql("drop table if exists cdc_feed_schema")
        project.run_sql(
            """
            create table cdc_feed_schema as
            select * from (values (1, 2, 'upsert', 'a2', 2, 'new'))
            as t(id, _cdc_version, _cdc_op, name, val, extra)
            """
        )
        run_dbt(["run", "--select", "cdc_schema"])
        columns = [row[0] for row in project.run_sql(f"describe {relation}", fetch="all")]
        assert columns == ["id", "name", "val", "extra"]
        assert _rows(project, relation) == [(1, "a2", 2, "new")]

    def test_in_transaction_hook_failure_rolls_back_everything(self, project):
        """A failing post-hook inside the transaction leaves no half-applied state."""
        project.run_sql("create or replace table cdc_hook_killswitch as select 1 as x")
        _feed(project, "cdc_feed_hooked", "(1, 1, 'upsert', 'a', 1)")
        run_dbt(["run", "--select", "cdc_hooked"])

        relation = relation_from_name(project.adapter, "cdc_hooked")
        state = relation_from_name(project.adapter, "cdc_hooked__dbt_cdc_state")
        assert _rows(project, relation) == [(1, "a", 1)]

        # the post-hook now fails; the whole model transaction must roll back
        project.run_sql("drop table cdc_hook_killswitch")
        _feed(project, "cdc_feed_hooked", "(1, 2, 'upsert', 'SHOULD-NOT-LAND', 2)")
        run_dbt(["run", "--select", "cdc_hooked"], expect_pass=False)

        assert _rows(project, relation) == [(1, "a", 1)]
        assert [(r[0], r[1], r[2]) for r in _state_rows(project, state)] == [(1, 1, "upsert")]

    def test_custom_columns_values_and_composite_key(self, project):
        """Custom meta column names/values and composite business keys."""
        project.run_sql("drop table if exists cdc_feed_custom")
        project.run_sql(
            """
            create table cdc_feed_custom as
            select * from (
                values
                ('a', 1, 5, 'U', 'row-a1'),
                ('a', 1, 3, 'u', 'row-a1-stale'),
                ('a', 2, 2, 'd', 'gone'),
                ('b', 1, 1, 'c', 'row-b1')
            ) as t(k1, k2, ver, op, payload)
            """
        )
        run_dbt(["run", "--select", "cdc_custom"])

        relation = relation_from_name(project.adapter, "cdc_custom")
        state = relation_from_name(project.adapter, "cdc_custom__dbt_cdc_state")
        columns = [row[0] for row in project.run_sql(f"describe {relation}", fetch="all")]
        assert columns == ["k1", "k2", "payload"]
        assert _rows(project, relation, order_by="k1, k2") == [
            ("a", 1, "row-a1"),
            ("b", 1, "row-b1"),
        ]
        assert [
            (r[0], r[1], r[2], r[3]) for r in _state_rows(project, state, order_by="k1, k2")
        ] == [
            ("a", 1, 5, "upsert"),
            ("a", 2, 2, "delete"),
            ("b", 1, 1, "upsert"),
        ]

    def test_invalid_operation_errors(self, project):
        _feed(project, "cdc_feed_err", "(1, 1, 'bogus', 'a', 1)")
        result = run_dbt(["run", "--select", "cdc_err"], expect_pass=False)
        assert "invalid operation 'bogus'" in str(result.results[0].message)

    def test_null_version_errors(self, project):
        _feed(project, "cdc_feed_err", "(1, null, 'upsert', 'a', 1)")
        result = run_dbt(["run", "--select", "cdc_err"], expect_pass=False)
        assert "null version" in str(result.results[0].message)

    def test_null_key_errors(self, project):
        _feed(project, "cdc_feed_err", "(null, 1, 'upsert', 'a', 1)")
        result = run_dbt(["run", "--select", "cdc_err"], expect_pass=False)
        assert "null value in unique_key" in str(result.results[0].message)

    def test_non_integer_version_errors(self, project):
        project.run_sql("drop table if exists cdc_feed_err")
        project.run_sql(
            """
            create table cdc_feed_err as
            select * from (values (1, 1.5, 'upsert', 'a', 1))
            as t(id, _cdc_version, _cdc_op, name, val)
            """
        )
        result = run_dbt(["run", "--select", "cdc_err"], expect_pass=False)
        assert "must have an integer type" in str(result.results[0].message)

    def test_missing_unique_key_errors(self, project):
        _feed(project, "cdc_feed_no_key", "(1, 1, 'upsert', 'a', 1)")
        result = run_dbt(["run", "--select", "cdc_no_key"], expect_pass=False)
        assert "requires a unique_key" in str(result.results[0].message)

    def test_business_column_named_like_internal_columns(self, project):
        """Business columns may reuse names the apply script uses internally."""
        project.run_sql("drop table if exists cdc_feed_collide")
        project.run_sql(
            """
            create table cdc_feed_collide as
            select * from (values (1, 1, 'upsert', 'keep-me', 'a'))
            as t(id, _cdc_version, _cdc_op, _cdc_ver, name)
            """
        )
        run_dbt(["run", "--select", "cdc_collide"])

        relation = relation_from_name(project.adapter, "cdc_collide")
        columns = [row[0] for row in project.run_sql(f"describe {relation}", fetch="all")]
        assert columns == ["id", "_cdc_ver", "name"]
        assert _rows(project, relation) == [(1, "keep-me", "a")]
