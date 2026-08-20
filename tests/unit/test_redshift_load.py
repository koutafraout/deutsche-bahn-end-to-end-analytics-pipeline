import pytest

from warehouse.redshift_load import load_monthly
from warehouse.redshift_load.load_monthly import build_copy_sql, build_delete_sql, load_month_to_redshift


def test_build_delete_sql_scopes_to_month():
    assert build_delete_sql("2026-07") == (
        "DELETE FROM db_monthly.raw_observations WHERE source_month = '2026-07'"
    )


def test_build_delete_sql_rejects_invalid_month():
    with pytest.raises(ValueError):
        build_delete_sql("not-a-month")


def test_build_copy_sql_uses_month_specific_s3_key():
    sql = build_copy_sql("2026-07", bucket="my-bucket", iam_role="arn:aws:iam::123:role/x")
    assert "FROM 's3://my-bucket/bronze/monthly-raw/year=2026/month=07/data-2026-07.parquet'" in sql
    assert "IAM_ROLE 'arn:aws:iam::123:role/x'" in sql
    assert "FORMAT AS PARQUET" in sql


def test_build_copy_sql_column_list_excludes_source_month():
    sql = build_copy_sql("2026-07", bucket="my-bucket", iam_role="role")
    assert "source_month" not in sql.split("FROM")[0]


def test_build_copy_sql_rejects_invalid_month():
    with pytest.raises(ValueError):
        build_copy_sql("2026-13", bucket="my-bucket", iam_role="role")


class FakeCursor:
    """Fakes just enough of redshift_connector's cursor to drive
    load_month_to_redshift without a live connection: the first
    SELECT COUNT(*) call reports what's already loaded, the second (only
    reached if a COPY actually ran) reports the post-COPY count."""

    def __init__(self, existing_row_count: int, final_row_count: int):
        self.existing_row_count = existing_row_count
        self.final_row_count = final_row_count
        self.executed: list[str] = []
        self._select_calls = 0

    def execute(self, sql, params=None):
        self.executed.append(sql)

    def fetchone(self):
        self._select_calls += 1
        return (self.existing_row_count,) if self._select_calls == 1 else (self.final_row_count,)


class FakeConnection:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


@pytest.fixture(autouse=True)
def iam_role(monkeypatch):
    monkeypatch.setattr(load_monthly, "REDSHIFT_S3_IAM_ROLE", "arn:aws:iam::123:role/x")


def test_skips_reload_when_month_already_loaded():
    cursor = FakeCursor(existing_row_count=100, final_row_count=999)
    conn = FakeConnection(cursor)

    result = load_month_to_redshift("2026-07", connection=conn)

    assert result == 100
    assert not any("DELETE FROM" in sql for sql in cursor.executed)
    assert not any("COPY db_monthly" in sql for sql in cursor.executed)
    assert conn.committed


def test_force_reloads_when_month_already_loaded():
    cursor = FakeCursor(existing_row_count=100, final_row_count=150)
    conn = FakeConnection(cursor)

    result = load_month_to_redshift("2026-07", connection=conn, force=True)

    assert result == 150
    assert any("DELETE FROM" in sql for sql in cursor.executed)
    assert any("COPY db_monthly" in sql for sql in cursor.executed)


def test_copies_when_month_not_yet_loaded():
    cursor = FakeCursor(existing_row_count=0, final_row_count=150)
    conn = FakeConnection(cursor)

    result = load_month_to_redshift("2026-07", connection=conn)

    assert result == 150
    assert not any("DELETE FROM" in sql for sql in cursor.executed)
    assert any("COPY db_monthly" in sql for sql in cursor.executed)


def test_rolls_back_on_failure():
    class FailingCursor(FakeCursor):
        def execute(self, sql, params=None):
            super().execute(sql, params)
            if "COPY db_monthly" in sql:
                raise RuntimeError("simulated COPY failure")

    cursor = FailingCursor(existing_row_count=0, final_row_count=150)
    conn = FakeConnection(cursor)

    with pytest.raises(RuntimeError):
        load_month_to_redshift("2026-07", connection=conn)

    assert conn.rolled_back
    assert not conn.committed
