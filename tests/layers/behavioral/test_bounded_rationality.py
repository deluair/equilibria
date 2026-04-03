import pytest
import numpy as np
from app.layers.behavioral.bounded_rationality import BoundedRationality


def test_instantiation():
    model = BoundedRationality()
    assert model is not None


def test_layer_id():
    model = BoundedRationality()
    assert model.layer_id == "l13"


def test_name():
    model = BoundedRationality()
    assert model.name == "Bounded Rationality"


async def test_compute_empty_db_returns_dict(db_conn):
    model = BoundedRationality()
    result = await model.compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score_key(db_conn):
    model = BoundedRationality()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "score" in result


async def test_compute_empty_db_score_is_none(db_conn):
    model = BoundedRationality()
    result = await model.compute(db_conn, country_iso3="USA")
    assert result["score"] is None


async def test_run_returns_layer_id(db_conn):
    model = BoundedRationality()
    result = await model.run(db_conn, country_iso3="USA")
    assert result["layer_id"] == "l13"


async def test_run_returns_signal(db_conn):
    model = BoundedRationality()
    result = await model.run(db_conn, country_iso3="USA")
    assert "signal" in result


async def test_compute_with_multiple_series_returns_score(db_conn):
    # Insert 3 series with 25 data points each
    for j in range(3):
        await db_conn.execute(
            "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
            ("fred", f"SER_{j}", "JPN", f"series {j}"),
        )
        cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
        row = await cursor.fetchone()
        sid = row[0]
        np.random.seed(j)
        for i in range(25):
            await db_conn.execute(
                "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
                (sid, f"{2000 + i}-01-01", float(np.random.normal(50, 10))),
            )

    model = BoundedRationality()
    result = await model.compute(db_conn, country_iso3="JPN")
    assert isinstance(result, dict)
    assert "score" in result
    if result["score"] is not None:
        assert 0 <= result["score"] <= 100


async def test_compute_with_data_has_satisficing_key(db_conn):
    for j in range(2):
        await db_conn.execute(
            "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
            ("wdi", f"WSERIES_{j}", "AUS", f"wdi series {j}"),
        )
        cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
        row = await cursor.fetchone()
        sid = row[0]
        for i in range(25):
            await db_conn.execute(
                "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
                (sid, f"{2000 + i}-01-01", float(i) + float(j) * 10),
            )

    model = BoundedRationality()
    result = await model.compute(db_conn, country_iso3="AUS")
    if result.get("score") is not None:
        assert "satisficing" in result
        assert "choice_overload" in result
        assert "attention_allocation" in result
        assert "rational_inattention" in result


def test_satisficing_test_static():
    np.random.seed(0)
    values = np.concatenate([
        np.random.normal(30, 5, 20),
        np.random.normal(60, 1, 20),
    ])
    result = BoundedRationality._satisficing_test(values)
    assert "is_satisficing" in result
    assert "aspiration_proxy" in result
    assert isinstance(result["is_satisficing"], bool)


def test_choice_overload_static():
    series_data = {
        f"s{i}": list(np.random.normal(50, 10, 15)) for i in range(5)
    }
    result = BoundedRationality._choice_overload(series_data)
    assert "overload_index" in result
    assert 0 <= result["overload_index"] <= 1
