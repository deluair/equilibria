import pytest
from app.layers.political.corruption import CorruptionEconomics


def test_instantiation():
    model = CorruptionEconomics()
    assert model is not None


def test_layer_id():
    model = CorruptionEconomics()
    assert model.layer_id == "l12"


def test_name():
    model = CorruptionEconomics()
    assert model.name == "Corruption Economics"


async def test_compute_empty_db_returns_dict(db_conn):
    model = CorruptionEconomics()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score_key(db_conn):
    model = CorruptionEconomics()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "score" in result


async def test_compute_empty_db_score_is_none(db_conn):
    model = CorruptionEconomics()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert result["score"] is None


async def test_compute_with_n_rent_seekers_kwarg(db_conn):
    model = CorruptionEconomics()
    result = await model.compute(db_conn, country_iso3="BGD", n_rent_seekers=20)
    assert isinstance(result, dict)
    assert "score" in result


async def test_run_returns_layer_id(db_conn):
    model = CorruptionEconomics()
    result = await model.run(db_conn, country_iso3="BGD")
    assert result["layer_id"] == "l12"


async def test_run_returns_signal(db_conn):
    model = CorruptionEconomics()
    result = await model.run(db_conn, country_iso3="BGD")
    assert "signal" in result


async def test_compute_with_corruption_data_returns_numeric_score(db_conn):
    # Insert WGI-style control of corruption series
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("wgi", "CC.EST", "NGA", "control of corruption"),
    )
    cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
    row = await cursor.fetchone()
    series_id = row[0]

    for i, val in enumerate([-0.8, -0.9, -1.0, -1.1, -0.95, -0.85, -0.75]):
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
            (series_id, f"{2015 + i}-01-01", val),
        )

    model = CorruptionEconomics()
    result = await model.compute(db_conn, country_iso3="NGA")
    assert isinstance(result, dict)
    assert "score" in result
    assert result["score"] is not None
    assert 0 <= result["score"] <= 100


async def test_tullock_rent_seeking_always_present(db_conn):
    # Insert corruption data so we get past the early return
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("wgi", "CC2", "BRA", "corruption"),
    )
    cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
    row = await cursor.fetchone()
    sid = row[0]
    await db_conn.execute(
        "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
        (sid, "2022-01-01", -0.5),
    )

    model = CorruptionEconomics()
    result = await model.compute(db_conn, country_iso3="BRA")
    assert "rent_seeking" in result
    rs = result["rent_seeking"]
    assert "dissipation_rate" in rs
    assert 0 < rs["dissipation_rate"] < 1
