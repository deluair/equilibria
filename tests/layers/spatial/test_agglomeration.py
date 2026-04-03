import json
import pytest
from app.layers.spatial.agglomeration import Agglomeration


async def test_instantiation():
    model = Agglomeration()
    assert model is not None


async def test_layer_id():
    model = Agglomeration()
    assert model.layer_id == "l11"


async def test_name():
    model = Agglomeration()
    assert model.name == "Agglomeration Economies"


async def test_compute_empty_db_returns_unavailable(db_conn):
    model = Agglomeration()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert result.get("signal") == "UNAVAILABLE" or result.get("score") is None


async def test_compute_insufficient_cities_returns_unavailable(db_conn):
    """Fewer than 5 cities should return UNAVAILABLE."""
    # Insert only 3 city_population rows
    async with db_conn.conn.execute(
        "INSERT INTO data_series(source, series_id, country_iso3, name) VALUES (?,?,?,?)",
        ("city_population", "city_pop_bgd", "BGD", "City Pop"),
    ) as cur:
        sid = cur.lastrowid
    await db_conn.conn.commit()

    for i, pop in enumerate([5_000_000, 2_000_000, 800_000]):
        await db_conn.conn.execute(
            "INSERT INTO data_points(series_id, date, value) VALUES (?,?,?)",
            (sid, f"2022-0{i+1}", float(pop)),
        )
    await db_conn.conn.commit()

    model = Agglomeration()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert result.get("signal") == "UNAVAILABLE" or result.get("score") is None


async def _seed_cities(db_conn, n=10, country="BGD"):
    async with db_conn.conn.execute(
        "INSERT INTO data_series(source, series_id, country_iso3, name) VALUES (?,?,?,?)",
        ("city_population", "city_pop", country, "City Pop"),
    ) as cur:
        sid = cur.lastrowid
    await db_conn.conn.commit()

    # Zipf-like distribution: rank 1 has 10M, rank k has 10M/k
    for k in range(1, n + 1):
        pop = 10_000_000.0 / k
        await db_conn.conn.execute(
            "INSERT INTO data_points(series_id, date, value) VALUES (?,?,?)",
            (sid, f"2022-{k:02d}", pop),
        )
    await db_conn.conn.commit()


async def test_compute_with_cities_returns_score(db_conn):
    await _seed_cities(db_conn, n=10)
    model = Agglomeration()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert result.get("score") is not None
    assert isinstance(result["score"], float)
    assert 0.0 <= result["score"] <= 100.0


async def test_compute_zipf_keys_present(db_conn):
    await _seed_cities(db_conn, n=10)
    model = Agglomeration()
    result = await model.compute(db_conn, country_iso3="BGD")
    if result.get("score") is not None:
        assert "zipf" in result
        assert "zeta" in result["zipf"]
        assert "r_squared" in result["zipf"]


async def test_compute_city_distribution_keys_present(db_conn):
    await _seed_cities(db_conn, n=10)
    model = Agglomeration()
    result = await model.compute(db_conn, country_iso3="BGD")
    if result.get("score") is not None:
        assert "city_distribution" in result
        assert result["city_distribution"]["primacy_ratio"] is not None


async def test_run_adds_layer_id(db_conn):
    model = Agglomeration()
    result = await model.run(db_conn, country_iso3="BGD")
    assert "layer_id" in result
    assert result["layer_id"] == "l11"
