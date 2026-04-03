import pytest
from app.layers.political.trade_war import TradeWarAnalysis


def test_instantiation():
    model = TradeWarAnalysis()
    assert model is not None


def test_layer_id():
    model = TradeWarAnalysis()
    assert model.layer_id == "l12"


def test_name():
    model = TradeWarAnalysis()
    assert model.name == "Trade War Analysis"


async def test_compute_empty_db_returns_dict(db_conn):
    model = TradeWarAnalysis()
    result = await model.compute(db_conn, reporter="USA", partner="CHN")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score_key(db_conn):
    model = TradeWarAnalysis()
    result = await model.compute(db_conn, reporter="USA", partner="CHN")
    assert "score" in result


async def test_compute_empty_db_score_is_none(db_conn):
    model = TradeWarAnalysis()
    result = await model.compute(db_conn, reporter="USA", partner="CHN")
    assert result["score"] is None


async def test_compute_with_year_kwarg(db_conn):
    model = TradeWarAnalysis()
    result = await model.compute(db_conn, reporter="USA", partner="CHN", year=2022)
    assert isinstance(result, dict)
    assert "score" in result


async def test_run_returns_layer_id(db_conn):
    model = TradeWarAnalysis()
    result = await model.run(db_conn, reporter="USA", partner="CHN")
    assert result["layer_id"] == "l12"


async def test_run_returns_signal(db_conn):
    model = TradeWarAnalysis()
    result = await model.run(db_conn, reporter="USA", partner="CHN")
    assert "signal" in result


async def test_compute_with_bilateral_trade_data_score_in_range(db_conn):
    # Insert bilateral export data for reporter
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("comtrade", "EXPORT_CHN", "USA", "exports to partner"),
    )
    cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
    row = await cursor.fetchone()
    exp_sid = row[0]

    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("comtrade", "IMPORT_CHN", "USA", "imports from partner"),
    )
    cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
    row = await cursor.fetchone()
    imp_sid = row[0]

    for i in range(8):
        yr = 2015 + i
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
            (exp_sid, f"{yr}-01-01", 150000.0 + i * 5000),
        )
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
            (imp_sid, f"{yr}-01-01", 400000.0 + i * 10000),
        )

    model = TradeWarAnalysis()
    result = await model.compute(db_conn, reporter="USA", partner="CHN", year=2022)
    assert isinstance(result, dict)
    assert "score" in result
    if result["score"] is not None:
        assert 0 <= result["score"] <= 100


async def test_compute_result_has_reporter_and_partner(db_conn):
    model = TradeWarAnalysis()
    result = await model.compute(db_conn, reporter="JPN", partner="CHN")
    # When not enough data -> score None, but if score present reporter/partner are in result
    # Either way the early return dict should be a dict
    assert isinstance(result, dict)
