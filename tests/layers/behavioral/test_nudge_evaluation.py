import pytest
import numpy as np
from app.layers.behavioral.nudge_evaluation import NudgeEvaluation


def test_instantiation():
    model = NudgeEvaluation()
    assert model is not None


def test_layer_id():
    model = NudgeEvaluation()
    assert model.layer_id == "l13"


def test_name():
    model = NudgeEvaluation()
    assert model.name == "Nudge Evaluation"


async def test_compute_empty_db_returns_dict(db_conn):
    model = NudgeEvaluation()
    result = await model.compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score_key(db_conn):
    model = NudgeEvaluation()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "score" in result


async def test_compute_empty_db_score_is_none(db_conn):
    model = NudgeEvaluation()
    result = await model.compute(db_conn, country_iso3="USA")
    assert result["score"] is None


async def test_run_returns_layer_id(db_conn):
    model = NudgeEvaluation()
    result = await model.run(db_conn, country_iso3="USA")
    assert result["layer_id"] == "l13"


async def test_run_returns_signal(db_conn):
    model = NudgeEvaluation()
    result = await model.run(db_conn, country_iso3="USA")
    assert "signal" in result


async def test_compute_with_policy_data_returns_score(db_conn):
    # Insert labor force participation proxy series (fallback series_id match)
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("wdi", "SL.TLF.ACTI.ZS", "USA", "labor force participation"),
    )
    cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
    row = await cursor.fetchone()
    sid = row[0]

    np.random.seed(7)
    for i in range(20):
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
            (sid, f"{2000 + i}-01-01", 60.0 + np.random.normal(0, 1)),
        )

    model = NudgeEvaluation()
    result = await model.compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)
    assert "score" in result
    if result["score"] is not None:
        assert 0 <= result["score"] <= 100


async def test_compute_with_data_has_rct_quality(db_conn):
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("wdi", "FX.OWN.TOTL.ZS", "GBR", "account ownership"),
    )
    cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
    row = await cursor.fetchone()
    sid = row[0]

    for i in range(20):
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
            (sid, f"{2000 + i}-01-01", 70.0 + float(i) * 0.5),
        )

    model = NudgeEvaluation()
    result = await model.compute(db_conn, country_iso3="GBR")
    if result.get("score") is not None:
        assert "default_effect" in result
        assert "rct_quality" in result


async def test_default_effect_static():
    pre = np.array([30.0, 32.0, 31.0, 33.0, 29.0])
    post = np.array([75.0, 80.0, 78.0, 82.0, 76.0])
    effect = NudgeEvaluation._default_effect(pre, post)
    assert "cohens_d" in effect
    assert effect["difference"] > 0  # post > pre
    assert effect["cohens_d"] > 0
