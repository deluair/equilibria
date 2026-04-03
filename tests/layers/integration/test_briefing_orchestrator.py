import json
import pytest
from app.layers.integration.briefing_orchestrator import BriefingOrchestrator, BriefingType, Priority
from tests.layers.integration.conftest import seed_layer_scores


def test_instantiation():
    m = BriefingOrchestrator()
    assert m.layer_id == "l6"
    assert m.name == "Briefing Orchestrator"


async def test_check_only_returns_queue_no_execution(db_conn):
    m = BriefingOrchestrator()
    result = await m.compute(db_conn, country_iso3="USA", check_only=True)
    assert "queue" in result
    assert "alerts" in result
    # check_only should not write briefing rows
    rows = await db_conn.fetch_all("SELECT id FROM briefings WHERE country_iso3 = 'USA'")
    assert len(rows) == 0


async def test_alert_triggered_by_crisis_composite(db_conn):
    # Insert a CRISIS-level composite score
    await db_conn.execute(
        """
        INSERT INTO analysis_results (analysis_type, country_iso3, layer, parameters, result, score, signal)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("composite_score", "USA", "l6", "{}", "{}", 85.0, "CRISIS"),
    )
    m = BriefingOrchestrator()
    result = await m.compute(db_conn, country_iso3="USA", check_only=True)
    alert_types = [a["type"] for a in result["alerts"]]
    assert "composite_signal" in alert_types


async def test_force_generates_briefings(db_conn):
    await seed_layer_scores(db_conn, country_iso3="GBR")
    m = BriefingOrchestrator()
    result = await m.compute(db_conn, country_iso3="GBR", force=True)
    assert result["n_generated"] > 0
    rows = await db_conn.fetch_all("SELECT id FROM briefings WHERE country_iso3 = 'GBR'")
    assert len(rows) > 0


async def test_specific_briefing_type_generates_one(db_conn):
    await seed_layer_scores(db_conn, country_iso3="FRA")
    m = BriefingOrchestrator()
    result = await m.compute(
        db_conn,
        country_iso3="FRA",
        briefing_type=BriefingType.TRADE_FLASH.value,
        force=True,
    )
    assert result["queue_size"] == 1
