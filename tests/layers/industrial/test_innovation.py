"""Tests for L14 Industrial: InnovationEconomics module."""

from __future__ import annotations

import json

import numpy as np
import pytest
from app.layers.industrial.innovation import InnovationEconomics
from tests.layers.industrial.conftest import _insert_series, _insert_points


# --- instantiation ---

def test_instantiation():
    assert InnovationEconomics() is not None


def test_layer_id():
    assert InnovationEconomics().layer_id == "l14"


def test_name():
    assert InnovationEconomics().name == "Innovation Economics"


# --- empty DB ---

async def test_compute_empty_db_returns_dict(db_conn):
    result = await InnovationEconomics().compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_signal_unavailable(db_conn):
    result = await InnovationEconomics().compute(db_conn, country_iso3="USA")
    assert result.get("signal") == "UNAVAILABLE" or result.get("score") is None


# --- with synthetic data ---

async def _insert_innovation_data(db_conn, n: int = 8):
    """Insert n innovation rows with patent_count and rd_spending metadata."""
    rng = np.random.default_rng(7)
    for i in range(n):
        meta = {
            "patent_count": float(rng.integers(50, 500)),
            "rd_spending": float(rng.uniform(1e6, 5e6)),
            "citations_forward": float(rng.integers(5, 50)),
            "citations_backward": float(rng.integers(2, 20)),
        }
        sid = await _insert_series(
            db_conn, "innovation", f"inn_{i}", "USA",
            f"innovation obs {i}", metadata=meta,
        )
        year = 2015 + i
        await _insert_points(db_conn, sid, [(f"{year}-01-01", float(meta["patent_count"]))])


async def test_compute_with_patent_data_returns_score(db_conn):
    await _insert_innovation_data(db_conn, n=8)
    result = await InnovationEconomics().compute(db_conn, country_iso3="USA")
    if result.get("score") is not None:
        assert 0.0 <= result["score"] <= 100.0


async def test_compute_patent_stats_present(db_conn):
    await _insert_innovation_data(db_conn, n=8)
    result = await InnovationEconomics().compute(db_conn, country_iso3="USA")
    if result.get("score") is not None:
        assert "patent_stats" in result
        assert "mean" in result["patent_stats"]


# --- static helpers ---

def test_compute_trend_increasing():
    arr = np.array([10.0, 20.0, 30.0, 40.0, 50.0])
    trend = InnovationEconomics._compute_trend(arr)
    assert trend is not None
    assert trend["slope"] > 0
    assert 0.0 <= trend["r_squared"] <= 1.0


def test_schumpeterian_test_inverted_u():
    """Construct quadratic data so inverted-U should be detected."""
    hhi = np.linspace(0.0, 1.0, 30)
    # patents peak at hhi = 0.5
    patents = 100.0 - 400.0 * (hhi - 0.5) ** 2 + np.random.default_rng(3).normal(0, 1, 30)
    result = InnovationEconomics._schumpeterian_test(hhi, patents)
    assert result is not None
    assert "inverted_u_detected" in result
    assert result["inverted_u_detected"]
