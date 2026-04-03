"""Tests for BaseCollector ABC and its pipeline."""

import pytest

from app.collectors.base import BaseCollector


# ---------------------------------------------------------------------------
# Minimal concrete subclass for testing
# ---------------------------------------------------------------------------

class _SuccessCollector(BaseCollector):
    """Collector that returns a fixed list of data points."""
    name = "test_success"

    async def collect(self) -> list[dict]:
        return [
            {"value": 1.0, "date": "2024-01-01"},
            {"value": 2.0, "date": "2024-02-01"},
            {"value": float("nan"), "date": "2024-03-01"},  # intentionally invalid
        ]

    async def validate(self, data: list[dict]) -> list[dict]:
        # Drop NaN values
        return [r for r in data if r["value"] == r["value"]]

    async def store(self, data: list[dict]) -> int:
        return len(data)


class _DictCollector(BaseCollector):
    """Collector that returns a summary dict (not a list)."""
    name = "test_dict"

    async def collect(self) -> dict:
        return {"series_fetched": 5, "points": 100}


class _FailCollector(BaseCollector):
    """Collector that always raises during collect."""
    name = "test_fail"

    async def collect(self):
        raise RuntimeError("simulated network error")


# ---------------------------------------------------------------------------
# Instantiation
# ---------------------------------------------------------------------------

def test_base_collector_cannot_instantiate():
    """BaseCollector is abstract and must not be directly instantiatable."""
    with pytest.raises(TypeError):
        BaseCollector()


def test_subclass_instantiation():
    """Concrete subclass instantiates without error."""
    c = _SuccessCollector()
    assert c.name == "test_success"


def test_subclass_max_retries_default():
    """Default max_retries is 3."""
    c = _SuccessCollector()
    assert c.max_retries == 3


def test_subclass_timeout_default():
    """Default timeout is 30 seconds."""
    c = _SuccessCollector()
    assert c.timeout == 30


# ---------------------------------------------------------------------------
# Pipeline: collect -> validate -> store
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_success_pipeline():
    """run() executes collect->validate->store and returns correct summary."""
    c = _SuccessCollector()
    result = await c.run()
    assert result["status"] == "success"
    # 3 collected, 2 valid (one NaN dropped), 2 stored
    assert result["collected"] == 3
    assert result["valid"] == 2
    assert result["stored"] == 2


@pytest.mark.asyncio
async def test_run_dict_result_passthrough():
    """When collect() returns a dict, run() merges it into the status response."""
    c = _DictCollector()
    result = await c.run()
    assert result["status"] == "success"
    assert result["series_fetched"] == 5
    assert result["points"] == 100


@pytest.mark.asyncio
async def test_run_handles_collect_exception():
    """run() catches exceptions from collect() and returns status=failed."""
    c = _FailCollector()
    result = await c.run()
    assert result["status"] == "failed"
    assert "error" in result
    assert "simulated network error" in result["error"]


# ---------------------------------------------------------------------------
# Validate default pass-through
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_default_validate_returns_all():
    """Default validate() returns all rows unchanged."""

    class _NoOpCollector(BaseCollector):
        name = "noop"
        async def collect(self): return []

    c = _NoOpCollector()
    rows = [{"a": 1}, {"b": 2}]
    out = await c.validate(rows)
    assert out == rows


# ---------------------------------------------------------------------------
# Store default
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_default_store_returns_count():
    """Default store() returns the count of items passed to it."""

    class _NoOpCollector(BaseCollector):
        name = "noop2"
        async def collect(self): return []

    c = _NoOpCollector()
    count = await c.store([{"x": 1}, {"y": 2}, {"z": 3}])
    assert count == 3
