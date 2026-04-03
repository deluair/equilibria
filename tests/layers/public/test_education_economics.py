import numpy as np
import pytest
from app.layers.public.education_economics import EducationEconomics, _maimonides_predicted


# --- Unit tests for pure functions ---

def test_maimonides_below_threshold():
    enrollments = np.array([30.0, 35.0, 39.0])
    predicted = _maimonides_predicted(enrollments, max_class=40)
    # All below 40 -> 1 segment -> predicted == enrollment
    np.testing.assert_array_equal(predicted, enrollments)


def test_maimonides_at_threshold():
    enrollments = np.array([40.0])
    predicted = _maimonides_predicted(enrollments, max_class=40)
    assert predicted[0] == pytest.approx(40.0)


def test_maimonides_split_at_41():
    enrollments = np.array([41.0])
    predicted = _maimonides_predicted(enrollments, max_class=40)
    # ceil(41/40) = 2 segments -> predicted = 41/2 = 20.5
    assert predicted[0] == pytest.approx(20.5)


def test_maimonides_80():
    enrollments = np.array([80.0])
    predicted = _maimonides_predicted(enrollments, max_class=40)
    # ceil(80/40) = 2 -> predicted = 40.0
    assert predicted[0] == pytest.approx(40.0)


# --- Integration tests with empty DB ---

async def test_instantiation():
    model = EducationEconomics()
    assert model is not None


async def test_layer_id():
    model = EducationEconomics()
    assert model.layer_id == "l10"


async def test_name():
    model = EducationEconomics()
    assert model.name == "Education Economics"


async def test_compute_returns_dict(db_conn):
    model = EducationEconomics()
    result = await model.compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)


async def test_compute_has_score(db_conn):
    model = EducationEconomics()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "score" in result
    assert isinstance(result["score"], (int, float))


async def test_compute_score_in_range(db_conn):
    model = EducationEconomics()
    result = await model.compute(db_conn, country_iso3="USA")
    assert 0.0 <= result["score"] <= 100.0


async def test_compute_has_production_function_key(db_conn):
    model = EducationEconomics()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "production_function" in result["results"]


async def test_compute_has_class_size_key(db_conn):
    model = EducationEconomics()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "class_size_angrist_lavy" in result["results"]


async def test_compute_has_teacher_value_added_key(db_conn):
    model = EducationEconomics()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "teacher_value_added" in result["results"]


async def test_compute_has_school_choice_key(db_conn):
    model = EducationEconomics()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "school_choice" in result["results"]


async def test_compute_has_indicators_key(db_conn):
    model = EducationEconomics()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "indicators" in result["results"]
    ind = result["results"]["indicators"]
    assert "spending_pct_gdp" in ind
    assert "literacy_rate" in ind


async def test_compute_error_on_insufficient_data(db_conn):
    model = EducationEconomics()
    result = await model.compute(db_conn, country_iso3="USA")
    # With empty DB, cross-country analysis should report error
    pf = result["results"]["production_function"]
    assert "error" in pf


async def test_run_adds_signal(db_conn):
    model = EducationEconomics()
    result = await model.run(db_conn, country_iso3="USA")
    assert "signal" in result
    assert result["signal"] in ("STABLE", "WATCH", "STRESS", "CRISIS", "UNAVAILABLE")
