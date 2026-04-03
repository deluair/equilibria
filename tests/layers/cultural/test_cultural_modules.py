"""Unit tests for cultural layer modules."""

import pytest

VALID_SIGNALS = {"UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS"}


# ---------------------------------------------------------------------------
# SocialCapital
# ---------------------------------------------------------------------------


def test_social_capital_instantiation():
    from app.layers.cultural.social_capital import SocialCapital
    assert SocialCapital() is not None


def test_social_capital_layer_id():
    from app.layers.cultural.social_capital import SocialCapital
    assert SocialCapital().layer_id == "lCU"


async def test_social_capital_compute_empty_db_returns_dict(db_conn):
    from app.layers.cultural.social_capital import SocialCapital
    result = await SocialCapital().compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)


async def test_social_capital_compute_empty_db_score_is_none(db_conn):
    from app.layers.cultural.social_capital import SocialCapital
    result = await SocialCapital().compute(db_conn, country_iso3="USA")
    assert result.get("score") is None


async def test_social_capital_run_valid_signal(db_conn):
    from app.layers.cultural.social_capital import SocialCapital
    result = await SocialCapital().run(db_conn, country_iso3="USA")
    assert result["signal"] in VALID_SIGNALS


async def test_social_capital_run_has_layer_id(db_conn):
    from app.layers.cultural.social_capital import SocialCapital
    result = await SocialCapital().run(db_conn, country_iso3="USA")
    assert result["layer_id"] == "lCU"


# ---------------------------------------------------------------------------
# TrustInstitutions
# ---------------------------------------------------------------------------


def test_trust_institutions_instantiation():
    from app.layers.cultural.trust_institutions import TrustInstitutions
    assert TrustInstitutions() is not None


def test_trust_institutions_layer_id():
    from app.layers.cultural.trust_institutions import TrustInstitutions
    assert TrustInstitutions().layer_id == "lCU"


async def test_trust_institutions_compute_empty_db_returns_dict(db_conn):
    from app.layers.cultural.trust_institutions import TrustInstitutions
    result = await TrustInstitutions().compute(db_conn, country_iso3="DEU")
    assert isinstance(result, dict)


async def test_trust_institutions_compute_empty_db_score_is_none(db_conn):
    from app.layers.cultural.trust_institutions import TrustInstitutions
    result = await TrustInstitutions().compute(db_conn, country_iso3="DEU")
    assert result.get("score") is None


async def test_trust_institutions_run_valid_signal(db_conn):
    from app.layers.cultural.trust_institutions import TrustInstitutions
    result = await TrustInstitutions().run(db_conn, country_iso3="DEU")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# DiasporaEconomics
# ---------------------------------------------------------------------------


def test_diaspora_economics_instantiation():
    from app.layers.cultural.diaspora_economics import DiasporaEconomics
    assert DiasporaEconomics() is not None


def test_diaspora_economics_layer_id():
    from app.layers.cultural.diaspora_economics import DiasporaEconomics
    assert DiasporaEconomics().layer_id == "lCU"


async def test_diaspora_economics_compute_empty_db_returns_dict(db_conn):
    from app.layers.cultural.diaspora_economics import DiasporaEconomics
    result = await DiasporaEconomics().compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_diaspora_economics_compute_empty_db_score_is_none(db_conn):
    from app.layers.cultural.diaspora_economics import DiasporaEconomics
    result = await DiasporaEconomics().compute(db_conn, country_iso3="BGD")
    assert result.get("score") is None


async def test_diaspora_economics_run_valid_signal(db_conn):
    from app.layers.cultural.diaspora_economics import DiasporaEconomics
    result = await DiasporaEconomics().run(db_conn, country_iso3="BGD")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# CulturalConsumption
# ---------------------------------------------------------------------------


def test_cultural_consumption_instantiation():
    from app.layers.cultural.cultural_consumption import CulturalConsumption
    assert CulturalConsumption() is not None


def test_cultural_consumption_layer_id():
    from app.layers.cultural.cultural_consumption import CulturalConsumption
    assert CulturalConsumption().layer_id == "lCU"


async def test_cultural_consumption_compute_empty_db_returns_dict(db_conn):
    from app.layers.cultural.cultural_consumption import CulturalConsumption
    result = await CulturalConsumption().compute(db_conn, country_iso3="FRA")
    assert isinstance(result, dict)


async def test_cultural_consumption_compute_empty_db_score_is_none(db_conn):
    from app.layers.cultural.cultural_consumption import CulturalConsumption
    result = await CulturalConsumption().compute(db_conn, country_iso3="FRA")
    assert result.get("score") is None


async def test_cultural_consumption_run_valid_signal(db_conn):
    from app.layers.cultural.cultural_consumption import CulturalConsumption
    result = await CulturalConsumption().run(db_conn, country_iso3="FRA")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# LanguageEconomics
# ---------------------------------------------------------------------------


def test_language_economics_instantiation():
    from app.layers.cultural.language_economics import LanguageEconomics
    assert LanguageEconomics() is not None


def test_language_economics_layer_id():
    from app.layers.cultural.language_economics import LanguageEconomics
    assert LanguageEconomics().layer_id == "lCU"


async def test_language_economics_compute_empty_db_returns_dict(db_conn):
    from app.layers.cultural.language_economics import LanguageEconomics
    result = await LanguageEconomics().compute(db_conn, country_iso3="IND")
    assert isinstance(result, dict)


async def test_language_economics_compute_empty_db_score_is_none(db_conn):
    from app.layers.cultural.language_economics import LanguageEconomics
    result = await LanguageEconomics().compute(db_conn, country_iso3="IND")
    assert result.get("score") is None


async def test_language_economics_run_valid_signal(db_conn):
    from app.layers.cultural.language_economics import LanguageEconomics
    result = await LanguageEconomics().run(db_conn, country_iso3="IND")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# CreativeIndustries
# ---------------------------------------------------------------------------


def test_creative_industries_instantiation():
    from app.layers.cultural.creative_industries import CreativeIndustries
    assert CreativeIndustries() is not None


def test_creative_industries_layer_id():
    from app.layers.cultural.creative_industries import CreativeIndustries
    assert CreativeIndustries().layer_id == "lCU"


async def test_creative_industries_compute_empty_db_returns_dict(db_conn):
    from app.layers.cultural.creative_industries import CreativeIndustries
    result = await CreativeIndustries().compute(db_conn, country_iso3="KOR")
    assert isinstance(result, dict)


async def test_creative_industries_compute_empty_db_score_is_none(db_conn):
    from app.layers.cultural.creative_industries import CreativeIndustries
    result = await CreativeIndustries().compute(db_conn, country_iso3="KOR")
    assert result.get("score") is None


async def test_creative_industries_run_valid_signal(db_conn):
    from app.layers.cultural.creative_industries import CreativeIndustries
    result = await CreativeIndustries().run(db_conn, country_iso3="KOR")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# NormsEnforcement
# ---------------------------------------------------------------------------


def test_norms_enforcement_instantiation():
    from app.layers.cultural.norms_enforcement import NormsEnforcement
    assert NormsEnforcement() is not None


def test_norms_enforcement_layer_id():
    from app.layers.cultural.norms_enforcement import NormsEnforcement
    assert NormsEnforcement().layer_id == "lCU"


async def test_norms_enforcement_compute_empty_db_returns_dict(db_conn):
    from app.layers.cultural.norms_enforcement import NormsEnforcement
    result = await NormsEnforcement().compute(db_conn, country_iso3="NGA")
    assert isinstance(result, dict)


async def test_norms_enforcement_compute_empty_db_score_is_none(db_conn):
    from app.layers.cultural.norms_enforcement import NormsEnforcement
    result = await NormsEnforcement().compute(db_conn, country_iso3="NGA")
    assert result.get("score") is None


async def test_norms_enforcement_run_valid_signal(db_conn):
    from app.layers.cultural.norms_enforcement import NormsEnforcement
    result = await NormsEnforcement().run(db_conn, country_iso3="NGA")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# ReligionEconomics
# ---------------------------------------------------------------------------


def test_religion_economics_instantiation():
    from app.layers.cultural.religion_economics import ReligionEconomics
    assert ReligionEconomics() is not None


def test_religion_economics_layer_id():
    from app.layers.cultural.religion_economics import ReligionEconomics
    assert ReligionEconomics().layer_id == "lCU"


async def test_religion_economics_compute_empty_db_returns_dict(db_conn):
    from app.layers.cultural.religion_economics import ReligionEconomics
    result = await ReligionEconomics().compute(db_conn, country_iso3="SAU")
    assert isinstance(result, dict)


async def test_religion_economics_compute_empty_db_score_is_none(db_conn):
    from app.layers.cultural.religion_economics import ReligionEconomics
    result = await ReligionEconomics().compute(db_conn, country_iso3="SAU")
    assert result.get("score") is None


async def test_religion_economics_run_valid_signal(db_conn):
    from app.layers.cultural.religion_economics import ReligionEconomics
    result = await ReligionEconomics().run(db_conn, country_iso3="SAU")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# IndigenousEconomics
# ---------------------------------------------------------------------------


def test_indigenous_economics_instantiation():
    from app.layers.cultural.indigenous_economics import IndigenousEconomics
    assert IndigenousEconomics() is not None


def test_indigenous_economics_layer_id():
    from app.layers.cultural.indigenous_economics import IndigenousEconomics
    assert IndigenousEconomics().layer_id == "lCU"


async def test_indigenous_economics_compute_empty_db_returns_dict(db_conn):
    from app.layers.cultural.indigenous_economics import IndigenousEconomics
    result = await IndigenousEconomics().compute(db_conn, country_iso3="AUS")
    assert isinstance(result, dict)


async def test_indigenous_economics_compute_empty_db_score_is_none(db_conn):
    from app.layers.cultural.indigenous_economics import IndigenousEconomics
    result = await IndigenousEconomics().compute(db_conn, country_iso3="AUS")
    assert result.get("score") is None


async def test_indigenous_economics_run_valid_signal(db_conn):
    from app.layers.cultural.indigenous_economics import IndigenousEconomics
    result = await IndigenousEconomics().run(db_conn, country_iso3="AUS")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# CulturalDistance
# ---------------------------------------------------------------------------


def test_cultural_distance_instantiation():
    from app.layers.cultural.cultural_distance import CulturalDistance
    assert CulturalDistance() is not None


def test_cultural_distance_layer_id():
    from app.layers.cultural.cultural_distance import CulturalDistance
    assert CulturalDistance().layer_id == "lCU"


async def test_cultural_distance_compute_empty_db_returns_dict(db_conn):
    from app.layers.cultural.cultural_distance import CulturalDistance
    result = await CulturalDistance().compute(db_conn, country_iso3="JPN")
    assert isinstance(result, dict)


async def test_cultural_distance_compute_empty_db_score_is_none(db_conn):
    from app.layers.cultural.cultural_distance import CulturalDistance
    result = await CulturalDistance().compute(db_conn, country_iso3="JPN")
    assert result.get("score") is None


async def test_cultural_distance_run_valid_signal(db_conn):
    from app.layers.cultural.cultural_distance import CulturalDistance
    result = await CulturalDistance().run(db_conn, country_iso3="JPN")
    assert result["signal"] in VALID_SIGNALS
