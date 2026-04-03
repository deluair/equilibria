"""Tests for all 26 L2 Macro layer modules.

Each module gets 4 tests:
1. instantiation
2. compute() with empty DB returns dict with score
3. no exceptions on empty data
4. layer_id == "l2"
"""

import pytest

from app.layers.macro.gdp_decomposition import GDPDecomposition
from app.layers.macro.phillips_curve import PhillipsCurve
from app.layers.macro.okuns_law import OkunsLaw
from app.layers.macro.taylor_rule import TaylorRule
from app.layers.macro.business_cycle import BusinessCycle
from app.layers.macro.fiscal_multiplier import FiscalMultiplier
from app.layers.macro.debt_sustainability import DebtSustainability
from app.layers.macro.twin_deficits import TwinDeficits
from app.layers.macro.erpt import ExchangeRatePassThrough
from app.layers.macro.ppp import PurchasingPowerParity
from app.layers.macro.fci import FinancialConditionsIndex
from app.layers.macro.credit_impulse import CreditImpulse
from app.layers.macro.yield_curve import YieldCurve
from app.layers.macro.inflation_decomposition import InflationDecomposition
from app.layers.macro.monetary_transmission import MonetaryTransmission
from app.layers.macro.output_gap import OutputGap
from app.layers.macro.structural_break import StructuralBreak
from app.layers.macro.recession_probability import RecessionProbability
from app.layers.macro.nowcasting import Nowcasting
from app.layers.macro.var_irf import VARImpulseResponse
from app.layers.macro.dsge_calibration import DSGECalibration
from app.layers.macro.regime_switching import RegimeSwitching
from app.layers.macro.global_var import GlobalVAR
from app.layers.macro.real_interest_rate import RealInterestRate
from app.layers.macro.wage_price_spiral import WagePriceSpiral
from app.layers.macro.fiscal_space import FiscalSpace


class MockDB:
    """Minimal DB stub: execute_fetchall always returns an empty list."""

    async def execute_fetchall(self, sql: str, params: tuple = ()) -> list:
        return []


# ---------------------------------------------------------------------------
# GDPDecomposition
# ---------------------------------------------------------------------------

def test_gdp_decomposition_instantiation():
    assert GDPDecomposition() is not None


async def test_gdp_decomposition_empty_db_returns_score(test_db):
    result = await GDPDecomposition().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_gdp_decomposition_no_exception_empty_data(test_db):
    result = await GDPDecomposition().compute(MockDB())
    assert result is not None


def test_gdp_decomposition_layer_id():
    assert GDPDecomposition.layer_id == "l2"


# ---------------------------------------------------------------------------
# PhillipsCurve
# ---------------------------------------------------------------------------

def test_phillips_curve_instantiation():
    assert PhillipsCurve() is not None


async def test_phillips_curve_empty_db_returns_score(test_db):
    result = await PhillipsCurve().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_phillips_curve_no_exception_empty_data(test_db):
    result = await PhillipsCurve().compute(MockDB())
    assert result is not None


def test_phillips_curve_layer_id():
    assert PhillipsCurve.layer_id == "l2"


# ---------------------------------------------------------------------------
# OkunsLaw
# ---------------------------------------------------------------------------

def test_okuns_law_instantiation():
    assert OkunsLaw() is not None


async def test_okuns_law_empty_db_returns_score(test_db):
    result = await OkunsLaw().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_okuns_law_no_exception_empty_data(test_db):
    result = await OkunsLaw().compute(MockDB())
    assert result is not None


def test_okuns_law_layer_id():
    assert OkunsLaw.layer_id == "l2"


# ---------------------------------------------------------------------------
# TaylorRule
# ---------------------------------------------------------------------------

def test_taylor_rule_instantiation():
    assert TaylorRule() is not None


async def test_taylor_rule_empty_db_returns_score(test_db):
    result = await TaylorRule().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_taylor_rule_no_exception_empty_data(test_db):
    result = await TaylorRule().compute(MockDB())
    assert result is not None


def test_taylor_rule_layer_id():
    assert TaylorRule.layer_id == "l2"


# ---------------------------------------------------------------------------
# BusinessCycle
# ---------------------------------------------------------------------------

def test_business_cycle_instantiation():
    assert BusinessCycle() is not None


async def test_business_cycle_empty_db_returns_score(test_db):
    result = await BusinessCycle().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_business_cycle_no_exception_empty_data(test_db):
    result = await BusinessCycle().compute(MockDB())
    assert result is not None


def test_business_cycle_layer_id():
    assert BusinessCycle.layer_id == "l2"


# ---------------------------------------------------------------------------
# FiscalMultiplier
# ---------------------------------------------------------------------------

def test_fiscal_multiplier_instantiation():
    assert FiscalMultiplier() is not None


async def test_fiscal_multiplier_empty_db_returns_score(test_db):
    result = await FiscalMultiplier().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_fiscal_multiplier_no_exception_empty_data(test_db):
    result = await FiscalMultiplier().compute(MockDB())
    assert result is not None


def test_fiscal_multiplier_layer_id():
    assert FiscalMultiplier.layer_id == "l2"


# ---------------------------------------------------------------------------
# DebtSustainability
# ---------------------------------------------------------------------------

def test_debt_sustainability_instantiation():
    assert DebtSustainability() is not None


async def test_debt_sustainability_empty_db_returns_score(test_db):
    result = await DebtSustainability().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_debt_sustainability_no_exception_empty_data(test_db):
    result = await DebtSustainability().compute(MockDB())
    assert result is not None


def test_debt_sustainability_layer_id():
    assert DebtSustainability.layer_id == "l2"


# ---------------------------------------------------------------------------
# TwinDeficits
# ---------------------------------------------------------------------------

def test_twin_deficits_instantiation():
    assert TwinDeficits() is not None


async def test_twin_deficits_empty_db_returns_score(test_db):
    result = await TwinDeficits().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_twin_deficits_no_exception_empty_data(test_db):
    result = await TwinDeficits().compute(MockDB())
    assert result is not None


def test_twin_deficits_layer_id():
    assert TwinDeficits.layer_id == "l2"


# ---------------------------------------------------------------------------
# ExchangeRatePassThrough (erpt)
# ---------------------------------------------------------------------------

def test_erpt_instantiation():
    assert ExchangeRatePassThrough() is not None


async def test_erpt_empty_db_returns_score(test_db):
    result = await ExchangeRatePassThrough().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_erpt_no_exception_empty_data(test_db):
    result = await ExchangeRatePassThrough().compute(MockDB())
    assert result is not None


def test_erpt_layer_id():
    assert ExchangeRatePassThrough.layer_id == "l2"


# ---------------------------------------------------------------------------
# PurchasingPowerParity (ppp)
# ---------------------------------------------------------------------------

def test_ppp_instantiation():
    assert PurchasingPowerParity() is not None


async def test_ppp_empty_db_returns_score(test_db):
    result = await PurchasingPowerParity().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_ppp_no_exception_empty_data(test_db):
    result = await PurchasingPowerParity().compute(MockDB())
    assert result is not None


def test_ppp_layer_id():
    assert PurchasingPowerParity.layer_id == "l2"


# ---------------------------------------------------------------------------
# FinancialConditionsIndex (fci)
# ---------------------------------------------------------------------------

def test_fci_instantiation():
    assert FinancialConditionsIndex() is not None


async def test_fci_empty_db_returns_score(test_db):
    result = await FinancialConditionsIndex().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_fci_no_exception_empty_data(test_db):
    result = await FinancialConditionsIndex().compute(MockDB())
    assert result is not None


def test_fci_layer_id():
    assert FinancialConditionsIndex.layer_id == "l2"


# ---------------------------------------------------------------------------
# CreditImpulse
# ---------------------------------------------------------------------------

def test_credit_impulse_instantiation():
    assert CreditImpulse() is not None


async def test_credit_impulse_empty_db_returns_score(test_db):
    result = await CreditImpulse().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_credit_impulse_no_exception_empty_data(test_db):
    result = await CreditImpulse().compute(MockDB())
    assert result is not None


def test_credit_impulse_layer_id():
    assert CreditImpulse.layer_id == "l2"


# ---------------------------------------------------------------------------
# YieldCurve
# ---------------------------------------------------------------------------

def test_yield_curve_instantiation():
    assert YieldCurve() is not None


async def test_yield_curve_empty_db_returns_score(test_db):
    result = await YieldCurve().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_yield_curve_no_exception_empty_data(test_db):
    result = await YieldCurve().compute(MockDB())
    assert result is not None


def test_yield_curve_layer_id():
    assert YieldCurve.layer_id == "l2"


# ---------------------------------------------------------------------------
# InflationDecomposition
# ---------------------------------------------------------------------------

def test_inflation_decomposition_instantiation():
    assert InflationDecomposition() is not None


async def test_inflation_decomposition_empty_db_returns_score(test_db):
    result = await InflationDecomposition().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_inflation_decomposition_no_exception_empty_data(test_db):
    result = await InflationDecomposition().compute(MockDB())
    assert result is not None


def test_inflation_decomposition_layer_id():
    assert InflationDecomposition.layer_id == "l2"


# ---------------------------------------------------------------------------
# MonetaryTransmission
# ---------------------------------------------------------------------------

def test_monetary_transmission_instantiation():
    assert MonetaryTransmission() is not None


async def test_monetary_transmission_empty_db_returns_score(test_db):
    result = await MonetaryTransmission().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_monetary_transmission_no_exception_empty_data(test_db):
    result = await MonetaryTransmission().compute(MockDB())
    assert result is not None


def test_monetary_transmission_layer_id():
    assert MonetaryTransmission.layer_id == "l2"


# ---------------------------------------------------------------------------
# OutputGap
# ---------------------------------------------------------------------------

def test_output_gap_instantiation():
    assert OutputGap() is not None


async def test_output_gap_empty_db_returns_score(test_db):
    result = await OutputGap().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_output_gap_no_exception_empty_data(test_db):
    result = await OutputGap().compute(MockDB())
    assert result is not None


def test_output_gap_layer_id():
    assert OutputGap.layer_id == "l2"


# ---------------------------------------------------------------------------
# StructuralBreak
# ---------------------------------------------------------------------------

def test_structural_break_instantiation():
    assert StructuralBreak() is not None


async def test_structural_break_empty_db_returns_score(test_db):
    result = await StructuralBreak().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_structural_break_no_exception_empty_data(test_db):
    result = await StructuralBreak().compute(MockDB())
    assert result is not None


def test_structural_break_layer_id():
    assert StructuralBreak.layer_id == "l2"


# ---------------------------------------------------------------------------
# RecessionProbability
# ---------------------------------------------------------------------------

def test_recession_probability_instantiation():
    assert RecessionProbability() is not None


async def test_recession_probability_empty_db_returns_score(test_db):
    result = await RecessionProbability().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_recession_probability_no_exception_empty_data(test_db):
    result = await RecessionProbability().compute(MockDB())
    assert result is not None


def test_recession_probability_layer_id():
    assert RecessionProbability.layer_id == "l2"


# ---------------------------------------------------------------------------
# Nowcasting
# ---------------------------------------------------------------------------

def test_nowcasting_instantiation():
    assert Nowcasting() is not None


async def test_nowcasting_empty_db_returns_score(test_db):
    result = await Nowcasting().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_nowcasting_no_exception_empty_data(test_db):
    result = await Nowcasting().compute(MockDB())
    assert result is not None


def test_nowcasting_layer_id():
    assert Nowcasting.layer_id == "l2"


# ---------------------------------------------------------------------------
# VARImpulseResponse (var_irf)
# ---------------------------------------------------------------------------

def test_var_irf_instantiation():
    assert VARImpulseResponse() is not None


async def test_var_irf_empty_db_returns_score(test_db):
    result = await VARImpulseResponse().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_var_irf_no_exception_empty_data(test_db):
    result = await VARImpulseResponse().compute(MockDB())
    assert result is not None


def test_var_irf_layer_id():
    assert VARImpulseResponse.layer_id == "l2"


# ---------------------------------------------------------------------------
# DSGECalibration
# ---------------------------------------------------------------------------

def test_dsge_calibration_instantiation():
    assert DSGECalibration() is not None


async def test_dsge_calibration_empty_db_returns_score(test_db):
    result = await DSGECalibration().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_dsge_calibration_no_exception_empty_data(test_db):
    result = await DSGECalibration().compute(MockDB())
    assert result is not None


def test_dsge_calibration_layer_id():
    assert DSGECalibration.layer_id == "l2"


# ---------------------------------------------------------------------------
# RegimeSwitching
# ---------------------------------------------------------------------------

def test_regime_switching_instantiation():
    assert RegimeSwitching() is not None


async def test_regime_switching_empty_db_returns_score(test_db):
    result = await RegimeSwitching().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_regime_switching_no_exception_empty_data(test_db):
    result = await RegimeSwitching().compute(MockDB())
    assert result is not None


def test_regime_switching_layer_id():
    assert RegimeSwitching.layer_id == "l2"


# ---------------------------------------------------------------------------
# GlobalVAR
# ---------------------------------------------------------------------------

def test_global_var_instantiation():
    assert GlobalVAR() is not None


async def test_global_var_empty_db_returns_score(test_db):
    result = await GlobalVAR().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_global_var_no_exception_empty_data(test_db):
    result = await GlobalVAR().compute(MockDB())
    assert result is not None


def test_global_var_layer_id():
    assert GlobalVAR.layer_id == "l2"


# ---------------------------------------------------------------------------
# RealInterestRate
# ---------------------------------------------------------------------------

def test_real_interest_rate_instantiation():
    assert RealInterestRate() is not None


async def test_real_interest_rate_empty_db_returns_score(test_db):
    result = await RealInterestRate().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_real_interest_rate_no_exception_empty_data(test_db):
    result = await RealInterestRate().compute(MockDB())
    assert result is not None


def test_real_interest_rate_layer_id():
    assert RealInterestRate.layer_id == "l2"


# ---------------------------------------------------------------------------
# WagePriceSpiral
# ---------------------------------------------------------------------------

def test_wage_price_spiral_instantiation():
    assert WagePriceSpiral() is not None


async def test_wage_price_spiral_empty_db_returns_score(test_db):
    result = await WagePriceSpiral().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_wage_price_spiral_no_exception_empty_data(test_db):
    result = await WagePriceSpiral().compute(MockDB())
    assert result is not None


def test_wage_price_spiral_layer_id():
    assert WagePriceSpiral.layer_id == "l2"


# ---------------------------------------------------------------------------
# FiscalSpace
# ---------------------------------------------------------------------------

def test_fiscal_space_instantiation():
    assert FiscalSpace() is not None


async def test_fiscal_space_empty_db_returns_score(test_db):
    result = await FiscalSpace().compute(MockDB())
    assert isinstance(result, dict)
    assert "score" in result


async def test_fiscal_space_no_exception_empty_data(test_db):
    result = await FiscalSpace().compute(MockDB())
    assert result is not None


def test_fiscal_space_layer_id():
    assert FiscalSpace.layer_id == "l2"
