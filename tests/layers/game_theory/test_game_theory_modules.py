"""Unit tests for game_theory layer modules."""

import pytest

VALID_SIGNALS = {"UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS"}


# ---------------------------------------------------------------------------
# RentSeeking
# ---------------------------------------------------------------------------


def test_rent_seeking_instantiation():
    from app.layers.game_theory.rent_seeking import RentSeeking
    assert RentSeeking() is not None


def test_rent_seeking_layer_id():
    from app.layers.game_theory.rent_seeking import RentSeeking
    assert RentSeeking().layer_id == "lGT"


async def test_rent_seeking_compute_empty_db_returns_dict(db_conn):
    from app.layers.game_theory.rent_seeking import RentSeeking
    result = await RentSeeking().compute(db_conn, country_iso3="NGA")
    assert isinstance(result, dict)


async def test_rent_seeking_compute_empty_db_score_is_none(db_conn):
    from app.layers.game_theory.rent_seeking import RentSeeking
    result = await RentSeeking().compute(db_conn, country_iso3="NGA")
    assert result.get("score") is None


async def test_rent_seeking_run_valid_signal(db_conn):
    from app.layers.game_theory.rent_seeking import RentSeeking
    result = await RentSeeking().run(db_conn, country_iso3="NGA")
    assert result["signal"] in VALID_SIGNALS


async def test_rent_seeking_run_has_layer_id(db_conn):
    from app.layers.game_theory.rent_seeking import RentSeeking
    result = await RentSeeking().run(db_conn, country_iso3="NGA")
    assert result["layer_id"] == "lGT"


# ---------------------------------------------------------------------------
# PublicGoodsProvision
# ---------------------------------------------------------------------------


def test_public_goods_provision_instantiation():
    from app.layers.game_theory.public_goods_provision import PublicGoodsProvision
    assert PublicGoodsProvision() is not None


def test_public_goods_provision_layer_id():
    from app.layers.game_theory.public_goods_provision import PublicGoodsProvision
    assert PublicGoodsProvision().layer_id == "lGT"


async def test_public_goods_provision_compute_empty_db_returns_dict(db_conn):
    from app.layers.game_theory.public_goods_provision import PublicGoodsProvision
    result = await PublicGoodsProvision().compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)


async def test_public_goods_provision_compute_empty_db_score_is_none(db_conn):
    from app.layers.game_theory.public_goods_provision import PublicGoodsProvision
    result = await PublicGoodsProvision().compute(db_conn, country_iso3="USA")
    assert result.get("score") is None


async def test_public_goods_provision_run_valid_signal(db_conn):
    from app.layers.game_theory.public_goods_provision import PublicGoodsProvision
    result = await PublicGoodsProvision().run(db_conn, country_iso3="USA")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# CooperativeBargaining
# ---------------------------------------------------------------------------


def test_cooperative_bargaining_instantiation():
    from app.layers.game_theory.cooperative_bargaining import CooperativeBargaining
    assert CooperativeBargaining() is not None


def test_cooperative_bargaining_layer_id():
    from app.layers.game_theory.cooperative_bargaining import CooperativeBargaining
    assert CooperativeBargaining().layer_id == "lGT"


async def test_cooperative_bargaining_compute_empty_db_returns_dict(db_conn):
    from app.layers.game_theory.cooperative_bargaining import CooperativeBargaining
    result = await CooperativeBargaining().compute(db_conn, country_iso3="DEU")
    assert isinstance(result, dict)


async def test_cooperative_bargaining_compute_empty_db_score_is_none(db_conn):
    from app.layers.game_theory.cooperative_bargaining import CooperativeBargaining
    result = await CooperativeBargaining().compute(db_conn, country_iso3="DEU")
    assert result.get("score") is None


async def test_cooperative_bargaining_run_valid_signal(db_conn):
    from app.layers.game_theory.cooperative_bargaining import CooperativeBargaining
    result = await CooperativeBargaining().run(db_conn, country_iso3="DEU")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# MechanismDesign
# ---------------------------------------------------------------------------


def test_mechanism_design_instantiation():
    from app.layers.game_theory.mechanism_design import MechanismDesign
    assert MechanismDesign() is not None


def test_mechanism_design_layer_id():
    from app.layers.game_theory.mechanism_design import MechanismDesign
    assert MechanismDesign().layer_id == "lGT"


async def test_mechanism_design_compute_empty_db_returns_dict(db_conn):
    from app.layers.game_theory.mechanism_design import MechanismDesign
    result = await MechanismDesign().compute(db_conn, country_iso3="BRA")
    assert isinstance(result, dict)


async def test_mechanism_design_compute_empty_db_score_is_none(db_conn):
    from app.layers.game_theory.mechanism_design import MechanismDesign
    result = await MechanismDesign().compute(db_conn, country_iso3="BRA")
    assert result.get("score") is None


async def test_mechanism_design_run_valid_signal(db_conn):
    from app.layers.game_theory.mechanism_design import MechanismDesign
    result = await MechanismDesign().run(db_conn, country_iso3="BRA")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# AuctionTheory
# ---------------------------------------------------------------------------


def test_auction_theory_instantiation():
    from app.layers.game_theory.auction_theory import AuctionTheory
    assert AuctionTheory() is not None


def test_auction_theory_layer_id():
    from app.layers.game_theory.auction_theory import AuctionTheory
    assert AuctionTheory().layer_id == "lGT"


async def test_auction_theory_compute_empty_db_returns_dict(db_conn):
    from app.layers.game_theory.auction_theory import AuctionTheory
    result = await AuctionTheory().compute(db_conn, country_iso3="GBR")
    assert isinstance(result, dict)


async def test_auction_theory_compute_empty_db_score_is_none(db_conn):
    from app.layers.game_theory.auction_theory import AuctionTheory
    result = await AuctionTheory().compute(db_conn, country_iso3="GBR")
    assert result.get("score") is None


async def test_auction_theory_run_valid_signal(db_conn):
    from app.layers.game_theory.auction_theory import AuctionTheory
    result = await AuctionTheory().run(db_conn, country_iso3="GBR")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# NashEquilibriumAnalysis
# ---------------------------------------------------------------------------


def test_nash_equilibrium_instantiation():
    from app.layers.game_theory.nash_equilibrium import NashEquilibriumAnalysis
    assert NashEquilibriumAnalysis() is not None


def test_nash_equilibrium_layer_id():
    from app.layers.game_theory.nash_equilibrium import NashEquilibriumAnalysis
    assert NashEquilibriumAnalysis().layer_id == "lGT"


async def test_nash_equilibrium_compute_empty_db_returns_dict(db_conn):
    from app.layers.game_theory.nash_equilibrium import NashEquilibriumAnalysis
    result = await NashEquilibriumAnalysis().compute(db_conn, country_iso3="JPN")
    assert isinstance(result, dict)


async def test_nash_equilibrium_compute_empty_db_score_is_none(db_conn):
    from app.layers.game_theory.nash_equilibrium import NashEquilibriumAnalysis
    result = await NashEquilibriumAnalysis().compute(db_conn, country_iso3="JPN")
    assert result.get("score") is None


async def test_nash_equilibrium_run_valid_signal(db_conn):
    from app.layers.game_theory.nash_equilibrium import NashEquilibriumAnalysis
    result = await NashEquilibriumAnalysis().run(db_conn, country_iso3="JPN")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# SignalingModel
# ---------------------------------------------------------------------------


def test_signaling_model_instantiation():
    from app.layers.game_theory.signaling_model import SignalingModel
    assert SignalingModel() is not None


def test_signaling_model_layer_id():
    from app.layers.game_theory.signaling_model import SignalingModel
    assert SignalingModel().layer_id == "lGT"


async def test_signaling_model_compute_empty_db_returns_dict(db_conn):
    from app.layers.game_theory.signaling_model import SignalingModel
    result = await SignalingModel().compute(db_conn, country_iso3="IND")
    assert isinstance(result, dict)


async def test_signaling_model_compute_empty_db_score_is_none(db_conn):
    from app.layers.game_theory.signaling_model import SignalingModel
    result = await SignalingModel().compute(db_conn, country_iso3="IND")
    assert result.get("score") is None


async def test_signaling_model_run_valid_signal(db_conn):
    from app.layers.game_theory.signaling_model import SignalingModel
    result = await SignalingModel().run(db_conn, country_iso3="IND")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# RepeatedGames
# ---------------------------------------------------------------------------


def test_repeated_games_instantiation():
    from app.layers.game_theory.repeated_games import RepeatedGames
    assert RepeatedGames() is not None


def test_repeated_games_layer_id():
    from app.layers.game_theory.repeated_games import RepeatedGames
    assert RepeatedGames().layer_id == "lGT"


async def test_repeated_games_compute_empty_db_returns_dict(db_conn):
    from app.layers.game_theory.repeated_games import RepeatedGames
    result = await RepeatedGames().compute(db_conn, country_iso3="CHN")
    assert isinstance(result, dict)


async def test_repeated_games_compute_empty_db_score_is_none(db_conn):
    from app.layers.game_theory.repeated_games import RepeatedGames
    result = await RepeatedGames().compute(db_conn, country_iso3="CHN")
    assert result.get("score") is None


async def test_repeated_games_run_valid_signal(db_conn):
    from app.layers.game_theory.repeated_games import RepeatedGames
    result = await RepeatedGames().run(db_conn, country_iso3="CHN")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# CoalitionFormation
# ---------------------------------------------------------------------------


def test_coalition_formation_instantiation():
    from app.layers.game_theory.coalition_formation import CoalitionFormation
    assert CoalitionFormation() is not None


def test_coalition_formation_layer_id():
    from app.layers.game_theory.coalition_formation import CoalitionFormation
    assert CoalitionFormation().layer_id == "lGT"


async def test_coalition_formation_compute_empty_db_returns_dict(db_conn):
    from app.layers.game_theory.coalition_formation import CoalitionFormation
    result = await CoalitionFormation().compute(db_conn, country_iso3="MEX")
    assert isinstance(result, dict)


async def test_coalition_formation_compute_empty_db_score_is_none(db_conn):
    from app.layers.game_theory.coalition_formation import CoalitionFormation
    result = await CoalitionFormation().compute(db_conn, country_iso3="MEX")
    assert result.get("score") is None


async def test_coalition_formation_run_valid_signal(db_conn):
    from app.layers.game_theory.coalition_formation import CoalitionFormation
    result = await CoalitionFormation().run(db_conn, country_iso3="MEX")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# EvolutionaryDynamics
# ---------------------------------------------------------------------------


def test_evolutionary_dynamics_instantiation():
    from app.layers.game_theory.evolutionary_dynamics import EvolutionaryDynamics
    assert EvolutionaryDynamics() is not None


def test_evolutionary_dynamics_layer_id():
    from app.layers.game_theory.evolutionary_dynamics import EvolutionaryDynamics
    assert EvolutionaryDynamics().layer_id == "lGT"


async def test_evolutionary_dynamics_compute_empty_db_returns_dict(db_conn):
    from app.layers.game_theory.evolutionary_dynamics import EvolutionaryDynamics
    result = await EvolutionaryDynamics().compute(db_conn, country_iso3="ZAF")
    assert isinstance(result, dict)


async def test_evolutionary_dynamics_compute_empty_db_score_is_none(db_conn):
    from app.layers.game_theory.evolutionary_dynamics import EvolutionaryDynamics
    result = await EvolutionaryDynamics().compute(db_conn, country_iso3="ZAF")
    assert result.get("score") is None


async def test_evolutionary_dynamics_run_valid_signal(db_conn):
    from app.layers.game_theory.evolutionary_dynamics import EvolutionaryDynamics
    result = await EvolutionaryDynamics().run(db_conn, country_iso3="ZAF")
    assert result["signal"] in VALID_SIGNALS
