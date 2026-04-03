from app.layers.risk.commodity_risk import CommodityRisk
from app.layers.risk.contagion_model import ContagionModel
from app.layers.risk.country_risk_index import CountryRiskIndex
from app.layers.risk.currency_crisis_risk import CurrencyCrisisRisk
from app.layers.risk.global_risk_appetite import GlobalRiskAppetite
from app.layers.risk.macro_volatility import MacroVolatility
from app.layers.risk.political_risk import PoliticalRisk
from app.layers.risk.sovereign_default_risk import SovereignDefaultRisk
from app.layers.risk.tail_risk import TailRisk
from app.layers.risk.var_cvar import VaRCVaR

ALL_MODULES = [
    CountryRiskIndex,
    SovereignDefaultRisk,
    TailRisk,
    VaRCVaR,
    PoliticalRisk,
    CommodityRisk,
    CurrencyCrisisRisk,
    ContagionModel,
    MacroVolatility,
    GlobalRiskAppetite,
]

__all__ = [
    "CountryRiskIndex",
    "SovereignDefaultRisk",
    "TailRisk",
    "VaRCVaR",
    "PoliticalRisk",
    "CommodityRisk",
    "CurrencyCrisisRisk",
    "ContagionModel",
    "MacroVolatility",
    "GlobalRiskAppetite",
    "ALL_MODULES",
]
