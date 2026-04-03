from app.layers.capital_markets.bond_market_depth import BondMarketDepth
from app.layers.capital_markets.capital_cost_index import CapitalCostIndex
from app.layers.capital_markets.capital_market_access import CapitalMarketAccess
from app.layers.capital_markets.equity_risk_premium import EquityRiskPremium
from app.layers.capital_markets.financial_deepening_index import FinancialDeepeningIndex
from app.layers.capital_markets.foreign_portfolio_flows import ForeignPortfolioFlows
from app.layers.capital_markets.institutional_investor_base import InstitutionalInvestorBase
from app.layers.capital_markets.market_concentration_finance import MarketConcentrationFinance
from app.layers.capital_markets.market_liquidity_index import MarketLiquidityIndex
from app.layers.capital_markets.stock_market_development import StockMarketDevelopment

ALL_MODULES = [
    StockMarketDevelopment,
    BondMarketDepth,
    EquityRiskPremium,
    CapitalMarketAccess,
    MarketLiquidityIndex,
    InstitutionalInvestorBase,
    ForeignPortfolioFlows,
    MarketConcentrationFinance,
    CapitalCostIndex,
    FinancialDeepeningIndex,
]

__all__ = [
    "StockMarketDevelopment",
    "BondMarketDepth",
    "EquityRiskPremium",
    "CapitalMarketAccess",
    "MarketLiquidityIndex",
    "InstitutionalInvestorBase",
    "ForeignPortfolioFlows",
    "MarketConcentrationFinance",
    "CapitalCostIndex",
    "FinancialDeepeningIndex",
    "ALL_MODULES",
]
