from app.layers.behavioral_finance.anchoring_inflation import AnchoringInflation
from app.layers.behavioral_finance.financial_herd_behavior import FinancialHerdBehavior
from app.layers.behavioral_finance.financial_literacy_proxy import FinancialLiteracyProxy
from app.layers.behavioral_finance.investor_sentiment_index import InvestorSentimentIndex
from app.layers.behavioral_finance.loss_aversion_premium import LossAversionPremium
from app.layers.behavioral_finance.market_overreaction import MarketOverreaction
from app.layers.behavioral_finance.mental_accounting_bias import MentalAccountingBias
from app.layers.behavioral_finance.overconfidence_investment import OverconfidenceInvestment
from app.layers.behavioral_finance.present_bias_savings import PresentBiasSavings
from app.layers.behavioral_finance.recency_bias_policy import RecencyBiasPolicy

ALL_MODULES = [
    MarketOverreaction,
    InvestorSentimentIndex,
    FinancialHerdBehavior,
    LossAversionPremium,
    PresentBiasSavings,
    OverconfidenceInvestment,
    AnchoringInflation,
    FinancialLiteracyProxy,
    MentalAccountingBias,
    RecencyBiasPolicy,
]

__all__ = [
    "MarketOverreaction",
    "InvestorSentimentIndex",
    "FinancialHerdBehavior",
    "LossAversionPremium",
    "PresentBiasSavings",
    "OverconfidenceInvestment",
    "AnchoringInflation",
    "FinancialLiteracyProxy",
    "MentalAccountingBias",
    "RecencyBiasPolicy",
    "ALL_MODULES",
]
