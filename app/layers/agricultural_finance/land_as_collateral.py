"""Land as collateral: strength of land rights enabling agricultural credit access.

Methodology
-----------
**Land-as-collateral effectiveness** proxied from:
    - RL.EST: Rule of Law estimate (World Bank Governance Indicators, -2.5 to +2.5).
      Strong rule of law underpins property rights and contract enforcement, enabling
      land to function as collateral.
    - FX.OWN.TOTL.ZS: Account ownership at financial institution (% population 15+).
      Financial inclusion proxy; access to formal finance complements land rights.

Where rule of law is weak (negative RL.EST) and financial inclusion is low,
land ownership cannot be effectively leveraged as collateral, reducing agricultural
credit access. The joint indicator captures institutional quality.

    rule_of_law_norm: map -2.5..+2.5 to 0..1
    inclusion_norm: FX.OWN.TOTL.ZS / 100

    collateral_effectiveness = 0.6 * rule_of_law_norm + 0.4 * inclusion_norm
    score = (1 - collateral_effectiveness) * 100

Score (0-100): higher = worse land collateral conditions (more stress).

Sources: World Bank WGI (RL.EST), Global Findex (FX.OWN.TOTL.ZS)
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase

_SQL = """
    SELECT value FROM data_points
    WHERE series_id = (
        SELECT id FROM data_series
        WHERE indicator_code = ? OR name LIKE ?
    )
    ORDER BY date DESC LIMIT 15
"""

_RL_MIN, _RL_MAX = -2.5, 2.5


class LandAsCollateral(LayerBase):
    layer_id = "lAF"
    name = "Land as Collateral"

    async def compute(self, db, **kwargs) -> dict:
        code_rl, name_rl = "RL.EST", "%rule of law%"
        code_own, name_own = "FX.OWN.TOTL.ZS", "%account ownership at a financial institution%"

        rows_rl = await db.fetch_all(_SQL, (code_rl, name_rl))
        rows_own = await db.fetch_all(_SQL, (code_own, name_own))

        if not rows_rl and not rows_own:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no rule of law or account ownership data"}

        rl_vals = [float(r["value"]) for r in rows_rl if r["value"] is not None]
        own_vals = [float(r["value"]) for r in rows_own if r["value"] is not None]

        rl = statistics.mean(rl_vals[:3]) if rl_vals else None
        account_own = statistics.mean(own_vals[:3]) if own_vals else None

        metrics: dict = {
            "rule_of_law_estimate": round(rl, 4) if rl is not None else None,
            "account_ownership_pct": round(account_own, 2) if account_own is not None else None,
        }

        if rl is None and account_own is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable data", "metrics": metrics}

        rl_norm = (rl - _RL_MIN) / (_RL_MAX - _RL_MIN) if rl is not None else 0.5
        inclusion_norm = account_own / 100.0 if account_own is not None else 0.5

        rl_norm = max(0.0, min(1.0, rl_norm))
        inclusion_norm = max(0.0, min(1.0, inclusion_norm))

        effectiveness = 0.6 * rl_norm + 0.4 * inclusion_norm
        score = max(0.0, min(100.0, (1.0 - effectiveness) * 100.0))

        metrics["rule_of_law_norm"] = round(rl_norm, 4)
        metrics["inclusion_norm"] = round(inclusion_norm, 4)
        metrics["collateral_effectiveness"] = round(effectiveness, 4)

        return {
            "score": round(score, 2),
            "metrics": metrics,
        }
