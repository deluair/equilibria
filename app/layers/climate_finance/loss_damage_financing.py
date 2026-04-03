"""Loss and damage financing: economic losses from climate events vs insurance coverage.

Methodology
-----------
**Loss and damage concept** (UNFCCC, COP27 Santiago Network):
    Residual climate-related losses beyond what adaptation can prevent.
    Includes both economic (quantifiable) and non-economic (culture, biodiversity)
    losses. Non-economic losses are out of scope here.

    Economic loss = direct_asset_loss + productivity_loss + fiscal_cost

**Protection gap** (Swiss Re, Munich Re):
    protection_gap = economic_loss - insured_loss
    gap_ratio = protection_gap / economic_loss

    Global average: ~75-80% of climate-related losses are uninsured.
    Developing countries: 90-97% uninsured.

**Loss trend** (Munich Re NatCatSERVICE):
    Tracks economic loss from natural catastrophes over time.
    Normalization by GDP removes economic growth effects.

**COP27 Fund** (Loss and Damage Fund, COP27/COP28):
    Fund established Nov 2022 (COP27), operationalized Nov 2023 (COP28).
    Pledges vs needs: pledged $700M vs estimated $400B+/yr need.
    fund_adequacy = total_pledges / estimated_annual_need

Score: large protection gap with rising losses and underfunded L&D
mechanism raises score (crisis).

Sources: Swiss Re Institute, Munich Re NatCatSERVICE, UNFCCC,
V20 Group, COP27/28 L&D Fund records
"""

from app.layers.base import LayerBase

_SQL = """
    SELECT dp.date, dp.value
    FROM data_points dp
    JOIN data_series ds ON dp.series_id = ds.id
    WHERE ds.code = ?
    ORDER BY dp.date
"""


class LossDamageFinancing(LayerBase):
    layer_id = "lGF"
    name = "Loss and Damage Financing"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "WLD")

        codes = {
            "economic_loss": f"CLIMATE_ECONOMIC_LOSS_{country}",
            "insured_loss": f"CLIMATE_INSURED_LOSS_{country}",
            "ld_fund_pledges": f"LD_FUND_PLEDGES_{country}",
            "ld_fund_need": f"LD_FUND_NEED_{country}",
            "gdp": f"GDP_{country}",
        }

        data: dict[str, dict] = {}
        for key, code in codes.items():
            rows = await db.fetch_all(_SQL, (code,))
            if rows:
                data[key] = {r["date"]: float(r["value"]) for r in rows}

        if "economic_loss" not in data:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No climate economic loss data",
            }

        def latest_val(key: str) -> float | None:
            if key not in data:
                return None
            vals = list(data[key].values())
            return float(vals[-1]) if vals else None

        econ_loss = latest_val("economic_loss") or 0.0
        insured = latest_val("insured_loss") or 0.0
        pledges = latest_val("ld_fund_pledges")
        ld_need = latest_val("ld_fund_need")
        gdp = latest_val("gdp")

        protection_gap = max(econ_loss - insured, 0)
        gap_ratio = protection_gap / econ_loss if econ_loss > 0 else 0.0

        loss_pct_gdp = econ_loss / gdp * 100 if gdp and gdp > 0 else None

        fund_adequacy = None
        if pledges is not None and ld_need is not None and ld_need > 0:
            fund_adequacy = pledges / ld_need

        # Score: large protection gap + underfunded L&D mechanism = high score
        # Gap ratio 1.0 (all uninsured) = 60 pts; ratio 0 = 0 pts
        gap_score = gap_ratio * 60

        # Loss as % GDP amplification
        if loss_pct_gdp is not None:
            gap_score += min(loss_pct_gdp * 2, 20)

        # L&D fund adequacy penalty
        fund_penalty = 0.0
        if fund_adequacy is not None:
            fund_penalty = max(1 - fund_adequacy, 0) * 20
        else:
            fund_penalty = 10  # unknown = partial penalty

        score = min(gap_score + fund_penalty, 100)

        return {
            "score": round(score, 1),
            "metrics": {
                "country": country,
                "economic_loss_usd_bn": round(econ_loss, 2),
                "insured_loss_usd_bn": round(insured, 2),
                "protection_gap_usd_bn": round(protection_gap, 2),
                "protection_gap_ratio": round(gap_ratio, 3),
                "loss_pct_gdp": round(loss_pct_gdp, 3) if loss_pct_gdp is not None else None,
                "ld_fund_pledges_usd_bn": round(pledges, 2) if pledges is not None else None,
                "ld_fund_need_usd_bn": round(ld_need, 2) if ld_need is not None else None,
                "ld_fund_adequacy": round(fund_adequacy, 4) if fund_adequacy is not None else None,
            },
        }
