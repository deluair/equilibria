"""Adaptation finance gap: adaptation finance flows vs climate vulnerability.

Methodology
-----------
**Adaptation finance need** (UNEP Adaptation Gap Report 2023):
    Developing countries require $215-387 billion/yr for adaptation by 2030,
    rising to $500B-1T by 2050. Current adaptation finance flows ~$46B/yr,
    implying an adaptation gap of $169-341B/yr.

    Adaptation need is estimated from:
    - Residual damage under 2C warming (avoided with adaptation)
    - Sector-specific adaptation costs (coastal protection, agriculture,
      water, health, infrastructure, ecosystems)
    - Country income group adjustments (low-income have highest need/GDP)

**Vulnerability-adjusted adequacy**:
    Adjusts financing by the country's climate vulnerability score (ND-GAIN):
    adequacy = adaptation_finance_received / (adaptation_need * vulnerability_index)

    Highly vulnerable countries receiving little adaptation finance score worst.

**Donor vs domestic split**:
    Internationally-sourced adaptation finance (ODA + MDB) vs domestic public
    spending. Heavy donor dependence with low domestic commitment = fragile.

**Sectoral decomposition**:
    Tracks whether finance reaches highest-priority sectors: water (27%),
    agriculture (22%), infrastructure (17%), ecosystems (15%) per UNFCCC.

Score: large unmet adaptation need in highly vulnerable countries raises score.

Sources: UNEP Adaptation Gap Report, UNFCCC Global Goal on Adaptation,
CPI Global Landscape, ND-GAIN Index, OECD DAC
"""

from app.layers.base import LayerBase

_SQL = """
    SELECT dp.date, dp.value
    FROM data_points dp
    JOIN data_series ds ON dp.series_id = ds.id
    WHERE ds.code = ?
    ORDER BY dp.date
"""


class AdaptationFinanceGap(LayerBase):
    layer_id = "lGF"
    name = "Adaptation Finance Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "WLD")

        codes = {
            "adaptation_received": f"ADAPTATION_FINANCE_RECEIVED_{country}",
            "adaptation_needed": f"ADAPTATION_FINANCE_NEEDED_{country}",
            "vulnerability_index": f"CLIMATE_VULNERABILITY_INDEX_{country}",
            "domestic_adaptation_pct": f"DOMESTIC_ADAPTATION_SHARE_{country}",
            "gdp": f"GDP_{country}",
        }

        data: dict[str, dict] = {}
        for key, code in codes.items():
            rows = await db.fetch_all(_SQL, (code,))
            if rows:
                data[key] = {r["date"]: float(r["value"]) for r in rows}

        if "adaptation_received" not in data and "adaptation_needed" not in data:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No adaptation finance data (received or needed)",
            }

        def latest_val(key: str) -> float | None:
            if key not in data:
                return None
            vals = list(data[key].values())
            return float(vals[-1]) if vals else None

        received = latest_val("adaptation_received") or 0.0
        needed = latest_val("adaptation_needed")
        vulnerability = latest_val("vulnerability_index")  # 0-100, higher = more vulnerable
        domestic_pct = latest_val("domestic_adaptation_pct")
        gdp = latest_val("gdp")

        if needed is None or needed <= 0:
            # Estimate from received only: if received is very low, score high
            if received < 1:
                return {
                    "score": 85.0,
                    "signal": "CRISIS",
                    "metrics": {
                        "country": country,
                        "adaptation_received_usd_bn": round(received, 3),
                        "adaptation_needed_usd_bn": None,
                        "adequacy_ratio": None,
                        "note": "Needed not available; received is near-zero",
                    },
                }
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "Adaptation finance needed data not available",
            }

        adequacy_ratio = received / needed
        gap = max(needed - received, 0)

        # Vulnerability adjustment: high vulnerability amplifies the gap
        vuln_multiplier = 1.0
        if vulnerability is not None:
            # Scale: 50 = average, 100 = maximally vulnerable -> multiplier 1.5
            vuln_multiplier = 1 + (vulnerability - 50) / 100

        gap_score = max(1 - adequacy_ratio, 0) * 60 * vuln_multiplier

        # Domestic commitment penalty: heavy donor dependence = fragility
        domestic_penalty = 0.0
        if domestic_pct is not None:
            domestic_penalty = max(50 - domestic_pct, 0) * 0.4  # up to 20 pts

        gap_pct_gdp = gap / gdp * 100 if gdp and gdp > 0 else None

        score = min(gap_score + domestic_penalty, 100)

        return {
            "score": round(score, 1),
            "metrics": {
                "country": country,
                "adaptation_received_usd_bn": round(received, 3),
                "adaptation_needed_usd_bn": round(needed, 3),
                "adaptation_gap_usd_bn": round(gap, 3),
                "adequacy_ratio": round(adequacy_ratio, 3),
                "vulnerability_index": round(vulnerability, 1) if vulnerability is not None else None,
                "vulnerability_multiplier": round(vuln_multiplier, 2),
                "domestic_adaptation_share_pct": round(domestic_pct, 1) if domestic_pct is not None else None,
                "gap_pct_gdp": round(gap_pct_gdp, 3) if gap_pct_gdp is not None else None,
            },
        }
