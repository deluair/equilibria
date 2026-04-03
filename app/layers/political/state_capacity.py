"""State Capacity module.

Four dimensions following Besley & Persson (2011):

1. **Tax capacity** (Besley-Persson 2011):
   The ability to raise revenue: legal enforcement, administrative reach,
   formal-sector share. Tax capacity index = tax/GDP corrected for
   economic structure. Low-income countries with high informal sectors
   have low tax capacity regardless of effort.
   Besley-Persson model: state invests in fiscal capacity when threatened
   by conflict or when coalition value of public goods is high.

2. **Legal capacity index**:
   Rule of law, contract enforcement, property rights protection.
   World Bank Doing Business: time to enforce a contract, cost as %
   of claim. WGI rule-of-law index. ICRG legal and order component.

3. **Bureaucratic quality** (Evans & Rauch 1999):
   Meritocratic recruitment, career stability, wages, professionalism.
   Evans-Rauch Weberianness Scale. WGI government effectiveness.
   High-quality bureaucracies deliver growth-enhancing public goods.

4. **State fragility scoring** (FSI/OECD):
   Fragile states index: security, political, economic, social/cohesion
   dimensions. OECD fragility framework: capacity, legitimacy, security.
   High fragility -> weak state capacity.

Score: low tax/GDP + weak rule of law + poor bureaucratic quality
+ high fragility -> high stress.

References:
    Besley, T. & Persson, T. (2011). Pillars of Prosperity. Princeton UP.
    Evans, P. & Rauch, J. (1999). "Bureaucracy and Growth." ASR 64(5).
    World Bank. (2023). Worldwide Governance Indicators.
    Fund for Peace. (2023). Fragile States Index.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class StateCapacity(LayerBase):
    layer_id = "l12"
    name = "State Capacity"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate Besley-Persson state capacity components.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 code (default BGD)
        """
        country = kwargs.get("country_iso3", "BGD")

        # Tax capacity: tax/GDP ratio
        tax_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('wdi', 'imf', 'fred')
              AND (ds.name LIKE '%tax%revenue%gdp%' OR ds.name LIKE '%tax%to%gdp%'
                   OR ds.name LIKE '%total%tax%revenue%' OR ds.name LIKE '%revenue%excluding%grants%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Legal capacity: rule of law, contract enforcement
        legal_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%rule%of%law%' OR ds.name LIKE '%contract%enforcement%'
                   OR ds.name LIKE '%property%rights%' OR ds.name LIKE '%legal%system%'
                   OR ds.name LIKE '%icrg%law%order%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Bureaucratic quality: government effectiveness, WGI
        bureau_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%government%effectiveness%' OR ds.name LIKE '%bureaucratic%quality%'
                   OR ds.name LIKE '%public%sector%quality%' OR ds.name LIKE '%state%effectiveness%'
                   OR ds.name LIKE '%wgi%effectiveness%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # State fragility
        fragility_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%fragil%state%' OR ds.name LIKE '%state%fragil%'
                   OR ds.name LIKE '%fragil%index%' OR ds.name LIKE '%failed%state%'
                   OR ds.name LIKE '%political%stability%')
            ORDER BY dp.date
            """,
            (country,),
        )

        all_empty = not any([tax_rows, legal_rows, bureau_rows, fragility_rows])
        if all_empty:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no state capacity data"}

        # --- 1. Tax capacity (Besley-Persson) ---
        tax_capacity = None
        tax_stress = 0.5
        if tax_rows:
            tv = np.array([float(r["value"]) for r in tax_rows])
            tax_dates = [r["date"] for r in tax_rows]
            latest_tax = float(tv[-1])

            # Tax/GDP benchmarks (IMF): low-income < 15%, middle-income 15-25%, high > 25%
            # Values stored as percent (e.g., 12.5 = 12.5% of GDP)
            if latest_tax > 1:
                tax_pct = latest_tax  # Already in percent
            else:
                tax_pct = latest_tax * 100.0  # Fractional -> percent

            # Stress: below 15% = low capacity, above 25% = high capacity
            if tax_pct < 10:
                tax_stress = 0.85
            elif tax_pct < 15:
                tax_stress = 0.60
            elif tax_pct < 20:
                tax_stress = 0.40
            elif tax_pct < 25:
                tax_stress = 0.20
            else:
                tax_stress = 0.10

            # Trend
            trend = None
            if len(tv) >= 3:
                t = np.arange(len(tv), dtype=float)
                slope, _, r_val, p_val, _ = stats.linregress(t, tv)
                trend = {
                    "slope_pp_per_year": round(float(slope), 4),
                    "direction": "improving" if slope > 0 else "declining",
                    "r_squared": round(float(r_val ** 2), 4),
                }

            tax_capacity = {
                "latest_tax_gdp_pct": round(tax_pct, 2),
                "mean_tax_gdp_pct": round(float(np.mean(tv)) if latest_tax > 1 else float(np.mean(tv)) * 100, 2),
                "capacity_tier": (
                    "low" if tax_pct < 15 else "middle" if tax_pct < 25 else "high"
                ),
                "stress": round(tax_stress, 4),
                "n_obs": len(tv),
                "date_range": [str(tax_dates[0]), str(tax_dates[-1])],
                "reference": "Besley & Persson 2011; IMF benchmarks: <15% low, 15-25% middle, >25% high",
            }
            if trend:
                tax_capacity["trend"] = trend

        # --- 2. Legal capacity ---
        legal_capacity = None
        legal_stress = 0.5
        if legal_rows:
            lv = np.array([float(r["value"]) for r in legal_rows])
            legal_dates = [r["date"] for r in legal_rows]
            latest_legal = float(lv[-1])

            # WGI rule of law: -2.5 to 2.5 (higher = better rule of law)
            if np.min(lv) < -1:
                # WGI scale
                normalized = (latest_legal + 2.5) / 5.0
            elif np.max(lv) <= 1.5:
                normalized = float(latest_legal)
            else:
                # 0-100 scale (e.g., Fraser index): higher = better
                normalized = latest_legal / 100.0

            legal_stress = 1.0 - float(np.clip(normalized, 0, 1))

            # Trend
            trend = None
            if len(lv) >= 3:
                t = np.arange(len(lv), dtype=float)
                slope, _, r_val, p_val, _ = stats.linregress(t, lv)
                trend = {
                    "slope": round(float(slope), 5),
                    "direction": "improving" if slope > 0 else "declining",
                    "r_squared": round(float(r_val ** 2), 4),
                    "p_value": round(float(p_val), 4),
                }

            legal_capacity = {
                "latest_index": round(latest_legal, 3),
                "normalized_0_1": round(float(np.clip(normalized, 0, 1)), 4),
                "stress": round(legal_stress, 4),
                "n_obs": len(lv),
                "date_range": [str(legal_dates[0]), str(legal_dates[-1])],
                "reference": "WGI Rule of Law; Evans & Rauch 1999",
            }
            if trend:
                legal_capacity["trend"] = trend

        # --- 3. Bureaucratic quality ---
        bureau_quality = None
        bureau_stress = 0.5
        if bureau_rows:
            bv = np.array([float(r["value"]) for r in bureau_rows])
            bureau_dates = [r["date"] for r in bureau_rows]
            latest_bureau = float(bv[-1])

            # WGI government effectiveness: -2.5 to 2.5
            if np.min(bv) < -1:
                normalized = (latest_bureau + 2.5) / 5.0
            elif np.max(bv) <= 1.5:
                normalized = float(latest_bureau)
            else:
                normalized = latest_bureau / 100.0

            bureau_stress = 1.0 - float(np.clip(normalized, 0, 1))

            bureau_quality = {
                "latest_index": round(latest_bureau, 3),
                "normalized_0_1": round(float(np.clip(normalized, 0, 1)), 4),
                "stress": round(bureau_stress, 4),
                "tier": (
                    "weak" if bureau_stress > 0.65 else "moderate" if bureau_stress > 0.35 else "strong"
                ),
                "n_obs": len(bv),
                "date_range": [str(bureau_dates[0]), str(bureau_dates[-1])],
                "reference": "Evans & Rauch 1999 Weberianness; WGI Government Effectiveness",
            }

        # --- 4. State fragility ---
        fragility_analysis = None
        fragility_stress = 0.5
        if fragility_rows:
            fv = np.array([float(r["value"]) for r in fragility_rows])
            frag_dates = [r["date"] for r in fragility_rows]
            latest_frag = float(fv[-1])

            # FSI: 0-120 (higher = more fragile). Political stability WGI: -2.5 to 2.5
            if latest_frag > 5:
                # FSI-style: normalize 0-120
                fragility_stress = float(np.clip(latest_frag / 120.0, 0, 1))
            else:
                # WGI political stability: -2.5 to 2.5 (higher = more stable)
                fragility_stress = 1.0 - (latest_frag + 2.5) / 5.0

            fragility_stress = float(np.clip(fragility_stress, 0, 1))

            fragility_analysis = {
                "latest_index": round(latest_frag, 3),
                "fragility_stress": round(fragility_stress, 4),
                "fragility_level": (
                    "alert" if fragility_stress > 0.7
                    else "warning" if fragility_stress > 0.45
                    else "stable"
                ),
                "n_obs": len(fv),
                "date_range": [str(frag_dates[0]), str(frag_dates[-1])],
                "reference": "Fund for Peace FSI; OECD fragility framework",
            }

        # --- Score ---
        # Weight components: tax 30, legal 25, bureaucracy 25, fragility 20
        score = float(np.clip(
            tax_stress * 30.0
            + legal_stress * 25.0
            + bureau_stress * 25.0
            + fragility_stress * 20.0,
            0, 100,
        ))

        result = {
            "score": round(score, 2),
            "country": country,
            "score_components": {
                "tax_capacity_stress": round(tax_stress * 30.0, 2),
                "legal_capacity_stress": round(legal_stress * 25.0, 2),
                "bureaucratic_quality_stress": round(bureau_stress * 25.0, 2),
                "fragility_stress": round(fragility_stress * 20.0, 2),
            },
            "besley_persson_summary": {
                "fiscal_capacity": round(1.0 - tax_stress, 4),
                "legal_capacity": round(1.0 - legal_stress, 4),
                "overall_state_capacity": round(1.0 - score / 100.0, 4),
            },
        }

        if tax_capacity:
            result["tax_capacity"] = tax_capacity
        if legal_capacity:
            result["legal_capacity"] = legal_capacity
        if bureau_quality:
            result["bureaucratic_quality"] = bureau_quality
        if fragility_analysis:
            result["state_fragility"] = fragility_analysis

        return result
