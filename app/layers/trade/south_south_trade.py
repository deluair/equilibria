"""South-South Trade analysis module.

Methodology
-----------
South-South trade refers to commerce among developing countries (the Global
South). Analysis dimensions:

1. S-S trade share: share of reporter's total trade going to other
   developing countries, tracked over time. Benchmark: UNCTAD reports
   global S-S trade exceeded 50% of developing country exports by 2010s.

2. South-South RCA comparison: revealed comparative advantage profiles
   for the reporter vs other Southern partners to measure complementarity
   vs competition within the South.

3. Technology transfer through trade: skill/technology content of S-S
   imports using Lall (2000) technology classification (primary, RB, LT,
   MT, HT). Higher HT share in S-S imports signals technology diffusion.

4. North dependency: share of exports to OECD/Northern markets --
   high dependency raises vulnerability to demand shocks from the North.

Country classification: World Bank income groups; OECD = North.
Score (0-100): higher score indicates greater vulnerability -- over-
dependence on Northern markets, low S-S complementarity, weak technology
transfer through Southern trade.

References:
    UNCTAD (2013). "Economic Development in Africa Report: Intra-African
        Trade: Unlocking Private Sector Dynamism." UNCTAD, Geneva.
    Lall, S. (2000). "The Technological Structure and Performance of
        Developing Country Manufactured Exports, 1985-98." Oxford
        Development Studies, 28(3), 337-369.
    Marquez-Ramos, L. et al. (2011). "Does South-South Trade Reduce North
        Dependency?" World Economy.
"""

from __future__ import annotations

import json

import numpy as np
from scipy import stats

from app.layers.base import LayerBase

# OECD members (proxy for the "North")
OECD_ISO3 = {
    "AUS", "AUT", "BEL", "CAN", "CHL", "CZE", "DNK", "EST", "FIN", "FRA",
    "DEU", "GRC", "HUN", "ISL", "IRL", "ISR", "ITA", "JPN", "KOR", "LVA",
    "LTU", "LUX", "MEX", "NLD", "NZL", "NOR", "POL", "PRT", "SVK", "SVN",
    "ESP", "SWE", "CHE", "TUR", "GBR", "USA",
}

# Lall (2000) technology levels (broad categories stored in metadata)
TECH_LEVELS = ("primary", "resource_based", "low_tech", "mid_tech", "high_tech")


class SouthSouthTrade(LayerBase):
    layer_id = "l1"
    name = "South-South Trade"

    async def compute(self, db, **kwargs) -> dict:
        """Compute South-South trade indicators.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            reporter : str - ISO3 country code (must be a developing country)
            year     : int - reference year
        """
        reporter = kwargs.get("reporter", "BGD")
        year = kwargs.get("year")

        year_clause = "AND dp.date = ?" if year else ""
        params_q: list = [reporter]
        if year:
            params_q.append(str(year))

        rows = await db.execute_fetchall(
            f"""
            SELECT dp.date, dp.value, ds.code, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('baci', 'comtrade', 'wdi_trade')
              AND ds.country_iso3 = ?
              {year_clause}
            ORDER BY dp.date DESC
            """,
            tuple(params_q),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": f"Insufficient bilateral trade data for {reporter}"}

        # Parse bilateral flows
        exports_to_south = 0.0
        exports_to_north = 0.0
        imports_from_south = 0.0
        imports_from_north = 0.0

        tech_imports_south: dict[str, float] = {t: 0.0 for t in TECH_LEVELS}
        tech_imports_north: dict[str, float] = {t: 0.0 for t in TECH_LEVELS}

        rca_reporter: dict[str, float] = {}
        rca_south_avg: dict[str, float] = {}
        time_series: dict[str, dict] = {}  # date -> {ss_export_share, north_dep}

        for row in rows:
            val = float(row[1]) if row[1] else 0.0
            if val <= 0:
                continue
            meta = json.loads(row[3]) if row[3] else {}
            partner = meta.get("partner_iso3", "")
            flow = meta.get("flow", "export")  # 'export' or 'import'
            tech = meta.get("tech_level", "")
            date = row[0]

            is_north = partner.upper() in OECD_ISO3

            if flow == "export":
                if is_north:
                    exports_to_north += val
                else:
                    exports_to_south += val
            else:
                if is_north:
                    imports_from_north += val
                    if tech in tech_imports_north:
                        tech_imports_north[tech] += val
                else:
                    imports_from_south += val
                    if tech in tech_imports_south:
                        tech_imports_south[tech] += val

            # RCA from metadata
            hs2 = meta.get("hs2", "")
            if hs2 and meta.get("rca_reporter") is not None:
                rca_reporter[hs2] = float(meta["rca_reporter"])
            if hs2 and meta.get("rca_south_avg") is not None:
                rca_south_avg[hs2] = float(meta.get("rca_south_avg", 0))

            # Time series aggregation
            if date not in time_series:
                time_series[date] = {"exp_south": 0.0, "exp_north": 0.0}
            if flow == "export":
                if is_north:
                    time_series[date]["exp_north"] += val
                else:
                    time_series[date]["exp_south"] += val

        total_exports = exports_to_south + exports_to_north

        ss_export_share = exports_to_south / total_exports if total_exports > 0 else 0.0
        north_dependency = exports_to_north / total_exports if total_exports > 0 else 1.0

        # Technology transfer index: HT + MT share of S-S imports
        total_south_imp = sum(tech_imports_south.values())
        ht_mt_share_south = (
            (tech_imports_south.get("high_tech", 0) + tech_imports_south.get("mid_tech", 0))
            / total_south_imp if total_south_imp > 0 else 0.0
        )

        # RCA complementarity: correlation between reporter and southern partner RCA profiles
        common_hs = sorted(set(rca_reporter) & set(rca_south_avg))
        if len(common_hs) >= 5:
            r_vals = np.array([rca_reporter[h] for h in common_hs])
            s_vals = np.array([rca_south_avg[h] for h in common_hs])
            rca_corr, rca_p = stats.pearsonr(r_vals, s_vals)
            # Negative correlation = complementary (reporter exports where South imports)
            complementarity_score = float(np.clip(50 - rca_corr * 50, 0, 100))
        else:
            rca_corr = None
            rca_p = None
            complementarity_score = 50.0

        # Time-series trend in SS share
        if len(time_series) >= 3:
            dates_sorted = sorted(time_series)
            ss_shares_ts = []
            for d in dates_sorted:
                tot = time_series[d]["exp_south"] + time_series[d]["exp_north"]
                ss_shares_ts.append(time_series[d]["exp_south"] / tot if tot > 0 else 0.0)
            x_ts = np.arange(len(ss_shares_ts), dtype=float)
            slope_ts, _, r_ts, p_ts, _ = stats.linregress(x_ts, ss_shares_ts)
            trend = {
                "slope": round(float(slope_ts), 6),
                "r_squared": round(r_ts ** 2, 4),
                "p_value": round(float(p_ts), 4),
                "direction": "increasing" if slope_ts > 0 else "decreasing",
            }
        else:
            trend = {"note": "insufficient time series"}

        # Score: higher = more vulnerable
        # High north dependency is bad
        north_dep_penalty = float(north_dependency) * 40
        # Low S-S share is bad
        ss_share_penalty = max(0.0, 0.5 - ss_export_share) * 40
        # Low HT/MT import share from South = weak technology transfer
        tech_penalty = max(0.0, 0.3 - ht_mt_share_south) * 20

        score = float(np.clip(north_dep_penalty + ss_share_penalty + tech_penalty, 0, 100))

        return {
            "score": round(score, 2),
            "reporter": reporter,
            "ss_export_share": round(ss_export_share, 4),
            "north_dependency": round(north_dependency, 4),
            "exports_to_south": round(exports_to_south, 2),
            "exports_to_north": round(exports_to_north, 2),
            "imports_from_south": round(imports_from_south, 2),
            "imports_from_north": round(imports_from_north, 2),
            "technology_transfer": {
                "ht_mt_share_ss_imports": round(ht_mt_share_south, 4),
                "tech_breakdown_south": {k: round(v, 2) for k, v in tech_imports_south.items()},
            },
            "rca_complementarity": {
                "correlation": round(float(rca_corr), 4) if rca_corr is not None else None,
                "p_value": round(float(rca_p), 4) if rca_p is not None else None,
                "complementarity_score": round(complementarity_score, 2),
                "n_products": len(common_hs),
            },
            "trend": trend,
        }
