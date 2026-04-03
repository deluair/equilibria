"""Social protection program coverage gap.

Social protection coverage measures the share of the population receiving
at least one social protection benefit. Low coverage leaves vulnerable
households exposed to income shocks, health crises, and poverty traps.
ILO SDG 1.3 target: universal social protection coverage.

Primary indicator: 'per_allsp.cov_pop_tot' (all social protection programs,
coverage % of total population, from ASPIRE/World Bank).
Fallback: 'GC.XPN.TRFT.ZS' (social transfers % of government expenditure,
as an indirect coverage proxy).

High score = low coverage = high welfare gap stress.

Scoring: score = clip(max(0, 60 - coverage_pct) * 1.67, 0, 100).
  At coverage = 60%+ the score is 0 (adequate baseline).
  At coverage = 0% the score is 100 (no coverage).

References:
    ILO (2022). World Social Protection Report 2022-24. Geneva.
    World Bank ASPIRE database (social protection coverage indicators).

Sources: World Bank 'per_allsp.cov_pop_tot'; fallback 'GC.XPN.TRFT.ZS'.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SocialProtectionCoverage(LayerBase):
    layer_id = "l10"
    name = "Social Protection Coverage"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score social protection coverage gap.

        Tries ASPIRE coverage first, then falls back to social transfers
        as a share of government expenditure as a proxy.
        """
        country = kwargs.get("country_iso3", "BGD")

        coverage_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'per_allsp.cov_pop_tot'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        fallback_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.XPN.TRFT.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not coverage_rows and not fallback_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no social protection coverage data",
            }

        indicator_used = None
        coverage_pct = None
        year = None

        if coverage_rows:
            row = coverage_rows[0]
            coverage_pct = float(row["value"])
            year = row["date"][:4]
            indicator_used = "per_allsp.cov_pop_tot"
        else:
            row = fallback_rows[0]
            # Transfers as % of govt expenditure: scale roughly to population coverage
            # Countries spending >10% of budget on transfers typically reach >40% coverage
            raw = float(row["value"])
            coverage_pct = min(raw * 4.0, 100.0)  # rough mapping
            year = row["date"][:4]
            indicator_used = "GC.XPN.TRFT.ZS (proxy)"

        score = float(min(max((60.0 - coverage_pct) * 1.67, 0.0), 100.0))

        return {
            "score": score,
            "results": {
                "country": country,
                "year": year,
                "coverage_pct": coverage_pct,
                "indicator": indicator_used,
                "ilo_target_pct": 60.0,
                "gap_to_target": max(0.0, 60.0 - coverage_pct),
                "coverage_tier": (
                    "high" if coverage_pct >= 60
                    else "moderate" if coverage_pct >= 30
                    else "low"
                ),
            },
        }
