"""Generative AI market impact: digital services concentration and platform dominance.

Generative AI accelerates winner-take-all dynamics in digital markets: large
foundation models require massive compute and data, creating insurmountable
barriers to entry that concentrate power among a handful of platform firms.
Economies hosting dominant AI platform companies capture rents; others face
platform dependency. Herfindahl-Hirschman concentration in digital services
and ICT sector value added proxy for domestic versus foreign platform dominance.

Parker, Van Alstyne, Choudary (2016): platform economies exhibit extreme
winner-take-all concentration. GenAI intensifies this via model scale effects
(Kaplan et al. 2020 scaling laws).

Score: high ICT sector concentration + low domestic digital capacity -> CRISIS
(platform dependency), competitive domestic digital economy -> STABLE.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class GenerativeAIMarketImpact(LayerBase):
    layer_id = "lAI"
    name = "Generative AI Market Impact"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        ict_va_code = "NV.SRV.TETC.ZS"
        mobile_code = "IT.CEL.SETS.P2"

        ict_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (ict_va_code, "%transport%storage%communication%"),
        )
        mobile_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (mobile_code, "%mobile cellular subscriptions%"),
        )

        ict_vals = [r["value"] for r in ict_rows if r["value"] is not None]
        mobile_vals = [r["value"] for r in mobile_rows if r["value"] is not None]

        if not ict_vals and not mobile_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for ICT value added NV.SRV.TETC.ZS or mobile subscriptions IT.CEL.SETS.P2",
            }

        ict_va_share = ict_vals[0] if ict_vals else None
        mobile_per_100 = mobile_vals[0] if mobile_vals else None

        # Base score from ICT services value added share
        # Low domestic ICT sector = higher platform dependency risk
        if ict_va_share is not None:
            if ict_va_share >= 15:
                base = 15.0
            elif ict_va_share >= 10:
                base = 15.0 + (15.0 - ict_va_share) * 3.0
            elif ict_va_share >= 5:
                base = 30.0 + (10.0 - ict_va_share) * 4.0
            else:
                base = min(80.0, 50.0 + (5.0 - ict_va_share) * 3.0)
        else:
            base = 50.0

        # Mobile penetration proxies digital platform market reach
        # Very high mobile = large addressable market, but also exposes more
        # consumers to dominant foreign platforms
        if mobile_per_100 is not None:
            if mobile_per_100 >= 120:
                # Saturated mobile market -- large exposure to platform dominance
                base = min(100.0, base + 5.0)
            elif mobile_per_100 >= 80:
                base = max(5.0, base - 5.0)
            elif mobile_per_100 < 40:
                base = min(100.0, base + 8.0)  # digital exclusion

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "ict_services_value_added_pct_gdp": round(ict_va_share, 2) if ict_va_share is not None else None,
                "mobile_subscriptions_per_100": round(mobile_per_100, 2) if mobile_per_100 is not None else None,
                "n_obs_ict": len(ict_vals),
                "n_obs_mobile": len(mobile_vals),
                "platform_dependency_risk": score > 50,
                "domestic_digital_capacity": (
                    "strong" if ict_va_share is not None and ict_va_share >= 12
                    else "moderate" if ict_va_share is not None and ict_va_share >= 6
                    else "weak"
                ),
            },
        }
