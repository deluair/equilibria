"""Gig economy and platform worker analysis.

Estimates the scale and welfare implications of platform-mediated gig work
using four dimensions:

1. Platform worker share: fraction of employment in app-mediated, contingent,
   or non-employer arrangements (ride-share, delivery, freelance platforms).
   BLS Contingent Worker Supplement and OECD STRI data provide proxies.

2. Earnings volatility: standard deviation of monthly earnings for gig vs
   standard workers. Higher intra-year volatility signals welfare risk from
   income unpredictability (Hannagan & Morduch 2015).

3. Benefits gap: gap in employer-sponsored benefits (health, retirement,
   paid leave) between platform and traditional employees. Monetized as
   share of total compensation.

4. Misclassification prevalence: share of workers classified as independent
   contractors who meet the economic dependence criteria for employee status
   (control, integration, exclusivity). Kessler & Kroft (2021) methods.

References:
    Katz, L. & Krueger, A. (2019). The rise and nature of alternative work
        arrangements in the United States. ILR Review 72(2): 382-416.
    Hannagan, A. & Morduch, J. (2015). Income gains and month-to-month
        income volatility. U.S. Financial Diaries working paper.
    Kessler, J. & Kroft, K. (2021). Misclassification and unemployment
        insurance take-up. Working paper.
    Harris, S. & Krueger, A. (2015). A proposal for modernizing labor laws
        for twenty-first-century work. Hamilton Project discussion paper.

Score: high gig share + high volatility + wide benefits gap -> STRESS.
Low share, low volatility, minimal gap -> STABLE.
"""

import json

import numpy as np

from app.layers.base import LayerBase


class GigEconomy(LayerBase):
    layer_id = "l3"
    name = "Gig Economy / Platform Work"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata, ds.description
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'gig_economy'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows or len(rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient gig economy data"}

        gig_shares = []
        earnings_gig = []
        earnings_standard = []
        benefits_gaps = []
        misclassification_rates = []

        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            indicator = meta.get("indicator", row.get("description", ""))
            val = row["value"]
            if val is None:
                continue

            if "gig_share" in indicator or "platform_share" in indicator:
                gig_shares.append(float(val))
            elif "earnings_volatility_gig" in indicator:
                earnings_gig.append(float(val))
            elif "earnings_volatility_standard" in indicator:
                earnings_standard.append(float(val))
            elif "benefits_gap" in indicator:
                benefits_gaps.append(float(val))
            elif "misclassification" in indicator:
                misclassification_rates.append(float(val))

        if not gig_shares:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no platform worker share data"}

        gig_share = float(np.mean(gig_shares))

        # Earnings volatility ratio (gig / standard)
        volatility_ratio = None
        if earnings_gig and earnings_standard:
            vol_gig = float(np.mean(earnings_gig))
            vol_std = float(np.mean(earnings_standard))
            if vol_std > 0:
                volatility_ratio = vol_gig / vol_std

        benefits_gap = float(np.mean(benefits_gaps)) if benefits_gaps else None
        misclassification = float(np.mean(misclassification_rates)) if misclassification_rates else None

        # Score construction: weighted penalty across four dimensions
        # Each dimension contributes up to 25 points
        score_components = []

        # Component 1: gig share (0-30% of workforce -> 0-25 pts)
        share_score = min(25.0, gig_share * 100.0 / 30.0 * 25.0) if gig_share <= 0.30 else 25.0
        score_components.append(share_score)

        # Component 2: earnings volatility ratio (>2x -> max stress)
        if volatility_ratio is not None:
            vol_score = min(25.0, max(0.0, (volatility_ratio - 1.0) * 25.0))
        else:
            vol_score = 12.5  # assume moderate when unknown
        score_components.append(vol_score)

        # Component 3: benefits gap (0-40% of compensation -> 0-25 pts)
        if benefits_gap is not None:
            ben_score = min(25.0, benefits_gap / 40.0 * 25.0)
        else:
            ben_score = 12.5
        score_components.append(ben_score)

        # Component 4: misclassification (0-50% of gig workers -> 0-25 pts)
        if misclassification is not None:
            mis_score = min(25.0, misclassification / 50.0 * 25.0)
        else:
            mis_score = 12.5
        score_components.append(mis_score)

        score = float(np.sum(score_components))
        score = max(0.0, min(100.0, score))

        result = {
            "score": round(score, 2),
            "country": country,
            "platform_worker_share": round(gig_share, 4),
            "platform_worker_pct": round(gig_share * 100.0, 2),
            "score_components": {
                "share_score": round(share_score, 2),
                "volatility_score": round(vol_score, 2),
                "benefits_score": round(ben_score, 2),
                "misclassification_score": round(mis_score, 2),
            },
        }

        if volatility_ratio is not None:
            result["earnings_volatility"] = {
                "ratio_gig_to_standard": round(volatility_ratio, 3),
                "interpretation": (
                    "gig workers face significantly higher income volatility" if volatility_ratio > 2.0
                    else "moderately higher volatility" if volatility_ratio > 1.5
                    else "comparable to standard employment"
                ),
            }

        if benefits_gap is not None:
            result["benefits_gap_pct"] = round(benefits_gap, 2)

        if misclassification is not None:
            result["misclassification_rate_pct"] = round(misclassification, 2)

        return result
