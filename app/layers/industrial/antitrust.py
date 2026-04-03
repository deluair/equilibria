"""Antitrust analysis: market definition, predatory pricing, vertical restraints, cartels.

Market definition via the SSNIP test (hypothetical monopolist test):
    Would a hypothetical monopolist profitably impose a Small but Significant
    Non-transitory Increase in Price (typically 5-10%)? If customers switch
    to substitutes, expand the market definition until SSNIP is profitable.

    Critical loss analysis: the threshold demand reduction that makes a
    price increase unprofitable.
    Critical loss = X / (X + M)
    where X = price increase (%), M = pre-merger margin (%).

Predatory pricing detection (Areeda & Turner 1975):
    Price below AVC is presumptively predatory. Price above ATC is
    presumptively legal. Between AVC and ATC is the grey zone.

    Recoupment test: can the predator raise prices enough post-exit
    to recover losses? Requires entry barriers and market power.

Vertical restraints assessment:
    - Resale price maintenance (RPM): manufacturer sets retailer price
    - Exclusive dealing: restricts retailer from carrying rivals
    - Tying: purchase of product A requires purchase of B
    Evaluated under rule of reason since Leegin (2007).

Cartel overcharge estimation (Connor & Bolotova 2006):
    Median overcharge ~23% for international cartels.
    Detection methods:
    - Structural break in price series at cartel formation/collapse
    - Variance screen: cartel prices less volatile (Abrantes-Metz et al. 2006)
    - Before-and-after: compare cartel period to competitive benchmark

References:
    Areeda, P. & Turner, D. (1975). Predatory Pricing and Related
        Practices Under Section 2 of the Sherman Act. HLR 88(4).
    Connor, J. & Bolotova, Y. (2006). Cartel Overcharges: Survey and
        Meta-Analysis. Int'l Journal of Industrial Organization 24(6).
    Abrantes-Metz, R. et al. (2006). A Variance Screen for Collusion.
        Int'l Journal of Industrial Organization 24(3): 467-486.

Score: evidence of anticompetitive conduct -> STRESS/CRISIS, competitive -> STABLE.
"""

import json

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class AntitrustAnalysis(LayerBase):
    layer_id = "l14"
    name = "Antitrust Analysis"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        sector = kwargs.get("sector")
        year = kwargs.get("year")

        clauses = ["ds.country_iso3 = ?", "ds.source = ?"]
        params: list = [country, "antitrust"]
        if sector:
            clauses.append("ds.description LIKE ?")
            params.append(f"%{sector}%")
        if year:
            clauses.append("dp.date = ?")
            params.append(str(year))

        where = " AND ".join(clauses)
        rows = await db.fetch_all(
            f"""
            SELECT dp.value, ds.metadata, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE {where}
            ORDER BY dp.date ASC
            """,
            tuple(params),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient antitrust data"}

        prices = []
        costs = []
        market_data = []

        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            price = row["value"]
            avc = meta.get("average_variable_cost")
            atc = meta.get("average_total_cost")
            margin = meta.get("margin")
            share = meta.get("market_share")
            is_cartel = meta.get("cartel_period", False)

            if price is not None:
                entry = {
                    "price": float(price),
                    "avc": float(avc) if avc is not None else None,
                    "atc": float(atc) if atc is not None else None,
                    "margin": float(margin) if margin is not None else None,
                    "share": float(share) if share is not None else None,
                    "date": row["date"],
                    "cartel_period": bool(is_cartel),
                }
                prices.append(float(price))
                if avc is not None:
                    costs.append({"avc": float(avc), "atc": float(atc) if atc is not None else None})
                market_data.append(entry)

        if len(market_data) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid data"}

        # SSNIP test / critical loss analysis
        ssnip = self._ssnip_test(market_data, ssnip_pct=kwargs.get("ssnip_pct", 0.05))

        # Predatory pricing detection
        predatory = self._predatory_pricing_test(market_data)

        # Vertical restraints assessment
        vertical = self._vertical_restraints(market_data)

        # Cartel overcharge estimation
        cartel = self._cartel_detection(np.array(prices), market_data)

        # Score: combine signals
        concern_score = 0.0
        if predatory and predatory.get("below_avc_pct", 0) > 10:
            concern_score += 25.0
        if cartel and cartel.get("collusion_detected"):
            concern_score += 40.0
        if ssnip and ssnip.get("narrow_market"):
            concern_score += 15.0
        if vertical and vertical.get("concern_level") == "high":
            concern_score += 20.0

        score = max(0.0, min(100.0, concern_score))

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": len(market_data),
            "ssnip_test": ssnip,
            "predatory_pricing": predatory,
            "vertical_restraints": vertical,
            "cartel_detection": cartel,
        }

    @staticmethod
    def _ssnip_test(data: list[dict], ssnip_pct: float = 0.05) -> dict | None:
        """SSNIP / hypothetical monopolist test with critical loss analysis.

        Critical loss = X / (X + M)
        where X = SSNIP percentage, M = margin.
        If actual loss < critical loss, market is defined narrowly.
        """
        margins = [d["margin"] for d in data if d["margin"] is not None]
        if not margins:
            return None

        avg_margin = float(np.mean(margins))
        if avg_margin <= 0:
            avg_margin = 0.01  # avoid division by zero

        critical_loss = ssnip_pct / (ssnip_pct + avg_margin)

        # Estimate actual demand response (use price-quantity if available)
        # With limited data, use margin-based heuristic
        # Low margin -> large critical loss -> market likely broad
        # High margin -> small critical loss -> market likely narrow
        narrow_market = critical_loss < 0.20  # if losing <20% of sales is enough

        return {
            "ssnip_pct": round(ssnip_pct * 100.0, 1),
            "avg_margin": round(avg_margin, 4),
            "critical_loss_pct": round(critical_loss * 100.0, 2),
            "narrow_market": narrow_market,
            "interpretation": (
                "narrow market definition (SSNIP profitable)"
                if narrow_market
                else "broad market definition (SSNIP unprofitable)"
            ),
        }

    @staticmethod
    def _predatory_pricing_test(data: list[dict]) -> dict | None:
        """Areeda-Turner predatory pricing test.

        Below AVC: presumptively predatory.
        Between AVC and ATC: grey zone.
        Above ATC: presumptively legal.
        """
        entries = [d for d in data if d["avc"] is not None]
        if not entries:
            return None

        n = len(entries)
        below_avc = sum(1 for d in entries if d["price"] < d["avc"])
        in_grey_zone = sum(
            1 for d in entries
            if d["atc"] is not None and d["avc"] <= d["price"] < d["atc"]
        )

        below_avc_pct = below_avc / n * 100.0
        grey_zone_pct = in_grey_zone / n * 100.0

        # Average price-cost margin
        avg_margin = float(np.mean([(d["price"] - d["avc"]) / d["price"]
                                     for d in entries if d["price"] > 0]))

        # Recoupment feasibility: do market shares suggest ability to recoup?
        shares = [d["share"] for d in data if d["share"] is not None]
        recoupment_feasible = max(shares) > 0.4 if shares else None

        classification = "legal"
        if below_avc_pct > 20:
            classification = "presumptively predatory"
        elif below_avc_pct > 5 or grey_zone_pct > 30:
            classification = "grey zone"

        return {
            "below_avc_pct": round(below_avc_pct, 2),
            "grey_zone_pct": round(grey_zone_pct, 2),
            "avg_price_cost_margin": round(avg_margin, 4),
            "recoupment_feasible": recoupment_feasible,
            "classification": classification,
        }

    @staticmethod
    def _vertical_restraints(data: list[dict]) -> dict | None:
        """Assess vertical restraint indicators."""
        shares = [d["share"] for d in data if d["share"] is not None]
        margins = [d["margin"] for d in data if d["margin"] is not None]

        if not shares or not margins:
            return None

        # Market power prerequisite
        max_share = max(shares)
        avg_margin = float(np.mean(margins))

        # High margins + high concentration = vertical restraint concern
        # (RPM, exclusive dealing, tying more concerning with market power)
        if max_share > 0.4 and avg_margin > 0.40:
            concern_level = "high"
        elif max_share > 0.3 or avg_margin > 0.30:
            concern_level = "medium"
        else:
            concern_level = "low"

        # Margin uniformity: very low variance in margins across firms
        # may indicate RPM (manufacturer-imposed pricing)
        margin_cv = float(np.std(margins) / np.mean(margins)) if np.mean(margins) > 0 else 0.0
        rpm_indicator = margin_cv < 0.10 and avg_margin > 0.20

        return {
            "max_market_share": round(max_share, 4),
            "avg_margin": round(avg_margin, 4),
            "concern_level": concern_level,
            "rpm_indicator": rpm_indicator,
            "margin_cv": round(margin_cv, 4),
        }

    @staticmethod
    def _cartel_detection(prices: np.ndarray, data: list[dict]) -> dict | None:
        """Detect cartel behavior using variance screen and structural breaks.

        Variance screen (Abrantes-Metz): cartel prices are less volatile
        than competitive prices. Test for significant variance reduction
        during suspected cartel period.

        Structural break: Chow test for break in price series.
        """
        n = len(prices)
        if n < 10:
            return None

        # Variance screen: split into windows and compare
        # High CV in first half vs low CV in second half (or vice versa)
        # suggests regime change
        mid = n // 2
        first_half = prices[:mid]
        second_half = prices[mid:]

        cv_first = float(np.std(first_half) / np.mean(first_half)) if np.mean(first_half) > 0 else 0.0
        cv_second = float(np.std(second_half) / np.mean(second_half)) if np.mean(second_half) > 0 else 0.0

        # Levene test for equality of variances
        levene_stat, levene_p = stats.levene(first_half, second_half)

        variance_changed = levene_p < 0.05

        # Structural break: Chow test at midpoint
        # Full model: price = a + b*time
        t = np.arange(n, dtype=float)
        X_full = np.column_stack([np.ones(n), t])
        beta_full = np.linalg.lstsq(X_full, prices, rcond=None)[0]
        rss_full = float(np.sum((prices - X_full @ beta_full) ** 2))

        # Sub-models
        X1 = np.column_stack([np.ones(mid), t[:mid]])
        X2 = np.column_stack([np.ones(n - mid), t[mid:]])
        beta1 = np.linalg.lstsq(X1, first_half, rcond=None)[0]
        beta2 = np.linalg.lstsq(X2, second_half, rcond=None)[0]
        rss1 = float(np.sum((first_half - X1 @ beta1) ** 2))
        rss2 = float(np.sum((second_half - X2 @ beta2) ** 2))
        rss_sub = rss1 + rss2

        k = 2  # number of regressors per sub-model
        chow_num = (rss_full - rss_sub) / k
        chow_den = rss_sub / (n - 2 * k)
        chow_f = chow_num / chow_den if chow_den > 0 else 0.0
        chow_p = 1.0 - float(stats.f.cdf(chow_f, k, n - 2 * k))

        structural_break = chow_p < 0.05

        # Overcharge estimation: if cartel period identified,
        # compare to competitive benchmark (first period or last period)
        overcharge_pct = None
        cartel_entries = [d for d in data if d.get("cartel_period")]
        non_cartel_entries = [d for d in data if not d.get("cartel_period")]
        if cartel_entries and non_cartel_entries:
            cartel_mean = float(np.mean([d["price"] for d in cartel_entries]))
            competitive_mean = float(np.mean([d["price"] for d in non_cartel_entries]))
            if competitive_mean > 0:
                overcharge_pct = (cartel_mean - competitive_mean) / competitive_mean * 100.0

        # Collusion signal: both variance change and structural break
        collusion_detected = variance_changed and structural_break

        return {
            "variance_screen": {
                "cv_period_1": round(cv_first, 4),
                "cv_period_2": round(cv_second, 4),
                "levene_stat": round(float(levene_stat), 4),
                "levene_p": round(float(levene_p), 4),
                "variance_changed": variance_changed,
            },
            "structural_break": {
                "chow_f": round(chow_f, 4),
                "chow_p": round(chow_p, 4),
                "break_detected": structural_break,
            },
            "overcharge_pct": round(overcharge_pct, 2) if overcharge_pct is not None else None,
            "collusion_detected": collusion_detected,
        }
