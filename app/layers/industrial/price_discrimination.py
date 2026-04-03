"""Price discrimination analysis: degree identification, bundling, and optimal tariffs.

First-degree (perfect): each consumer pays their maximum willingness to pay.
Extracts full consumer surplus. Identified by high price variance conditional
on identical cost, and near-zero consumer surplus.

Second-degree (versioning/quantity discounts): firms offer a menu of
price-quantity bundles; consumers self-select. Identified by nonlinear
pricing schedules and quantity discounts.

Third-degree (group pricing): different prices for identifiable market
segments (student, senior, geographic). Identified by systematic price
differences across groups controlling for cost.

Bundling analysis (Adams & Yellen 1976):
    Pure bundling: only the bundle is sold.
    Mixed bundling: bundle and individual items available.
    Optimal when valuations are negatively correlated.

Two-part tariff optimization (Oi 1971):
    T(q) = F + p*q
    Optimal F = consumer surplus at price p.
    Welfare depends on heterogeneity of consumer types.

Ramsey pricing (Ramsey 1927, Boiteux 1956):
    Markup inversely proportional to demand elasticity:
    (p_i - MC_i) / p_i = lambda / epsilon_i
    Minimizes deadweight loss subject to budget constraint.

References:
    Tirole, J. (1988). The Theory of Industrial Organization, Ch. 3.
    Adams, W. & Yellen, J. (1976). Commodity Bundling. QJE 90(3).
    Oi, W. (1971). A Disneyland Dilemma. QJE 85(1).
    Ramsey, F. (1927). A Contribution to the Theory of Taxation. EJ 37.

Score: high discrimination intensity -> STRESS (welfare loss), low -> STABLE.
"""

import json

import numpy as np

from app.layers.base import LayerBase


class PriceDiscrimination(LayerBase):
    layer_id = "l14"
    name = "Price Discrimination"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        sector = kwargs.get("sector")
        year = kwargs.get("year")

        clauses = ["ds.country_iso3 = ?", "ds.source = ?"]
        params: list = [country, "price_discrimination"]
        if sector:
            clauses.append("ds.description LIKE ?")
            params.append(f"%{sector}%")
        if year:
            clauses.append("dp.date = ?")
            params.append(str(year))

        where = " AND ".join(clauses)
        rows = await db.fetch_all(
            f"""
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE {where}
            ORDER BY dp.date DESC
            """,
            tuple(params),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient pricing data"}

        transactions = []
        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            price = row["value"]
            if price is None or price <= 0:
                continue
            transactions.append({
                "price": float(price),
                "cost": float(meta["cost"]) if meta.get("cost") is not None else None,
                "quantity": float(meta["quantity"]) if meta.get("quantity") is not None else None,
                "segment": meta.get("segment"),
                "bundle": meta.get("bundle"),
                "wtp": float(meta["wtp"]) if meta.get("wtp") is not None else None,
            })

        if len(transactions) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid transactions"}

        prices = np.array([t["price"] for t in transactions])

        # Degree identification
        degree_scores = self._identify_degrees(transactions)

        # Bundling analysis
        bundling = self._bundling_analysis(transactions)

        # Two-part tariff optimization
        two_part = self._two_part_tariff(transactions)

        # Ramsey pricing
        ramsey = self._ramsey_pricing(transactions)

        # Price dispersion metrics
        cv = float(np.std(prices) / np.mean(prices)) if np.mean(prices) > 0 else 0.0
        price_range_ratio = float((np.max(prices) - np.min(prices)) / np.mean(prices)) if np.mean(prices) > 0 else 0.0

        # Discrimination intensity: composite of degree scores
        max_degree = max(degree_scores.values())
        intensity = max_degree

        # Score: high discrimination -> welfare concern
        # intensity 0-0.3 -> STABLE, 0.3-0.6 -> WATCH, 0.6-0.8 -> STRESS, >0.8 -> CRISIS
        if intensity < 0.3:
            score = intensity / 0.3 * 25.0
        elif intensity < 0.6:
            score = 25.0 + (intensity - 0.3) / 0.3 * 25.0
        elif intensity < 0.8:
            score = 50.0 + (intensity - 0.6) / 0.2 * 25.0
        else:
            score = 75.0 + min((intensity - 0.8) / 0.2 * 25.0, 25.0)
        score = max(0.0, min(100.0, score))

        return {
            "score": round(score, 2),
            "country": country,
            "n_transactions": len(transactions),
            "degree_scores": {k: round(v, 4) for k, v in degree_scores.items()},
            "dominant_type": max(degree_scores, key=degree_scores.get),
            "price_dispersion": {
                "cv": round(cv, 4),
                "range_ratio": round(price_range_ratio, 4),
                "mean_price": round(float(np.mean(prices)), 2),
            },
            "bundling": bundling,
            "two_part_tariff": two_part,
            "ramsey_pricing": ramsey,
        }

    @staticmethod
    def _identify_degrees(transactions: list[dict]) -> dict[str, float]:
        """Score likelihood of each price discrimination degree (0-1)."""
        prices = np.array([t["price"] for t in transactions])

        # First degree: price closely tracks WTP
        first_score = 0.0
        wtps = [(t["price"], t["wtp"]) for t in transactions if t["wtp"] is not None]
        if len(wtps) >= 5:
            p_arr = np.array([w[0] for w in wtps])
            wtp_arr = np.array([w[1] for w in wtps])
            if np.std(wtp_arr) > 0:
                corr = float(np.corrcoef(p_arr, wtp_arr)[0, 1])
                surplus_ratio = float(np.mean((wtp_arr - p_arr) / wtp_arr))
                first_score = max(0.0, corr) * (1.0 - max(0.0, min(1.0, surplus_ratio)))

        # Second degree: quantity discounts (negative price-quantity correlation)
        second_score = 0.0
        pq = [(t["price"], t["quantity"]) for t in transactions if t["quantity"] is not None]
        if len(pq) >= 5:
            p_arr = np.array([x[0] for x in pq])
            q_arr = np.array([x[1] for x in pq])
            if np.std(q_arr) > 0 and np.std(p_arr) > 0:
                # Unit price decreasing in quantity -> second degree
                unit_prices = p_arr / np.maximum(q_arr, 1e-10)
                corr = float(np.corrcoef(unit_prices, q_arr)[0, 1])
                second_score = max(0.0, -corr)  # negative correlation = quantity discount

        # Third degree: price differences across segments
        third_score = 0.0
        segments = {}
        for t in transactions:
            seg = t.get("segment")
            if seg is not None:
                segments.setdefault(seg, []).append(t["price"])
        if len(segments) >= 2:
            seg_means = [np.mean(v) for v in segments.values() if len(v) >= 2]
            if len(seg_means) >= 2:
                between_var = np.var(seg_means)
                total_var = np.var(prices)
                if total_var > 0:
                    third_score = float(between_var / total_var)

        return {
            "first_degree": first_score,
            "second_degree": second_score,
            "third_degree": third_score,
        }

    @staticmethod
    def _bundling_analysis(transactions: list[dict]) -> dict | None:
        """Analyze bundling patterns in transaction data."""
        bundled = [t for t in transactions if t.get("bundle")]
        if len(bundled) < 3:
            return None

        bundle_groups: dict[str, list[float]] = {}
        for t in bundled:
            bundle_groups.setdefault(t["bundle"], []).append(t["price"])

        individual = [t for t in transactions if not t.get("bundle")]
        if not individual:
            return {"type": "pure_bundling", "n_bundles": len(bundle_groups)}

        avg_bundle = np.mean([np.mean(v) for v in bundle_groups.values()])
        avg_individual = np.mean([t["price"] for t in individual])

        discount = 1.0 - avg_bundle / (avg_individual * 2.0) if avg_individual > 0 else 0.0

        return {
            "type": "mixed_bundling",
            "n_bundles": len(bundle_groups),
            "avg_bundle_price": round(float(avg_bundle), 2),
            "avg_individual_price": round(float(avg_individual), 2),
            "bundle_discount_pct": round(float(discount * 100.0), 2),
        }

    @staticmethod
    def _two_part_tariff(transactions: list[dict]) -> dict | None:
        """Optimize two-part tariff T(q) = F + p*q."""
        pq = [(t["price"], t["quantity"]) for t in transactions
              if t["quantity"] is not None and t.get("cost") is not None]
        if len(pq) < 5:
            return None

        prices = np.array([x[0] for x in pq])
        quantities = np.array([x[1] for x in pq])
        mc = np.mean([t["cost"] for t in transactions if t["cost"] is not None])

        # Estimate linear demand: q = a - b*p
        if np.std(prices) < 1e-10:
            return None
        b_est = -float(np.cov(quantities, prices)[0, 1] / np.var(prices))
        a_est = float(np.mean(quantities) + b_est * np.mean(prices))

        if b_est >= 0 or a_est <= 0:
            return None

        b_est = abs(b_est)

        # Optimal per-unit price = MC, fixed fee = consumer surplus at MC
        p_star = mc
        q_at_mc = max(a_est - b_est * p_star, 0.0)
        cs_at_mc = 0.5 * q_at_mc ** 2 / b_est if b_est > 0 else 0.0

        return {
            "optimal_unit_price": round(p_star, 2),
            "optimal_fixed_fee": round(cs_at_mc, 2),
            "demand_intercept": round(a_est, 2),
            "demand_slope": round(-b_est, 4),
            "quantity_at_mc": round(q_at_mc, 2),
        }

    @staticmethod
    def _ramsey_pricing(transactions: list[dict]) -> dict | None:
        """Compute Ramsey pricing: markup inversely proportional to elasticity."""
        segments: dict[str, list[dict]] = {}
        for t in transactions:
            seg = t.get("segment")
            if seg is not None and t.get("cost") is not None:
                segments.setdefault(seg, []).append(t)

        if len(segments) < 2:
            return None

        results = {}
        for seg, seg_txns in segments.items():
            prices = np.array([t["price"] for t in seg_txns])
            quantities = np.array([t["quantity"] for t in seg_txns if t["quantity"] is not None])
            mc = np.mean([t["cost"] for t in seg_txns if t["cost"] is not None])

            if len(prices) < 3 or len(quantities) < 3 or np.std(prices) < 1e-10:
                continue

            # Point elasticity estimate
            p_mean = float(np.mean(prices))
            q_mean = float(np.mean(quantities))
            if q_mean <= 0:
                continue

            # Use arc elasticity
            p_sorted = np.sort(prices)
            q_sorted = quantities[np.argsort(prices)] if len(quantities) == len(prices) else quantities
            if len(p_sorted) >= 2:
                dp = p_sorted[-1] - p_sorted[0]
                dq = q_sorted[-1] - q_sorted[0] if len(q_sorted) == len(p_sorted) else 0.0
                elasticity = (dq / q_mean) / (dp / p_mean) if dp > 0 else -1.0
            else:
                elasticity = -1.0

            actual_markup = (p_mean - mc) / p_mean if p_mean > 0 else 0.0
            ramsey_markup = 1.0 / abs(elasticity) if abs(elasticity) > 0.01 else 1.0

            results[seg] = {
                "elasticity": round(float(elasticity), 4),
                "actual_markup": round(actual_markup, 4),
                "ramsey_markup": round(ramsey_markup, 4),
                "deviation": round(abs(actual_markup - ramsey_markup), 4),
            }

        if not results:
            return None

        avg_deviation = float(np.mean([v["deviation"] for v in results.values()]))
        return {
            "segments": results,
            "avg_deviation_from_ramsey": round(avg_deviation, 4),
        }
