"""Digital markets: data barriers, algorithmic collusion, and interoperability.

Methodology
-----------
1. **Digital Market Concentration (Data Barrier to Entry)**:
   Data network effects create self-reinforcing barriers: more users -> more
   data -> better algorithms -> more users. Quantified via:
     - Data HHI: Herfindahl index of data holdings across incumbents
     - Data learning curve: marginal value of additional data (diminishing returns)
       V(d) = V_max * (1 - exp(-lambda * d)), lambda estimated from quality metrics
   Calvano et al. (2020): even without explicit coordination, Q-learning algorithms
   converge to supracompetitive prices (above Nash equilibrium).

2. **Algorithmic Collusion Risk**:
   Maskin-Tirole (1988) folk theorem: with frequent interaction and patient firms,
   collusive outcomes are sustainable. Algorithmic pricing intensifies this:
     - Pricing frequency index: times per day prices are updated
     - Price correlation across rivals (Pearson rho) as collusion proxy
     - Markup over marginal cost relative to static Nash benchmark
   Risk index = 0.4 * price_correlation + 0.3 * (frequency / 1000) + 0.3 * markup_excess

3. **Interoperability Mandate Assessment**:
   Based on Katz-Shapiro (1985) network externalities model. Mandated interop:
     - Reduces switching costs (measured as share of consumer surplus at stake)
     - May reduce investment incentives (dynamic inefficiency)
     - Competition-access trade-off: market share Gini reduction vs R&D impact
   Farrell-Weiser (2003) conditions for welfare-improving interoperability.

4. **Data Portability Score**:
   EU GDPR Art. 20 / DSA / DMA portability requirements. Score reflects:
     - Portability coverage: share of data types portable
     - Ease of transfer: API availability, machine-readable format
     - Consumer take-up: porting events / total users

References:
    Calvano, E., Calzolari, G., Denicolo, V. & Pastorello, S. (2020). Artificial
        Intelligence, Algorithmic Pricing, and Collusion. AER 110(10): 3267-3297.
    Katz, M.L. & Shapiro, C. (1985). Network Externalities, Competition, and
        Compatibility. AER 75(3): 424-440.
    Farrell, J. & Weiser, P.J. (2003). Modularity, Vertical Integration, and
        Open Access Policies. Harvard Journal of Law & Technology 17(1): 85-134.
    Maskin, E. & Tirole, J. (1988). A Theory of Dynamic Oligopoly, II: Price
        Competition, Kinked Demand Curves, and Edgeworth Cycles. Econometrica 56(3).

Score: high data concentration + high collusion risk -> STRESS/CRISIS.
"""

from __future__ import annotations

import json

import numpy as np

from app.layers.base import LayerBase


class DigitalMarkets(LayerBase):
    layer_id = "l14"
    name = "Digital Markets"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        sector = kwargs.get("sector")
        year = kwargs.get("year")

        clauses = ["ds.country_iso3 = ?", "ds.source = ?"]
        params: list = [country, "digital_markets"]
        if sector:
            clauses.append("ds.description LIKE ?")
            params.append(f"%{sector}%")
        if year:
            clauses.append("dp.date = ?")
            params.append(str(year))

        where = " AND ".join(clauses)
        rows = await db.fetch_all(
            f"""
            SELECT dp.value, dp.date, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE {where}
            ORDER BY dp.date DESC
            """,
            tuple(params),
        )

        if not rows or len(rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient digital markets data"}

        firms = []
        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            firms.append({
                "market_share": float(meta["market_share"]) if meta.get("market_share") is not None else None,
                "data_volume": float(meta["data_volume"]) if meta.get("data_volume") is not None else None,
                "price": float(meta["price"]) if meta.get("price") is not None else None,
                "marginal_cost": float(meta["marginal_cost"]) if meta.get("marginal_cost") is not None else None,
                "price_update_freq": float(meta["price_update_freq"]) if meta.get("price_update_freq") is not None else None,
                "portability_coverage": float(meta["portability_coverage"]) if meta.get("portability_coverage") is not None else None,
                "switching_cost": float(meta["switching_cost"]) if meta.get("switching_cost") is not None else None,
            })

        data_concentration = self._data_barrier(firms)
        collusion_risk = self._algorithmic_collusion(firms)
        interop = self._interoperability_assessment(firms)
        portability = self._data_portability(firms)

        # Score: concentration + collusion risk -> stress
        conc_score = 0.0
        if data_concentration and data_concentration.get("data_hhi") is not None:
            hhi = data_concentration["data_hhi"]
            # HHI 0-0.15 -> 0-25, 0.15-0.25 -> 25-50, 0.25-0.5 -> 50-75, >0.5 -> 75-100
            if hhi < 0.15:
                conc_score = hhi / 0.15 * 25.0
            elif hhi < 0.25:
                conc_score = 25.0 + (hhi - 0.15) / 0.10 * 25.0
            elif hhi < 0.50:
                conc_score = 50.0 + (hhi - 0.25) / 0.25 * 25.0
            else:
                conc_score = 75.0 + min((hhi - 0.50) / 0.50 * 25.0, 25.0)

        collusion_score = 0.0
        if collusion_risk and collusion_risk.get("risk_index") is not None:
            collusion_score = float(collusion_risk["risk_index"]) * 50.0

        score = max(0.0, min(100.0, 0.5 * conc_score + 0.5 * collusion_score))

        return {
            "score": round(score, 2),
            "country": country,
            "n_firms": len(firms),
            "data_concentration": data_concentration,
            "algorithmic_collusion": collusion_risk,
            "interoperability": interop,
            "data_portability": portability,
        }

    @staticmethod
    def _data_barrier(firms: list[dict]) -> dict | None:
        """Data HHI and learning curve as barrier to entry."""
        data_vols = [f["data_volume"] for f in firms if f["data_volume"] is not None]
        shares = [f["market_share"] for f in firms if f["market_share"] is not None]
        if not data_vols and not shares:
            return None

        result: dict = {}

        if data_vols:
            vol_arr = np.array(data_vols, dtype=float)
            vol_share = vol_arr / vol_arr.sum() if vol_arr.sum() > 0 else vol_arr
            data_hhi = float(np.sum(vol_share ** 2))
            result["data_hhi"] = round(data_hhi, 4)
            result["top_data_holder_share"] = round(float(vol_share[np.argmax(vol_arr)]), 4)
            # Learning curve: marginal data value diminishing (estimated as 1/n)
            n = len(vol_arr)
            result["learning_curve_lambda"] = round(float(1.0 / (n + 1)), 4)
            result["diminishing_returns"] = n > 5

        if shares:
            s_arr = np.array(shares, dtype=float)
            s_arr = s_arr / s_arr.sum() if s_arr.sum() > 0 else s_arr
            result["market_hhi"] = round(float(np.sum(s_arr ** 2)), 4)

        return result

    @staticmethod
    def _algorithmic_collusion(firms: list[dict]) -> dict | None:
        """Calvano-style algorithmic collusion risk index."""
        prices = [f["price"] for f in firms if f["price"] is not None]
        mcs = [f["marginal_cost"] for f in firms if f["marginal_cost"] is not None]
        freqs = [f["price_update_freq"] for f in firms if f["price_update_freq"] is not None]

        if not prices:
            return None

        prices_arr = np.array(prices, dtype=float)

        # Price correlation across firms (deviation from mean as proxy for coordination)
        price_cv = float(np.std(prices_arr, ddof=1) / np.mean(prices_arr)) if np.mean(prices_arr) > 0 else 1.0
        # Low CV -> prices move together -> coordination signal (inverted for risk)
        price_correlation_proxy = max(0.0, 1.0 - price_cv)

        # Markup excess
        markup_excess = 0.0
        if mcs and len(mcs) == len(prices):
            mc_arr = np.array(mcs, dtype=float)
            markups = (prices_arr - mc_arr) / np.maximum(prices_arr, 1e-10)
            # Nash benchmark: markup = 1/|epsilon|; use 0.15 as competitive baseline
            competitive_markup = 0.15
            mean_markup = float(np.mean(markups))
            markup_excess = max(0.0, mean_markup - competitive_markup)
        else:
            mean_markup = None

        # Frequency penalty
        freq_factor = 0.0
        if freqs:
            avg_freq = float(np.mean(freqs))
            freq_factor = min(avg_freq / 1000.0, 1.0)

        risk_index = (
            0.4 * price_correlation_proxy
            + 0.3 * freq_factor
            + 0.3 * min(markup_excess / 0.3, 1.0)
        )

        return {
            "price_correlation_proxy": round(price_correlation_proxy, 4),
            "mean_markup": round(mean_markup, 4) if mean_markup is not None else None,
            "markup_excess": round(markup_excess, 4),
            "avg_price_update_freq": round(float(np.mean(freqs)), 1) if freqs else None,
            "risk_index": round(float(risk_index), 4),
            "risk_level": (
                "high" if risk_index > 0.6
                else "moderate" if risk_index > 0.3
                else "low"
            ),
        }

    @staticmethod
    def _interoperability_assessment(firms: list[dict]) -> dict | None:
        """Katz-Shapiro interoperability welfare assessment."""
        shares = [f["market_share"] for f in firms if f["market_share"] is not None]
        switching = [f["switching_cost"] for f in firms if f["switching_cost"] is not None]
        if not shares:
            return None

        s_arr = np.array(sorted(shares, reverse=True), dtype=float)
        s_arr = s_arr / s_arr.sum() if s_arr.sum() > 0 else s_arr

        n = len(s_arr)
        index = np.arange(1, n + 1, dtype=float)
        gini = float(
            (2.0 * np.sum(index * np.sort(s_arr)) - (n + 1) * np.sum(s_arr))
            / (n * np.sum(s_arr))
        ) if n > 1 and np.sum(s_arr) > 0 else 0.0

        avg_switching = float(np.mean(switching)) if switching else None

        # Interop benefit: proportional to Gini * switching_cost
        interop_benefit = None
        if avg_switching is not None:
            interop_benefit = round(gini * avg_switching, 4)

        # Farrell-Weiser: interop welfare-improving if network effect < competition benefit
        interop_recommended = gini > 0.5 and (avg_switching or 0) > 0.3

        return {
            "market_share_gini": round(gini, 4),
            "avg_switching_cost": round(avg_switching, 4) if avg_switching is not None else None,
            "interop_welfare_benefit": interop_benefit,
            "interop_recommended": interop_recommended,
            "n_firms": len(s_arr),
        }

    @staticmethod
    def _data_portability(firms: list[dict]) -> dict | None:
        """Data portability score (GDPR/DMA framework)."""
        coverage = [f["portability_coverage"] for f in firms if f["portability_coverage"] is not None]
        if not coverage:
            return None

        cov_arr = np.array(coverage, dtype=float)
        avg_coverage = float(np.mean(cov_arr))
        min_coverage = float(np.min(cov_arr))

        # Score: 0 = no portability, 1 = full portability
        portability_score = avg_coverage
        compliance_level = (
            "full" if avg_coverage > 0.8
            else "partial" if avg_coverage > 0.4
            else "minimal"
        )

        return {
            "avg_portability_coverage": round(avg_coverage, 4),
            "min_portability_coverage": round(min_coverage, 4),
            "portability_score": round(portability_score, 4),
            "compliance_level": compliance_level,
            "gap_to_full": round(1.0 - avg_coverage, 4),
        }
