"""Product space proximity and complexity measures.

Methodology:
    The product space (Hidalgo et al., 2007) maps the relatedness between
    products based on co-export patterns. Key measures:

    1. Proximity: phi_{ij} = min(P(RCA_i|RCA_j), P(RCA_j|RCA_i))
       where RCA is Balassa's revealed comparative advantage.

    2. Density: omega_{cp} = (sum of proximities to products with RCA>1)
       / (sum of all proximities to product p). Measures how close a
       country is to acquiring comparative advantage in product p.

    3. Economic Complexity Index (ECI): eigenvector of the bipartite
       country-product RCA matrix (Hausmann et al., 2014).

    4. Product Complexity Index (PCI): eigenvector of the product-country
       RCA matrix.

    Score (0-100): Higher score means lower complexity and density
    (less diversified, more vulnerable).

References:
    Hidalgo, C. et al. (2007). "The Product Space Conditions the
        Development of Nations." Science, 317(5837), 482-487.
    Hausmann, R. et al. (2014). The Atlas of Economic Complexity.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ProductSpace(LayerBase):
    layer_id = "l1"
    name = "Product Space & Complexity"

    async def compute(self, db, **kwargs) -> dict:
        """Compute product space density and complexity for a country.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            reporter : str - ISO3 country code
            year : int - reference year
            rca_threshold : float - RCA cutoff (default 1.0)
        """
        reporter = kwargs.get("reporter", "USA")
        year = kwargs.get("year", 2022)
        rca_threshold = kwargs.get("rca_threshold", 1.0)

        # Fetch RCA matrix: countries x products
        rows = await db.execute(
            """
            SELECT reporter_iso3, product_code, rca
            FROM rca_matrix
            WHERE year = ?
            """,
            (year,),
        )
        records = await rows.fetchall()

        if not records:
            return {"score": 50.0, "eci": None, "density": None,
                    "note": "No RCA data available"}

        # Build country-product RCA matrix
        countries = sorted(set(r["reporter_iso3"] for r in records))
        products = sorted(set(r["product_code"] for r in records))
        c_idx = {c: i for i, c in enumerate(countries)}
        p_idx = {p: i for i, p in enumerate(products)}

        n_c = len(countries)
        n_p = len(products)
        rca_mat = np.zeros((n_c, n_p))

        for r in records:
            ci = c_idx[r["reporter_iso3"]]
            pi = p_idx[r["product_code"]]
            rca_mat[ci, pi] = float(r["rca"])

        # Binary RCA matrix (Mcp): 1 if RCA >= threshold
        mcp = (rca_mat >= rca_threshold).astype(float)

        # Diversification (kc) and ubiquity (kp)
        kc = mcp.sum(axis=1)  # how many products each country exports with RCA
        kp = mcp.sum(axis=0)  # how many countries export each product with RCA

        # Proximity matrix: phi_{pp'} = min(P(p|p'), P(p'|p))
        # P(p|p') = (# countries exporting both) / (# countries exporting p')
        # Use kp to normalize
        kp_safe = np.where(kp > 0, kp, 1)
        co_export = mcp.T @ mcp  # (n_p x n_p): number of countries exporting both

        # Conditional probabilities
        with np.errstate(divide="ignore", invalid="ignore"):
            cond = co_export / kp_safe[np.newaxis, :]
        # Proximity: minimum of both directions
        proximity = np.minimum(cond, cond.T)
        np.fill_diagonal(proximity, 0.0)

        # Density for target country
        if reporter not in c_idx:
            return {"score": 50.0, "eci": None, "density": None,
                    "note": f"Country {reporter} not in RCA data"}

        ci_target = c_idx[reporter]
        country_rca = mcp[ci_target]

        # Density: for each product, sum proximity to products the country exports
        prox_sum_all = proximity.sum(axis=1)
        prox_sum_all_safe = np.where(prox_sum_all > 0, prox_sum_all, 1)
        prox_to_exports = proximity @ country_rca
        density = prox_to_exports / prox_sum_all_safe

        avg_density = float(np.mean(density))
        density_of_non_exports = float(
            np.mean(density[country_rca == 0]) if (country_rca == 0).sum() > 0 else 0
        )

        # Economic Complexity Index (method of reflections, 2 iterations)
        kc_safe = np.where(kc > 0, kc, 1)
        kp_safe_vec = np.where(kp > 0, kp, 1)

        # Iterative method: eigenvector of Mcc' = (1/kc) * Mcp * (1/kp) * Mcp'
        mcc = (mcp / kc_safe[:, np.newaxis]) @ (mcp / kp_safe_vec[np.newaxis, :]).T

        try:
            eigenvalues, eigenvectors = np.linalg.eigh(mcc)
            # Second largest eigenvector is ECI
            sort_idx = np.argsort(eigenvalues)[::-1]
            eci_raw = eigenvectors[:, sort_idx[1]].real
            # Standardize
            eci = (eci_raw - np.mean(eci_raw)) / (np.std(eci_raw) + 1e-10)
        except np.linalg.LinAlgError:
            eci = np.zeros(n_c)

        # PCI via product-product matrix
        mpp = (mcp / kp_safe_vec[np.newaxis, :]).T @ (mcp / kc_safe[:, np.newaxis])
        try:
            eigenvalues_p, eigenvectors_p = np.linalg.eigh(mpp)
            sort_idx_p = np.argsort(eigenvalues_p)[::-1]
            pci_raw = eigenvectors_p[:, sort_idx_p[1]].real
            pci = (pci_raw - np.mean(pci_raw)) / (np.std(pci_raw) + 1e-10)
        except np.linalg.LinAlgError:
            pci = np.zeros(n_p)

        country_eci = float(eci[ci_target])
        country_diversification = float(kc[ci_target])
        eci_rank = int(np.sum(eci > eci[ci_target]) + 1)

        # Top opportunities: products not exported with high density
        non_exports = np.where(country_rca == 0)[0]
        if len(non_exports) > 0:
            opp_scores = density[non_exports] * np.maximum(pci[non_exports], 0)
            top_opp_idx = non_exports[np.argsort(opp_scores)[::-1][:10]]
            opportunities = [
                {
                    "product": products[i],
                    "density": float(density[i]),
                    "pci": float(pci[i]),
                    "opportunity_score": float(density[i] * max(pci[i], 0)),
                }
                for i in top_opp_idx
            ]
        else:
            opportunities = []

        # Score: low complexity + low density = high vulnerability
        # Normalize ECI to 0-100 where low ECI = high score (bad)
        eci_min, eci_max = float(eci.min()), float(eci.max())
        if eci_max > eci_min:
            eci_pct = (country_eci - eci_min) / (eci_max - eci_min)
        else:
            eci_pct = 0.5
        score = float(np.clip((1 - eci_pct) * 100, 0, 100))

        return {
            "score": score,
            "eci": country_eci,
            "eci_rank": eci_rank,
            "diversification": country_diversification,
            "avg_density": avg_density,
            "density_non_exports": density_of_non_exports,
            "n_products_with_rca": int(country_rca.sum()),
            "n_total_products": n_p,
            "n_countries": n_c,
            "top_opportunities": opportunities,
            "reporter": reporter,
            "year": year,
        }
