"""Innovation economics: patents, R&D productivity, and knowledge production.

Patent citation analysis measures knowledge flows and innovation quality.
Forward citations (received) proxy patent value (Hall, Jaffe & Trajtenberg 2005).
Backward citations (made) measure knowledge breadth. Citation-weighted patent
counts are the standard quality-adjusted innovation metric.

R&D productivity measures output per unit of research input:
    Productivity = patents / R&D_expenditure (patents per million $)
    Diminishing returns are common: doubling R&D rarely doubles patents.

Schumpeterian competition (Aghion et al. 2005) models the inverted-U
relationship between competition and innovation:
    - Low competition: no incentive to innovate (escape-competition absent)
    - High competition: no rents to fund innovation (Schumpeterian effect)
    - Moderate competition: both effects balance, innovation peaks

The Griliches (1979) knowledge production function:
    ln(A) = a0 + a1*ln(R&D) + a2*ln(H) + a3*ln(S) + e

where A is knowledge output (patents, TFP), R&D is research spending,
H is human capital (researchers), and S is knowledge stock (cumulative
depreciated R&D). a1 is the R&D elasticity of innovation (typically 0.1-0.5).

References:
    Griliches, Z. (1979). Issues in Assessing the Contribution of R&D to
        Productivity Growth. Bell Journal of Economics 10(1): 92-116.
    Hall, B., Jaffe, A. & Trajtenberg, M. (2005). Market Value and Patent
        Citations. RAND Journal of Economics 36(1): 16-38.
    Aghion, P. et al. (2005). Competition and Innovation: An Inverted-U
        Relationship. QJE 120(2): 701-728.

Score: low R&D productivity or stagnant innovation -> STRESS, healthy -> STABLE.
"""

import json

import numpy as np

from app.layers.base import LayerBase


class InnovationEconomics(LayerBase):
    layer_id = "l14"
    name = "Innovation Economics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        year = kwargs.get("year")

        year_clause = "AND dp.date = ?" if year else ""
        params: list = [country, "innovation"]
        if year:
            params.append(str(year))

        rows = await db.fetch_all(
            f"""
            SELECT dp.value, ds.metadata, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = ?
              {year_clause}
            ORDER BY dp.date DESC
            """,
            tuple(params),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient innovation data"}

        patents = []
        rd_spending = []
        researchers = []
        knowledge_stock = []
        citations_forward = []
        citations_backward = []
        hhi_values = []

        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            pat_count = meta.get("patent_count")
            rd = meta.get("rd_spending")
            res = meta.get("researchers")
            stock = meta.get("knowledge_stock")
            fwd = meta.get("citations_forward")
            bwd = meta.get("citations_backward")
            hhi = meta.get("market_hhi")

            if pat_count is not None:
                patents.append(float(pat_count))
            if rd is not None:
                rd_spending.append(float(rd))
            if res is not None:
                researchers.append(float(res))
            if stock is not None:
                knowledge_stock.append(float(stock))
            if fwd is not None:
                citations_forward.append(float(fwd))
            if bwd is not None:
                citations_backward.append(float(bwd))
            if hhi is not None:
                hhi_values.append(float(hhi))

        if len(patents) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient patent data"}

        patents_arr = np.array(patents)

        # Citation analysis
        citation_analysis = None
        if citations_forward:
            fwd_arr = np.array(citations_forward)
            bwd_arr = np.array(citations_backward) if citations_backward else np.zeros(len(fwd_arr))
            citation_analysis = {
                "mean_forward": round(float(np.mean(fwd_arr)), 2),
                "median_forward": round(float(np.median(fwd_arr)), 2),
                "mean_backward": round(float(np.mean(bwd_arr)), 2),
                "citation_concentration": round(float(np.std(fwd_arr) / (np.mean(fwd_arr) + 1e-10)), 4),
                "top_10pct_share": round(
                    float(np.sum(np.sort(fwd_arr)[-max(1, len(fwd_arr) // 10):]) / (np.sum(fwd_arr) + 1e-10)), 4
                ),
            }

        # R&D productivity
        rd_productivity = None
        if rd_spending and len(rd_spending) == len(patents):
            rd_arr = np.array(rd_spending)
            valid = rd_arr > 0
            if valid.any():
                productivity = patents_arr[valid] / (rd_arr[valid] / 1e6)  # patents per million $
                rd_productivity = {
                    "patents_per_million": round(float(np.mean(productivity)), 4),
                    "trend": self._compute_trend(productivity),
                }

        # Griliches knowledge production function
        griliches = self._griliches_kpf(patents, rd_spending, researchers, knowledge_stock)

        # Schumpeterian inverted-U
        schumpeter = None
        if hhi_values and len(hhi_values) >= 5 and len(hhi_values) == len(patents):
            schumpeter = self._schumpeterian_test(np.array(hhi_values), patents_arr)

        # Score: low and declining innovation -> high stress
        mean_patents = float(np.mean(patents_arr))
        trend = self._compute_trend(patents_arr)
        trend_coef = trend["slope_normalized"] if trend else 0.0

        # Declining innovation is concerning
        if trend_coef < -0.05:
            score = 60.0 + abs(trend_coef) * 200.0
        elif trend_coef < 0.0:
            score = 30.0 + abs(trend_coef) * 600.0
        else:
            score = max(0.0, 25.0 - trend_coef * 100.0)
        score = max(0.0, min(100.0, score))

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": len(patents),
            "patent_stats": {
                "mean": round(mean_patents, 1),
                "trend_slope": round(trend_coef, 4) if trend else None,
                "total": round(float(np.sum(patents_arr)), 0),
            },
            "citation_analysis": citation_analysis,
            "rd_productivity": rd_productivity,
            "griliches_kpf": griliches,
            "schumpeterian_inverted_u": schumpeter,
        }

    @staticmethod
    def _compute_trend(arr: np.ndarray) -> dict | None:
        """Linear trend with normalized slope."""
        n = len(arr)
        if n < 3:
            return None
        t = np.arange(n, dtype=float)
        X = np.column_stack([np.ones(n), t])
        beta = np.linalg.lstsq(X, arr, rcond=None)[0]
        y_hat = X @ beta
        ss_res = float(np.sum((arr - y_hat) ** 2))
        ss_tot = float(np.sum((arr - np.mean(arr)) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
        mean_val = float(np.mean(arr))
        slope_norm = beta[1] / mean_val if abs(mean_val) > 1e-10 else 0.0
        return {
            "slope": round(float(beta[1]), 4),
            "slope_normalized": round(float(slope_norm), 4),
            "r_squared": round(r2, 4),
        }

    @staticmethod
    def _griliches_kpf(
        patents: list[float],
        rd_spending: list[float],
        researchers: list[float],
        knowledge_stock: list[float],
    ) -> dict | None:
        """Estimate Griliches knowledge production function in logs.

        ln(patents) = a0 + a1*ln(R&D) + a2*ln(H) + a3*ln(S)
        """
        n = len(patents)
        if n < 10:
            return None

        # Build regressors from available data
        y = np.log(np.maximum(np.array(patents[:n]), 1.0))
        regressors = [np.ones(n)]
        regressor_names = ["constant"]

        if rd_spending and len(rd_spending) >= n:
            rd_arr = np.maximum(np.array(rd_spending[:n]), 1.0)
            regressors.append(np.log(rd_arr))
            regressor_names.append("ln_rd")

        if researchers and len(researchers) >= n:
            res_arr = np.maximum(np.array(researchers[:n]), 1.0)
            regressors.append(np.log(res_arr))
            regressor_names.append("ln_researchers")

        if knowledge_stock and len(knowledge_stock) >= n:
            stock_arr = np.maximum(np.array(knowledge_stock[:n]), 1.0)
            regressors.append(np.log(stock_arr))
            regressor_names.append("ln_knowledge_stock")

        if len(regressors) < 2:
            return None

        X = np.column_stack(regressors)
        beta = np.linalg.lstsq(X, y, rcond=None)[0]
        y_hat = X @ beta
        ss_res = float(np.sum((y - y_hat) ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        elasticities = dict(zip(regressor_names[1:], [round(float(b), 4) for b in beta[1:]]))

        return {
            "elasticities": elasticities,
            "r_squared": round(r2, 4),
            "n_obs": n,
            "returns_to_scale": round(float(sum(beta[1:])), 4),
        }

    @staticmethod
    def _schumpeterian_test(hhi: np.ndarray, patents: np.ndarray) -> dict | None:
        """Test for inverted-U relationship between competition and innovation.

        Regress: patents = b0 + b1*hhi + b2*hhi^2 + e
        Inverted-U requires b1 > 0 and b2 < 0.
        """
        n = len(hhi)
        if n < 10:
            return None

        X = np.column_stack([np.ones(n), hhi, hhi ** 2])
        beta = np.linalg.lstsq(X, patents, rcond=None)[0]
        y_hat = X @ beta
        ss_res = float(np.sum((patents - y_hat) ** 2))
        ss_tot = float(np.sum((patents - np.mean(patents)) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        inverted_u = beta[1] > 0 and beta[2] < 0
        peak_hhi = -beta[1] / (2.0 * beta[2]) if abs(beta[2]) > 1e-12 else None

        return {
            "linear_coef": round(float(beta[1]), 4),
            "quadratic_coef": round(float(beta[2]), 4),
            "r_squared": round(r2, 4),
            "inverted_u_detected": inverted_u,
            "peak_competition_hhi": round(float(peak_hhi), 4) if peak_hhi is not None and 0 < peak_hhi < 1 else None,
        }
