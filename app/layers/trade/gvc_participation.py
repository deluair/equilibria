"""Global value chain participation index.

Methodology:
    Measure a country's integration into global value chains using
    trade-in-value-added concepts. Following Koopman et al. (2014):

    1. GVC Participation Index = Forward + Backward linkages.
       - Forward: domestic value added in partner exports / gross exports.
         Measures how much a country's value added is used as inputs by
         other countries' exports.
       - Backward: foreign value added in own exports / gross exports.
         Measures how much a country relies on imported intermediates
         for its exports.

    2. Upstreamness: average distance from final demand, following
       Antras et al. (2012). U_i = 1 + sum_j(a_ij * U_j) where a_ij
       is the direct requirements coefficient.

    3. GVC position index: log(forward) - log(backward). Positive means
       the country is upstream (supplies intermediates), negative means
       downstream (assembles final goods).

    Score (0-100): Higher score indicates lower GVC participation
    (less integrated, potentially more isolated from global production).

References:
    Koopman, R. et al. (2014). "Tracing Value-Added and Double Counting
        in Gross Exports." American Economic Review, 104(2), 459-494.
    Antras, P. et al. (2012). "Measuring the Upstreamness of Production
        and Trade Flows." American Economic Review P&P, 102(3), 412-416.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class GVCParticipation(LayerBase):
    layer_id = "l1"
    name = "GVC Participation"

    async def compute(self, db, **kwargs) -> dict:
        """Compute GVC participation, position, and upstreamness.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            reporter : str - ISO3 country code
            year : int - reference year
        """
        reporter = kwargs.get("reporter", "USA")
        year = kwargs.get("year", 2022)

        # Fetch value-added trade data
        rows = await db.execute(
            """
            SELECT reporter_iso3, partner_iso3,
                   gross_exports, domestic_va_in_exports,
                   foreign_va_in_exports, domestic_va_in_partner_exports
            FROM value_added_trade
            WHERE year = ?
            """,
            (year,),
        )
        records = await rows.fetchall()

        if not records:
            return {"score": 50.0, "gvc_participation": None,
                    "note": "No value-added trade data available"}

        # Build country-level aggregates
        countries = sorted(set(
            r["reporter_iso3"] for r in records
        ).union(
            r["partner_iso3"] for r in records
        ))
        c_idx = {c: i for i, c in enumerate(countries)}
        n_c = len(countries)

        gross_exports = np.zeros(n_c)
        dva_in_exports = np.zeros(n_c)
        fva_in_exports = np.zeros(n_c)
        dva_in_partner = np.zeros(n_c)

        # Input-output coefficients matrix for upstreamness
        io_matrix = np.zeros((n_c, n_c))

        for r in records:
            ri = c_idx.get(r["reporter_iso3"])
            pi = c_idx.get(r["partner_iso3"])
            if ri is None or pi is None:
                continue

            ge = float(r["gross_exports"] or 0)
            dva = float(r["domestic_va_in_exports"] or 0)
            fva = float(r["foreign_va_in_exports"] or 0)
            dva_p = float(r["domestic_va_in_partner_exports"] or 0)

            gross_exports[ri] += ge
            dva_in_exports[ri] += dva
            fva_in_exports[ri] += fva
            dva_in_partner[ri] += dva_p

            if ge > 0:
                io_matrix[ri, pi] += fva / ge  # intermediate use share

        if reporter not in c_idx:
            return {"score": 50.0, "gvc_participation": None,
                    "note": f"Country {reporter} not in data"}

        ci = c_idx[reporter]
        ge = gross_exports[ci]

        if ge <= 0:
            return {"score": 50.0, "gvc_participation": None,
                    "note": "Zero gross exports"}

        # Forward linkage: DVA in partner exports / gross exports
        forward = dva_in_partner[ci] / ge

        # Backward linkage: FVA in own exports / gross exports
        backward = fva_in_exports[ci] / ge

        # GVC participation index
        gvc_participation = forward + backward

        # GVC position index: log(1+forward) - log(1+backward)
        gvc_position = float(np.log(1 + forward) - np.log(1 + backward))

        # Upstreamness: U = (I - A)^{-1} * 1
        # Where A is the direct requirements matrix
        try:
            eye = np.eye(n_c)
            leontief = np.linalg.inv(eye - io_matrix)
            upstreamness_vec = leontief @ np.ones(n_c)
            country_upstreamness = float(upstreamness_vec[ci])
        except np.linalg.LinAlgError:
            upstreamness_vec = np.ones(n_c)
            country_upstreamness = 1.0

        # DVA share
        dva_share = dva_in_exports[ci] / ge

        # Cross-country comparison
        all_gvc = np.zeros(n_c)
        for i in range(n_c):
            if gross_exports[i] > 0:
                fwd = dva_in_partner[i] / gross_exports[i]
                bwd = fva_in_exports[i] / gross_exports[i]
                all_gvc[i] = fwd + bwd

        gvc_rank = int(np.sum(all_gvc > gvc_participation) + 1)
        gvc_percentile = float(1 - gvc_rank / n_c) * 100

        # Top partner linkages
        partner_rows = [r for r in records if r["reporter_iso3"] == reporter]
        partner_linkages = []
        for r in sorted(partner_rows,
                        key=lambda x: float(x["foreign_va_in_exports"] or 0),
                        reverse=True)[:10]:
            partner_linkages.append({
                "partner": r["partner_iso3"],
                "fva": float(r["foreign_va_in_exports"] or 0),
                "dva_in_partner": float(r["domestic_va_in_partner_exports"] or 0),
            })

        # Score: low GVC participation = high isolation = high score
        # Normalize: GVC participation typically ranges 0.1 to 0.7
        score = float(np.clip((1 - gvc_participation / 0.7) * 100, 0, 100))

        return {
            "score": score,
            "gvc_participation": float(gvc_participation),
            "forward_linkage": float(forward),
            "backward_linkage": float(backward),
            "gvc_position": gvc_position,
            "upstreamness": country_upstreamness,
            "dva_share": float(dva_share),
            "fva_share": float(backward),
            "gross_exports": float(ge),
            "gvc_rank": gvc_rank,
            "gvc_percentile": gvc_percentile,
            "n_countries": n_c,
            "top_partner_linkages": partner_linkages,
            "reporter": reporter,
            "year": year,
        }
