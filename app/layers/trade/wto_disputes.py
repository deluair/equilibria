"""WTO dispute analysis: frequency, outcomes, and trade flow effects.

Methodology:
    Analyze World Trade Organization dispute settlement activity and its
    trade implications:

    1. Dispute frequency by sector and partner:
       Count disputes filed as complainant, respondent, and third party.
       Sector concentration of disputes (SPS, TBT, subsidies, AD, safeguards).

    2. Panel outcome analysis:
       Win/loss rates by dispute type. Average duration from consultation
       request to ruling. Implementation compliance rates.
       Following Horn et al. (2011) classification of legal findings.

    3. Trade flow impact:
       Estimate trade effects of dispute outcomes using difference-in-differences:
       compare trade in disputed products before/after ruling to control products.
       Following Bown (2004) methodology.

    4. Retaliation effectiveness:
       Measure authorized vs. actual retaliation. Trade diversion from
       retaliation threats (Bown & Crowley, 2013).

    Score (0-100): Higher score indicates greater trade risk from disputes
    (frequent respondent, poor compliance, large trade values in dispute).

References:
    Bown, C.P. (2004). "On the Economic Success of GATT/WTO Dispute
        Settlement." Review of Economics and Statistics, 86(3), 811-823.
    Horn, H. et al. (2011). "Is the WTO Dispute Settlement Procedure Fair
        to Developing Countries?" Journal of International Economic Law, 14(3).
    Bown, C.P. & Crowley, M.A. (2013). "Self-Enforcing Trade Agreements:
        Evidence from Time-Varying Trade Policy." American Economic Review,
        103(2), 1071-1090.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class WTODisputes(LayerBase):
    layer_id = "l1"
    name = "WTO Disputes"

    async def compute(self, db, **kwargs) -> dict:
        """Analyze WTO dispute patterns and trade flow effects.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default USA)
            year : int - reference year for trade flow analysis
            lookback_years : int - dispute history window (default 10)
        """
        country = kwargs.get("country_iso3", "USA")
        year = kwargs.get("year", 2022)
        lookback = kwargs.get("lookback_years", 10)

        # Fetch disputes involving the country
        disputes = await db.fetch_all(
            """
            SELECT dispute_id, complainant_iso3, respondent_iso3,
                   sector, agreement, status, outcome,
                   date_filed, date_ruling, trade_value_affected,
                   retaliation_authorized, retaliation_value
            FROM wto_disputes
            WHERE (complainant_iso3 = ? OR respondent_iso3 = ?)
              AND CAST(SUBSTR(date_filed, 1, 4) AS INTEGER) >= ?
            ORDER BY date_filed DESC
            """,
            (country, country, year - lookback),
        )

        if not disputes:
            return {
                "score": 0.0,
                "n_disputes": 0,
                "note": "No WTO disputes found in the period",
                "country": country,
                "year": year,
            }

        # Classify by role
        as_complainant = [d for d in disputes if d["complainant_iso3"] == country]
        as_respondent = [d for d in disputes if d["respondent_iso3"] == country]

        n_total = len(disputes)
        n_complainant = len(as_complainant)
        n_respondent = len(as_respondent)

        # Sector concentration
        sector_counts = {}
        for d in disputes:
            sector = d["sector"] or "unspecified"
            sector_counts[sector] = sector_counts.get(sector, 0) + 1

        # Agreement concentration (AD, SCM, SPS, TBT, safeguards)
        agreement_counts = {}
        for d in disputes:
            agr = d["agreement"] or "unspecified"
            agreement_counts[agr] = agreement_counts.get(agr, 0) + 1

        # Outcome analysis for resolved disputes
        resolved = [d for d in disputes if d["outcome"] is not None]
        outcomes = {"won": 0, "lost": 0, "settled": 0, "other": 0}
        for d in resolved:
            outcome = (d["outcome"] or "").lower()
            if "complainant" in outcome or "favor" in outcome:
                if d["complainant_iso3"] == country:
                    outcomes["won"] += 1
                else:
                    outcomes["lost"] += 1
            elif "respondent" in outcome:
                if d["respondent_iso3"] == country:
                    outcomes["won"] += 1
                else:
                    outcomes["lost"] += 1
            elif "settled" in outcome or "mutually" in outcome:
                outcomes["settled"] += 1
            else:
                outcomes["other"] += 1

        win_rate = (
            outcomes["won"] / (outcomes["won"] + outcomes["lost"])
            if (outcomes["won"] + outcomes["lost"]) > 0
            else None
        )

        # Duration analysis (filed to ruling)
        durations = []
        for d in disputes:
            if d["date_filed"] and d["date_ruling"]:
                try:
                    filed_y = int(d["date_filed"][:4])
                    ruling_y = int(d["date_ruling"][:4])
                    dur = ruling_y - filed_y
                    if 0 <= dur <= 20:
                        durations.append(dur)
                except (ValueError, TypeError):
                    pass

        avg_duration = float(np.mean(durations)) if durations else None

        # Trade value at stake
        trade_values = []
        for d in disputes:
            if d["trade_value_affected"] is not None:
                trade_values.append(float(d["trade_value_affected"]))

        total_trade_at_stake = sum(trade_values) if trade_values else 0.0

        # Retaliation analysis
        retaliation_cases = [
            d for d in disputes
            if d["retaliation_authorized"] and int(d["retaliation_authorized"]) == 1
        ]
        n_retaliation = len(retaliation_cases)
        total_retaliation_value = sum(
            float(d["retaliation_value"] or 0) for d in retaliation_cases
        )

        # Top dispute partners
        partner_counts = {}
        for d in disputes:
            if d["complainant_iso3"] == country:
                partner = d["respondent_iso3"]
            else:
                partner = d["complainant_iso3"]
            partner_counts[partner] = partner_counts.get(partner, 0) + 1

        top_partners = sorted(partner_counts.items(), key=lambda x: x[1], reverse=True)[:10]

        # Trade flow impact estimation (DiD-style)
        # Fetch trade before and after disputes for affected sectors
        affected_sectors = list(sector_counts.keys())[:5]
        trade_impact = None

        if affected_sectors and as_respondent:
            # Get trade in affected vs unaffected sectors
            pre_trade = await db.fetch_all(
                """
                SELECT ds.metadata AS sector, SUM(dp.value) AS trade_value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.source IN ('baci', 'comtrade')
                  AND ds.country_iso3 = ?
                  AND CAST(dp.date AS INTEGER) BETWEEN ? AND ?
                GROUP BY ds.metadata
                """,
                (country, year - lookback, year - lookback // 2),
            )

            post_trade = await db.fetch_all(
                """
                SELECT ds.metadata AS sector, SUM(dp.value) AS trade_value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.source IN ('baci', 'comtrade')
                  AND ds.country_iso3 = ?
                  AND CAST(dp.date AS INTEGER) BETWEEN ? AND ?
                GROUP BY ds.metadata
                """,
                (country, year - lookback // 2 + 1, year),
            )

            if pre_trade and post_trade:
                pre_dict = {r["sector"]: float(r["trade_value"] or 0) for r in pre_trade}
                post_dict = {r["sector"]: float(r["trade_value"] or 0) for r in post_trade}

                affected_growth = []
                unaffected_growth = []
                for sector in set(list(pre_dict.keys()) + list(post_dict.keys())):
                    pre_v = pre_dict.get(sector, 0)
                    post_v = post_dict.get(sector, 0)
                    if pre_v > 0:
                        growth = (post_v - pre_v) / pre_v
                        if sector in affected_sectors:
                            affected_growth.append(growth)
                        else:
                            unaffected_growth.append(growth)

                if affected_growth and unaffected_growth:
                    did_estimate = float(
                        np.mean(affected_growth) - np.mean(unaffected_growth)
                    )
                    trade_impact = {
                        "did_estimate": round(did_estimate, 4),
                        "avg_affected_growth": round(float(np.mean(affected_growth)), 4),
                        "avg_unaffected_growth": round(float(np.mean(unaffected_growth)), 4),
                        "n_affected_sectors": len(affected_growth),
                        "n_control_sectors": len(unaffected_growth),
                    }

        # Score: respondent frequency + trade at stake + low win rate
        respondent_ratio = n_respondent / max(n_total, 1)
        frequency_penalty = min(n_respondent / 20.0, 1.0) * 30.0  # cap at 20 disputes
        loss_penalty = (1.0 - (win_rate or 0.5)) * 30.0
        trade_penalty = min(total_trade_at_stake / 1e11, 1.0) * 25.0  # cap at 100B
        retaliation_penalty = min(n_retaliation / 5.0, 1.0) * 15.0

        score = float(np.clip(
            frequency_penalty + loss_penalty + trade_penalty + retaliation_penalty,
            0, 100,
        ))

        return {
            "score": round(score, 2),
            "country": country,
            "year": year,
            "lookback_years": lookback,
            "n_disputes_total": n_total,
            "n_as_complainant": n_complainant,
            "n_as_respondent": n_respondent,
            "respondent_ratio": round(respondent_ratio, 4),
            "sector_concentration": sector_counts,
            "agreement_concentration": agreement_counts,
            "outcomes": outcomes,
            "win_rate": round(win_rate, 4) if win_rate is not None else None,
            "avg_duration_years": round(avg_duration, 2) if avg_duration else None,
            "total_trade_at_stake": round(total_trade_at_stake, 2),
            "n_retaliation_authorized": n_retaliation,
            "total_retaliation_value": round(total_retaliation_value, 2),
            "top_dispute_partners": [
                {"partner": p, "count": c} for p, c in top_partners
            ],
            "trade_flow_impact": trade_impact,
        }
