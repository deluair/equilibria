"""Bioenergy transition: bioenergy share of renewable energy and biomass potential.

Bioenergy -- from agricultural residues, forestry waste, dedicated energy crops,
and municipal organic waste -- is the largest renewable energy source globally by
final energy consumption. Its economic role spans electricity generation, heat,
and liquid biofuels for transport. Effective bioenergy transition balances
feedstock sustainability with energy security gains.

Score: high renewable energy share + rising access to clean fuels -> STABLE
(energy transition progressing), high biomass burning with low clean fuel access
-> STRESS (traditional biomass dependency, not modern bioenergy), low renewables
with fossil-lock-in -> CRISIS.

Proxies: renewable energy (% total final energy), access to clean fuels (% population),
fossil fuel energy consumption (% total) as transition headroom indicator.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class BioenergyTransition(LayerBase):
    layer_id = "lBI"
    name = "Bioenergy Transition"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        renew_code = "EG.FEC.RNEW.ZS"
        clean_fuel_code = "EG.CFT.ACCS.ZS"
        fossil_code = "EG.USE.COMM.FO.ZS"

        renew_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (renew_code, "%Renewable energy consumption%"),
        )
        clean_fuel_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (clean_fuel_code, "%Access to clean fuels%"),
        )
        fossil_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (fossil_code, "%Fossil fuel energy%"),
        )

        renew_vals = [r["value"] for r in renew_rows if r["value"] is not None]
        clean_fuel_vals = [r["value"] for r in clean_fuel_rows if r["value"] is not None]
        fossil_vals = [r["value"] for r in fossil_rows if r["value"] is not None]

        if not renew_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for renewable energy consumption EG.FEC.RNEW.ZS",
            }

        renew_pct = renew_vals[0]
        clean_fuel_pct = clean_fuel_vals[0] if clean_fuel_vals else None
        fossil_pct = fossil_vals[0] if fossil_vals else None

        # Renewable trend
        renew_trend = round(renew_vals[0] - renew_vals[-1], 2) if len(renew_vals) > 1 else None

        # Base score from renewable share: higher renewable = lower stress
        if renew_pct >= 60:
            base = 10.0
        elif renew_pct >= 40:
            base = 10.0 + (60 - renew_pct) * 1.0
        elif renew_pct >= 20:
            base = 30.0 + (40 - renew_pct) * 1.5
        elif renew_pct >= 10:
            base = 60.0 + (20 - renew_pct) * 1.0
        else:
            base = min(90.0, 70.0 + (10 - renew_pct) * 1.0)

        # Traditional biomass trap: high renewables but low clean fuel access
        # signals traditional biomass burning, not modern bioenergy
        if clean_fuel_pct is not None:
            if clean_fuel_pct >= 80:
                base = max(5.0, base - 10.0)  # modern energy system
            elif clean_fuel_pct < 30 and renew_pct > 40:
                base = min(100.0, base + 20.0)  # traditional biomass trap
            elif clean_fuel_pct < 30:
                base = min(100.0, base + 10.0)

        # Fossil fuel lock-in
        if fossil_pct is not None:
            if fossil_pct >= 80:
                base = min(100.0, base + 10.0)
            elif fossil_pct < 40:
                base = max(5.0, base - 5.0)

        score = round(min(100.0, base), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "renewable_energy_pct": round(renew_pct, 2),
                "clean_fuel_access_pct": round(clean_fuel_pct, 2) if clean_fuel_pct is not None else None,
                "fossil_fuel_pct": round(fossil_pct, 2) if fossil_pct is not None else None,
                "renewable_trend_pct": renew_trend,
                "n_obs_renew": len(renew_vals),
                "n_obs_clean_fuel": len(clean_fuel_vals),
                "n_obs_fossil": len(fossil_vals),
                "traditional_biomass_trap": (
                    clean_fuel_pct is not None and clean_fuel_pct < 30 and renew_pct > 40
                ),
            },
        }
