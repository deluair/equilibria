"""Migration demographic balance: net migration effect on working-age structure.

Net migration modifies the effective working-age population independently of
natural demographic dynamics. Large positive net migration can temporarily offset
aging and labor shortages (the EU experience post-2004 accession), but places
pressure on housing, services, and social cohesion if the pace exceeds absorptive
capacity. Large negative net migration (emigration) drains the skilled working-age
cohort, creating human capital loss and remittance dependency. Docquier and Rapoport
(2012) document the "brain drain" effect; Clemens (2011) quantifies the wage gains
to migrants and receiving economies.

Indicator: SM.POP.NETM (World Bank WDI)
    Net migration (persons, 5-year estimates)
Positive: net immigration (inflow)
Negative: net emigration (outflow)
Near-zero: balanced migration

Score (0-100): higher = greater demographic stress from migration imbalance
    Moderate net immigration: low score (demographic dividend)
    Extreme emigration (brain drain): high score
    Extreme immigration (absorption stress): moderate-high score

References:
    Docquier, F. & Rapoport, H. (2012). Globalization, brain drain, and
        development. Journal of Economic Literature, 50(3), 681-730.
    Clemens, M.A. (2011). Economics and emigration: trillion-dollar bills
        on the sidewalk? Journal of Economic Perspectives, 25(3), 83-106.
    Ottaviano, G. & Peri, G. (2012). Rethinking the effect of immigration
        on wages. Journal of the European Economic Association, 10(1), 152-197.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MigrationDemographicBalance(LayerBase):
    layer_id = "lDT"
    name = "Migration Demographic Balance"

    async def compute(self, db, **kwargs) -> dict:
        code = "SM.POP.NETM"
        name = "net migration"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no net migration data"}

        values = [row["value"] for row in rows if row["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no net migration data"}

        latest = float(values[0])
        avg = float(np.mean(values))
        max_abs = float(max(abs(v) for v in values))

        net_emigration = latest < 0
        brain_drain_risk = latest < -50_000
        absorption_stress = latest > 200_000

        score = _net_migration_to_score(latest, max_abs)
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "net_migration_persons": round(latest, 0),
            "avg_net_migration_15y": round(avg, 0),
            "net_emigration": net_emigration,
            "brain_drain_risk": brain_drain_risk,
            "absorption_stress": absorption_stress,
            "migration_direction": "immigration" if latest > 0 else "emigration" if latest < 0 else "balanced",
            "n_obs": len(values),
            "indicator": code,
        }


def _net_migration_to_score(net: float, max_abs: float) -> float:
    # Normalize relative to historical max for scale-invariance
    # Absolute bounds: large emigration (-500k+) -> crisis; large immigration (1M+) -> stress
    if abs(net) < 10_000:
        return 15.0  # near-balanced, low stress
    if net < 0:
        # Emigration: stress rises with scale
        magnitude = abs(net)
        if magnitude < 50_000:
            return 15.0 + (magnitude / 50_000) * 20.0
        if magnitude < 200_000:
            return 35.0 + ((magnitude - 50_000) / 150_000) * 25.0
        return min(100.0, 60.0 + ((magnitude - 200_000) / 300_000) * 40.0)
    # Immigration: absorption stress
    if net < 100_000:
        return 10.0 + (net / 100_000) * 10.0
    if net < 500_000:
        return 20.0 + ((net - 100_000) / 400_000) * 20.0
    return min(100.0, 40.0 + ((net - 500_000) / 500_000) * 35.0)
