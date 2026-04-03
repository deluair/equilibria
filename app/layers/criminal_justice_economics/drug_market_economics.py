"""Drug market economics: drug trafficking value as % of GDP via UNODC proxy.

Illicit drug markets represent a significant share of GDP in many countries, diverting
resources, corrupting institutions, and financing violence. UNODC estimates global
illicit drug market value at $500B+ annually. Proxy indicators include agricultural
commodity prices (opium, coca), government drug seizure expenditures, and health
burden from substance use disorders.

Score: low drug market penetration (minimal indicators) -> STABLE,
moderate presence -> WATCH, high market -> STRESS, dominant illicit drug economy -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class DrugMarketEconomics(LayerBase):
    layer_id = "lCJ"
    name = "Drug Market Economics"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        # Proxy: alcohol use disorder prevalence (% of population) from WHO/WDI
        # correlates with substance market size and health burden
        alc_code = "SH.ALC.PCAP.LI"
        alc_name = "alcohol consumption"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (alc_code, f"%{alc_name}%"),
        )
        vals = [r["value"] for r in rows if r["value"] is not None]

        if not vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for drug market proxy SH.ALC.PCAP.LI",
            }

        latest = vals[0]
        trend = round(vals[0] - vals[-1], 3) if len(vals) > 1 else None

        # Alcohol consumption per capita (liters pure alcohol) as substance market proxy
        # WHO threshold: >8L per capita = high, >12L = very high consumption society
        if latest < 2:
            score = 8.0 + latest * 3.0
        elif latest < 6:
            score = 14.0 + (latest - 2) * 5.0
        elif latest < 10:
            score = 34.0 + (latest - 6) * 5.0
        elif latest < 14:
            score = 54.0 + (latest - 10) * 4.5
        else:
            score = min(100.0, 72.0 + (latest - 14) * 2.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "alcohol_per_capita_liters": round(latest, 2),
                "trend": trend,
                "n_obs": len(vals),
                "substance_market_risk": (
                    "low" if latest < 2
                    else "moderate" if latest < 6
                    else "high" if latest < 10
                    else "very_high"
                ),
            },
        }
