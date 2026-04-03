"""Bio-based industry share: bio-based products as share of industrial output.

Bio-based industries convert biological resources into goods and energy,
spanning agricultural processing, bio-chemicals, bio-plastics, and industrial
biotechnology. A higher share of bio-based output in total industrial production
signals a mature bioeconomy with reduced fossil-material dependence.

Score: low bio-based share -> STABLE nascent transition, rising share with
moderate productivity -> WATCH active development, high share alongside
strong agri-processing -> STRESS structural lock-in pressure, stalled
transition with high chemical import reliance -> CRISIS.

Proxy: agriculture value added (% GDP) combined with manufacturing share,
used as a lower-bound estimate of bio-based industrial throughput.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class BiobsedIndustryShare(LayerBase):
    layer_id = "lBI"
    name = "Bio-based Industry Share"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        agri_code = "NV.AGR.TOTL.ZS"
        manuf_code = "NV.IND.MANF.ZS"

        agri_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (agri_code, "%Agriculture, forestry%"),
        )
        manuf_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (manuf_code, "%Manufacturing, value added%"),
        )

        agri_vals = [r["value"] for r in agri_rows if r["value"] is not None]
        manuf_vals = [r["value"] for r in manuf_rows if r["value"] is not None]

        if not agri_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for agriculture value added NV.AGR.TOTL.ZS",
            }

        agri_gdp = agri_vals[0]
        manuf_gdp = manuf_vals[0] if manuf_vals else None

        # Proxy bio-based share: agri-processing as fraction of manufacturing
        # Higher agri share with moderate manufacturing = active bio-based sector
        if manuf_gdp and manuf_gdp > 0:
            bio_proxy = agri_gdp / manuf_gdp * 100.0
        else:
            bio_proxy = agri_gdp  # fall back to agri-only proxy

        # Score: bio_proxy high (agri-led economy, not yet value-added) -> rising stress
        # Moderate proxy with rising manufacturing -> WATCH (transition phase)
        if bio_proxy < 20:
            score = 10.0 + bio_proxy * 0.5
        elif bio_proxy < 50:
            score = 20.0 + (bio_proxy - 20) * 0.8
        elif bio_proxy < 100:
            score = 44.0 + (bio_proxy - 50) * 0.5
        else:
            score = min(100.0, 69.0 + (bio_proxy - 100) * 0.3)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "agriculture_value_added_gdp_pct": round(agri_gdp, 2),
                "manufacturing_value_added_gdp_pct": round(manuf_gdp, 2) if manuf_gdp is not None else None,
                "biobased_proxy_index": round(bio_proxy, 2),
                "n_obs_agri": len(agri_vals),
                "n_obs_manuf": len(manuf_vals),
            },
        }
