"""Bioeconomy innovation capacity: biotech patent intensity and life sciences R&D.

Innovation capacity in the bioeconomy is driven by the density of biotech-related
patents, life sciences R&D investment, the availability of skilled researchers,
and the strength of intellectual property frameworks. Countries at the frontier
combine high R&D intensity with strong researcher density and patent output, creating
self-reinforcing innovation ecosystems.

Score: high R&D + high researcher density + strong patent activity -> STABLE
(bioeconomy innovation frontier), low R&D with brain drain and weak IP -> CRISIS
(bioeconomy innovation capacity gap constraining long-run growth).

Proxies: R&D expenditure (% GDP), researchers per million population, patent
applications per billion GDP (approximated via WDI indicators).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class BioeconomyInnovationCapacity(LayerBase):
    layer_id = "lBI"
    name = "Bioeconomy Innovation Capacity"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        rnd_code = "GB.XPD.RSDV.GD.ZS"
        researcher_code = "SP.POP.SCIE.RD.P6"
        patent_code = "IP.PAT.RESD.IR.D"

        rnd_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (rnd_code, "%research and development%"),
        )
        researcher_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (researcher_code, "%Researchers in R&D%"),
        )
        patent_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (patent_code, "%Patent applications, residents%"),
        )

        rnd_vals = [r["value"] for r in rnd_rows if r["value"] is not None]
        researcher_vals = [r["value"] for r in researcher_rows if r["value"] is not None]
        patent_vals = [r["value"] for r in patent_rows if r["value"] is not None]

        if not rnd_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for R&D expenditure GB.XPD.RSDV.GD.ZS",
            }

        rnd_gdp = rnd_vals[0]
        researchers_pm = researcher_vals[0] if researcher_vals else None
        patents = patent_vals[0] if patent_vals else None

        # Base score from R&D intensity
        if rnd_gdp >= 3.5:
            base = 5.0
        elif rnd_gdp >= 2.5:
            base = 5.0 + (3.5 - rnd_gdp) * 8.0
        elif rnd_gdp >= 1.5:
            base = 13.0 + (2.5 - rnd_gdp) * 15.0
        elif rnd_gdp >= 0.5:
            base = 28.0 + (1.5 - rnd_gdp) * 25.0
        else:
            base = min(85.0, 53.0 + (0.5 - rnd_gdp) * 30.0)

        # Researcher density: life sciences capacity depends on human capital
        if researchers_pm is not None:
            if researchers_pm >= 4000:
                base = max(5.0, base - 20.0)
            elif researchers_pm >= 2000:
                base = max(5.0, base - 12.0)
            elif researchers_pm >= 500:
                base = max(5.0, base - 5.0)
            elif researchers_pm < 100:
                base = min(100.0, base + 15.0)

        # Patent activity: bioeconomy IP generation
        if patents is not None:
            if patents >= 5000:
                base = max(5.0, base - 8.0)
            elif patents >= 1000:
                base = max(5.0, base - 4.0)
            elif patents < 50:
                base = min(100.0, base + 8.0)

        score = round(min(100.0, base), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "rnd_gdp_pct": round(rnd_gdp, 2),
                "researchers_per_million": round(researchers_pm, 1) if researchers_pm is not None else None,
                "patent_applications": round(patents, 0) if patents is not None else None,
                "n_obs_rnd": len(rnd_vals),
                "n_obs_researchers": len(researcher_vals),
                "n_obs_patents": len(patent_vals),
                "innovation_frontier": rnd_gdp >= 2.5 and (researchers_pm or 0) >= 2000,
            },
        }
