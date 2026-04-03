"""Poverty trap detection via threshold estimation and bimodality tests.

Identifies multiple equilibria in cross-country income distribution by
testing for bimodality (twin peaks) and estimating threshold income levels
that separate convergence clubs.

Key references:
    Quah, D. (1996). Twin peaks: growth and convergence in models of
        distribution dynamics. Economic Journal, 106(437), 1045-1055.
    Bloom, D., Canning, D. & Sevilla, J. (2003). Geography and poverty traps.
        Journal of Economic Growth, 8(4), 355-378.
    Azariadis, C. & Stachurski, J. (2005). Poverty traps. Handbook of
        Economic Growth, 1, 295-384.
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats
from scipy.signal import argrelextrema

from app.layers.base import LayerBase


class PovertyTrap(LayerBase):
    layer_id = "l4"
    name = "Poverty Trap Detection"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Detect poverty traps through bimodality and threshold estimation.

        Tests the cross-country distribution of log GDP per capita for
        bimodality (twin peaks), estimates the antimode (threshold), and
        checks for persistence of low-income clusters.

        Returns dict with score, bimodality test results, threshold
        estimates, and convergence club membership.
        """
        country_iso3 = kwargs.get("country_iso3")

        rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
              AND dp.value > 0
            ORDER BY dp.date DESC
            """
        )

        if not rows:
            return {"score": 50, "results": {"error": "no GDP per capita data"}}

        # Get most recent value per country
        latest: dict[str, float] = {}
        for r in rows:
            iso = r["country_iso3"]
            if iso not in latest:
                latest[iso] = r["value"]

        values = np.array(list(latest.values()))
        log_values = np.log(values)

        if len(log_values) < 20:
            return {"score": 50, "results": {"error": "insufficient countries"}}

        # Bimodality coefficient: BC = (skewness^2 + 1) / kurtosis
        # BC > 5/9 suggests bimodality
        skew = float(sp_stats.skew(log_values))
        kurt = float(sp_stats.kurtosis(log_values, fisher=False))
        bc = (skew**2 + 1) / kurt if kurt > 0 else 0
        bc_threshold = 5.0 / 9.0
        is_bimodal_bc = bc > bc_threshold

        # Hartigan's dip test approximation via kernel density
        # Estimate KDE and count modes
        kde_x = np.linspace(log_values.min() - 1, log_values.max() + 1, 512)
        kernel = sp_stats.gaussian_kde(log_values)
        kde_y = kernel(kde_x)

        # Find local maxima (modes) and minima (antimodes)
        local_max_idx = argrelextrema(kde_y, np.greater, order=10)[0]
        local_min_idx = argrelextrema(kde_y, np.less, order=10)[0]

        n_modes = len(local_max_idx)
        modes = [float(kde_x[i]) for i in local_max_idx]
        antimodes = [float(kde_x[i]) for i in local_min_idx]

        # Threshold estimation: use the deepest antimode between the two highest peaks
        threshold = None
        if n_modes >= 2 and len(antimodes) >= 1:
            # Find antimode with lowest density between two highest peaks
            peak_heights = [(kde_y[i], i) for i in local_max_idx]
            peak_heights.sort(reverse=True)
            top_peaks = sorted([peak_heights[0][1], peak_heights[1][1]])

            candidate_antimodes = [
                i for i in local_min_idx if top_peaks[0] < i < top_peaks[1]
            ]
            if candidate_antimodes:
                best = min(candidate_antimodes, key=lambda i: kde_y[i])
                threshold = float(kde_x[best])

        # Classify countries into clubs
        clubs = None
        target_club = None
        if threshold is not None:
            low_club = [iso for iso, v in latest.items() if np.log(v) < threshold]
            high_club = [iso for iso, v in latest.items() if np.log(v) >= threshold]
            clubs = {
                "low": {"n": len(low_club), "mean_gdppc": float(np.exp(np.mean([np.log(latest[c]) for c in low_club]))) if low_club else 0},
                "high": {"n": len(high_club), "mean_gdppc": float(np.exp(np.mean([np.log(latest[c]) for c in high_club]))) if high_club else 0},
                "threshold_gdppc": float(np.exp(threshold)),
            }
            if country_iso3:
                target_club = "low" if country_iso3 in low_club else "high" if country_iso3 in high_club else None

        # Persistence test: check if countries in bottom quartile stayed there
        # Fetch earlier data for mobility check
        early_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
              AND dp.value > 0
              AND dp.date <= '2000'
            ORDER BY dp.date ASC
            """
        )
        persistence = None
        if early_rows:
            earliest: dict[str, float] = {}
            for r in early_rows:
                iso = r["country_iso3"]
                if iso not in earliest:
                    earliest[iso] = r["value"]

            common = set(earliest.keys()) & set(latest.keys())
            if len(common) >= 20:
                early_vals = {c: earliest[c] for c in common}
                late_vals = {c: latest[c] for c in common}
                early_q25 = np.percentile(list(early_vals.values()), 25)
                late_q25 = np.percentile(list(late_vals.values()), 25)

                early_bottom = {c for c, v in early_vals.items() if v <= early_q25}
                late_bottom = {c for c, v in late_vals.items() if v <= late_q25}

                stuck = early_bottom & late_bottom
                escaped = early_bottom - late_bottom
                persistence = {
                    "n_early_bottom": len(early_bottom),
                    "n_stayed": len(stuck),
                    "n_escaped": len(escaped),
                    "persistence_rate": len(stuck) / len(early_bottom) if early_bottom else 0,
                }

        # Score: evidence of poverty traps = high score (stress)
        # No traps, convergence = low score (stable)
        trap_signals = 0
        if is_bimodal_bc:
            trap_signals += 1
        if n_modes >= 2:
            trap_signals += 1
        if persistence and persistence["persistence_rate"] > 0.7:
            trap_signals += 1

        if trap_signals >= 3:
            score = 80
        elif trap_signals == 2:
            score = 65
        elif trap_signals == 1:
            score = 45
        else:
            score = 25

        # Adjust for target country
        if target_club == "low":
            score = min(95, score + 15)
        elif target_club == "high":
            score = max(10, score - 10)

        results = {
            "bimodality": {
                "bimodality_coefficient": bc,
                "bc_threshold": bc_threshold,
                "is_bimodal": is_bimodal_bc,
                "n_modes": n_modes,
                "modes_log_gdp": modes,
                "antimodes_log_gdp": antimodes,
            },
            "threshold": threshold,
            "clubs": clubs,
            "persistence": persistence,
            "country_iso3": country_iso3,
            "target_club": target_club,
            "n_countries": len(latest),
        }

        return {"score": score, "results": results}
