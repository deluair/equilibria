"""Law-Development Nexus module.

Estimates the strength of the relationship between Rule of Law (RL.EST) and
real GDP per capita (NY.GDP.PCAP.KD) via OLS correlation across available
time-series observations. A weak or negative correlation signals that legal
quality is decoupled from economic development.

Score formula:
  Run OLS: ln(GDP_pc) ~ RL.EST  using paired observations.
  Correlation r is transformed to a stress score:
    score = clip((1 - r) / 2 * 100, 0, 100)
  r = 1.0 (perfect positive): score = 0 (no stress — law and development aligned)
  r = 0.0 (no correlation):   score = 50 (moderate stress)
  r = -1.0 (inverse):         score = 100 (extreme stress)

Sources: World Bank WDI (RL.EST, NY.GDP.PCAP.KD)
"""

from __future__ import annotations

import math

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase

_RL_CODE = "RL.EST"
_GDP_CODE = "NY.GDP.PCAP.KD"
_GDP_NAME = "GDP per capita constant"


class LawDevelopmentNexus(LayerBase):
    layer_id = "lLW"
    name = "Law Development Nexus"

    async def compute(self, db, **kwargs) -> dict:
        rl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_RL_CODE, f"%{_RL_CODE}%"),
        )

        gdp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_GDP_CODE, f"%{_GDP_NAME}%"),
        )

        rl_vals = [float(r["value"]) for r in rl_rows if r["value"] is not None]
        gdp_vals = [float(r["value"]) for r in gdp_rows if r["value"] is not None]

        if not rl_vals or not gdp_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for OLS: need both RL.EST and NY.GDP.PCAP.KD",
            }

        n = min(len(rl_vals), len(gdp_vals))
        if n < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"insufficient paired observations: {n} < 3",
            }

        rl_arr = np.array(rl_vals[:n])
        gdp_arr = np.array(gdp_vals[:n])
        log_gdp = np.array([math.log(g) for g in gdp_arr if g > 0])

        if len(log_gdp) < n:
            # Some GDP values were non-positive; trim rl_arr to match
            valid = [i for i, g in enumerate(gdp_arr) if g > 0]
            rl_arr = rl_arr[valid]
            log_gdp = log_gdp

        if len(rl_arr) < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "fewer than 3 valid paired observations after filtering",
            }

        result = linregress(rl_arr, log_gdp)
        r = float(result.rvalue)
        slope = float(result.slope)
        intercept = float(result.intercept)
        r_squared = float(result.rvalue ** 2)

        # r in [-1, 1]. Transform: higher r = better alignment = lower stress.
        score = float(np.clip((1.0 - r) / 2.0 * 100.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "ols_r": round(r, 4),
            "ols_r_squared": round(r_squared, 4),
            "ols_slope": round(slope, 4),
            "ols_intercept": round(intercept, 4),
            "n_paired_obs": len(rl_arr),
            "rl_latest": round(rl_vals[0], 4),
            "gdp_pc_latest": round(gdp_vals[0], 2),
            "note": (
                "OLS: ln(GDP_pc) ~ RL.EST. r=1 -> score=0 (aligned). "
                "r=-1 -> score=100 (decoupled)."
            ),
        }
