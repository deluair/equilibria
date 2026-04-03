"""IP Protection Strength module.

Composite proxy for intellectual property protection strength using resident
patent applications (IP.PAT.RESD) and Rule of Law (RL.EST).

Higher patent filings by residents signal a functional IP system; stronger rule
of law underpins enforcement. Both deteriorating = high IP risk.

Score formula:
  score_pat: normalises resident patent log(applications+1) against a benchmark
             of log(50001) ~ 10.8 (high-IP economy). Inverted to stress scale.
             score_pat = clip((1 - log(pat+1)/10.8) * 100, 0, 100)
  score_rl  = clip(50 - rl_latest * 20, 0, 100)
  composite = 0.5 * score_pat + 0.5 * score_rl  (or single if one unavailable)

Sources: World Bank WDI (IP.PAT.RESD, RL.EST)
"""

from __future__ import annotations

import math

import numpy as np

from app.layers.base import LayerBase

_PAT_CODE = "IP.PAT.RESD"
_RL_CODE = "RL.EST"
_PAT_NAME = "patent applications residents"
_BENCHMARK_LOG = math.log(50001)  # ~10.82 — large high-IP economy benchmark


class IpProtectionStrength(LayerBase):
    layer_id = "lLW"
    name = "IP Protection Strength"

    async def compute(self, db, **kwargs) -> dict:
        pat_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_PAT_CODE, f"%{_PAT_NAME}%"),
        )

        rl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_RL_CODE, f"%{_RL_CODE}%"),
        )

        pat_vals = [float(r["value"]) for r in pat_rows if r["value"] is not None]
        rl_vals = [float(r["value"]) for r in rl_rows if r["value"] is not None]

        if not pat_vals and not rl_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for IP.PAT.RESD or RL.EST",
            }

        score_pat = None
        score_rl = None

        if pat_vals:
            pat_log = math.log(pat_vals[0] + 1)
            score_pat = float(np.clip((1.0 - pat_log / _BENCHMARK_LOG) * 100.0, 0.0, 100.0))

        if rl_vals:
            score_rl = float(np.clip(50.0 - rl_vals[0] * 20.0, 0.0, 100.0))

        if score_pat is not None and score_rl is not None:
            score = 0.5 * score_pat + 0.5 * score_rl
        elif score_pat is not None:
            score = score_pat
        else:
            score = score_rl

        return {
            "score": round(score, 1),
            "patent_applications_residents": round(pat_vals[0], 0) if pat_vals else None,
            "rl_latest": round(rl_vals[0], 4) if rl_vals else None,
            "score_patent_component": round(score_pat, 2) if score_pat is not None else None,
            "score_rl_component": round(score_rl, 2) if score_rl is not None else None,
            "indicators_used": (
                ([_PAT_CODE] if pat_vals else []) + ([_RL_CODE] if rl_vals else [])
            ),
            "note": "Higher score = weaker IP protection. Benchmark: 50,000 resident patents.",
        }
