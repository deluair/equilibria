"""Shadow Banking analysis module.

Methodology
-----------
Shadow banking refers to credit intermediation outside the regulated banking
system (Pozsar et al. 2010, FSB). Key measurement and risk dimensions:

1. Size: Non-bank financial intermediation (NBFI) as % of total financial
   assets. FSB Global Monitoring Report uses narrow and broad measures.
   - Narrow measure: NBFI entities with bank-like risks (MMFs, repo, ABCP)
   - Broad measure: total non-bank financial sector

2. Repo market stress: repo rate spread over risk-free rate. Widening
   spread signals collateral quality concerns or funding stress
   (Gorton & Metrick 2012).

3. Money market fund (MMF) vulnerability: NAV deviation from $1 (breaking
   the buck risk), total net assets trend, institutional vs retail splits.

4. Regulatory arbitrage index: estimated share of bank credit intermediated
   outside capital requirements -- proxied by rapid NBFI growth in
   post-regulation periods.

Score (0-100): higher score indicates greater shadow banking risk --
large NBFI sector, widening repo spreads, MMF vulnerability, regulatory
arbitrage.

References:
    Pozsar, Z., Adrian, T., Ashcraft, A. and Boesky, H. (2010). "Shadow
        Banking." Federal Reserve Bank of New York Staff Report 458.
    Financial Stability Board (2023). "Global Monitoring Report on
        Non-Bank Financial Intermediation." FSB, Basel.
    Gorton, G. and Metrick, A. (2012). "Securitized Banking and the Run
        on Repo." Journal of Financial Economics, 104(3), 425-451.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class ShadowBanking(LayerBase):
    layer_id = "l2"
    name = "Shadow Banking"

    async def compute(self, db, **kwargs) -> dict:
        """Compute shadow banking risk indicators.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country : str - ISO3 country code
            year    : int - reference year
        """
        country = kwargs.get("country", "USA")

        series_map = {
            "nbfi_assets":      f"NBFI_ASSETS_PCT_{country}",
            "bank_assets":      f"BANK_ASSETS_PCT_{country}",
            "repo_rate":        f"REPO_RATE_{country}",
            "risk_free":        f"RISK_FREE_RATE_{country}",
            "mmf_nav":          f"MMF_NAV_{country}",
            "mmf_assets":       f"MMF_ASSETS_{country}",
            "credit_growth":    f"CREDIT_GROWTH_{country}",
            "nbfi_growth":      f"NBFI_GROWTH_{country}",
        }

        data: dict[str, np.ndarray] = {}
        for label, code in series_map.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
                (code,),
            )
            if rows:
                data[label] = np.array([float(r[1]) for r in rows])

        if len(data) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"Insufficient shadow banking data for {country}",
            }

        results: dict = {"country": country}

        # --- 1. NBFI size ---
        nbfi_size = {}
        nbfi_pct = 0.0
        if "nbfi_assets" in data:
            nb = data["nbfi_assets"]
            nbfi_pct = float(nb[-1])
            if len(nb) >= 5:
                x_t = np.arange(len(nb), dtype=float)
                slope_n, _, r_n, p_n, _ = stats.linregress(x_t, nb)
                nbfi_size = {
                    "latest_nbfi_pct_total": round(nbfi_pct, 2),
                    "trend_slope": round(float(slope_n), 4),
                    "r_squared": round(r_n ** 2, 4),
                    "p_value": round(float(p_n), 4),
                    "rapidly_growing": float(slope_n) > 1.0,
                    "large_sector": nbfi_pct > 40,
                }
            else:
                nbfi_size = {"latest_nbfi_pct_total": round(nbfi_pct, 2)}
        else:
            nbfi_size = {"note": "NBFI asset data unavailable"}

        results["nbfi_size"] = nbfi_size

        # --- 2. Repo market stress ---
        repo_stress = {}
        repo_spread_latest = 0.0
        if "repo_rate" in data and "risk_free" in data:
            repo = data["repo_rate"]
            rf = data["risk_free"]
            n_r = min(len(repo), len(rf))
            if n_r >= 5:
                spread = repo[-n_r:] - rf[-n_r:]
                repo_spread_latest = float(spread[-1])
                repo_stress = {
                    "spread_latest_bps": round(repo_spread_latest * 100, 2),
                    "spread_mean_bps": round(float(np.mean(spread)) * 100, 2),
                    "spread_std_bps": round(float(np.std(spread, ddof=1)) * 100, 2),
                    "z_score": round(
                        float((spread[-1] - np.mean(spread)) / (np.std(spread, ddof=1) + 1e-8)), 3
                    ),
                    "stress_flag": repo_spread_latest > 0.005,  # >50bps
                    "historical_max_bps": round(float(np.max(spread)) * 100, 2),
                }
        else:
            repo_stress = {"note": "repo rate data unavailable"}

        results["repo_market"] = repo_stress

        # --- 3. Money market fund vulnerability ---
        mmf_vuln = {}
        mmf_risk = 0.0
        if "mmf_nav" in data:
            nav = data["mmf_nav"]
            # NAV per $1 (should be exactly 1.0; <0.9950 = "breaking the buck")
            nav_dev = abs(float(nav[-1]) - 1.0)
            breaking_buck = float(nav[-1]) < 0.995
            mmf_vuln = {
                "nav_latest": round(float(nav[-1]), 4),
                "nav_deviation": round(nav_dev, 6),
                "breaking_buck_risk": breaking_buck,
                "min_nav": round(float(np.min(nav)), 4),
            }
            if "mmf_assets" in data and len(data["mmf_assets"]) >= 3:
                ma = data["mmf_assets"]
                mmf_vuln["assets_trend"] = round(
                    float(np.mean(np.diff(ma[-6:] if len(ma) >= 6 else ma))), 2
                )
            mmf_risk = nav_dev * 50
        else:
            mmf_vuln = {"note": "MMF NAV data unavailable"}

        results["mmf_vulnerability"] = mmf_vuln

        # --- 4. Regulatory arbitrage ---
        reg_arb = {}
        arb_penalty = 0.0
        if "nbfi_growth" in data and "credit_growth" in data:
            nbfi_g = data["nbfi_growth"]
            credit_g = data["credit_growth"]
            n_a = min(len(nbfi_g), len(credit_g))
            if n_a >= 5:
                nbfi_ex = nbfi_g[-n_a:] - credit_g[-n_a:]  # excess NBFI growth
                mean_ex = float(np.mean(nbfi_ex[-5:] if n_a >= 5 else nbfi_ex))
                arb_index = float(np.clip(max(0.0, mean_ex), 0, 20))
                arb_penalty = arb_index * 2.5
                reg_arb = {
                    "nbfi_excess_growth": round(mean_ex, 4),
                    "arbitrage_index": round(arb_index, 2),
                    "arbitrage_present": mean_ex > 2.0,
                }
        else:
            reg_arb = {"note": "growth data unavailable"}

        results["regulatory_arbitrage"] = reg_arb

        # --- Score ---
        # Large NBFI sector
        nbfi_penalty = min(max(0.0, nbfi_pct - 20) * 0.8, 25)

        # Repo stress (z-score based)
        repo_z = repo_stress.get("z_score", 0.0)
        repo_penalty = min(max(0.0, float(repo_z) if isinstance(repo_z, (int, float)) else 0.0) * 8, 25)

        # MMF vulnerability
        mmf_penalty = min(float(mmf_risk), 25)

        # Regulatory arbitrage
        score = float(np.clip(nbfi_penalty + repo_penalty + mmf_penalty + arb_penalty, 0, 100))

        return {"score": round(score, 2), "results": results}
