"""Shift-share IV (Bartik instruments).

Constructs Bartik instruments from industry (or sector) shares and aggregate
shifts, then runs IV regression. Provides exposure-robust standard errors
following Borusyak, Hull, & Jaravel (2022) and Rotemberg weight decomposition.

References:
    Bartik, T. J. (1991). Who Benefits from State and Local Economic
    Development Policies? W.E. Upjohn Institute.

    Borusyak, K., Hull, P., & Jaravel, X. (2022). Quasi-experimental
    shift-share research designs. Review of Economic Studies, 89(1), 181-213.

    Goldsmith-Pinkham, P., Sorkin, I., & Swift, H. (2020). Bartik instruments:
    What, when, why, and how. American Economic Review, 110(8), 2586-2624.

    Adao, R., Kolesar, M., & Morales, E. (2019). Shift-share designs:
    Theory and inference. Quarterly Journal of Economics, 134(4), 1949-2010.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats


@dataclass
class ShiftShareResult:
    """Output from a shift-share IV estimation.

    Attributes:
        iv_coef: IV coefficient on the endogenous variable.
        iv_se: Standard error (exposure-robust if computed).
        iv_pval: P-value.
        iv_ci_lower: Lower 95% CI.
        iv_ci_upper: Upper 95% CI.
        first_stage_f: First-stage F-statistic.
        rotemberg_weights: Dictionary mapping sector IDs to Rotemberg weights.
        top_contributors: List of (sector_id, weight, sector_shift) tuples for
            the top contributing sectors.
        n_obs: Number of observations (regions/units).
        n_sectors: Number of sectors used.
        method: Estimation method description.
        diagnostics: Additional diagnostics.
    """

    iv_coef: float
    iv_se: float
    iv_pval: float
    iv_ci_lower: float
    iv_ci_upper: float
    first_stage_f: float
    rotemberg_weights: dict[Any, float]
    top_contributors: list[tuple[Any, float, float]]
    n_obs: int
    n_sectors: int
    method: str = "Shift-Share IV"
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        lines = [
            f"=== {self.method} Results ===",
            f"N(regions) = {self.n_obs}    N(sectors) = {self.n_sectors}",
            "",
            f"IV coefficient: {self.iv_coef:.4f} (SE: {self.iv_se:.4f})",
            f"95% CI: [{self.iv_ci_lower:.4f}, {self.iv_ci_upper:.4f}]",
            f"P-value: {self.iv_pval:.4f}",
            f"First-stage F: {self.first_stage_f:.2f}",
            "",
            "Top Rotemberg weight contributors:",
            f"{'Sector':<20} {'Weight':>10} {'Shift':>10}",
            "-" * 44,
        ]
        for sector, weight, shift in self.top_contributors[:10]:
            lines.append(f"{str(sector):<20} {weight:>10.4f} {shift:>10.4f}")

        neg_weights = sum(1 for w in self.rotemberg_weights.values() if w < 0)
        if neg_weights > 0:
            lines.append(f"\nWarning: {neg_weights} sectors have negative Rotemberg weights.")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dictionary."""
        return {
            "iv_coef": self.iv_coef,
            "iv_se": self.iv_se,
            "iv_pval": self.iv_pval,
            "iv_ci_lower": self.iv_ci_lower,
            "iv_ci_upper": self.iv_ci_upper,
            "first_stage_f": self.first_stage_f,
            "n_obs": self.n_obs,
            "n_sectors": self.n_sectors,
            "top_contributors": [
                {"sector": s, "weight": w, "shift": sh} for s, w, sh in self.top_contributors[:20]
            ],
        }


def _construct_bartik(
    shares: pd.DataFrame,
    shifts: pd.Series,
) -> pd.Series:
    """Construct the Bartik instrument from shares and shifts.

    Args:
        shares: DataFrame with regions as rows, sectors as columns.
            Each cell is the share of sector k in region i (should sum to ~1).
        shifts: Series indexed by sector, containing the aggregate shift
            (e.g., national employment growth in sector k).

    Returns:
        Series indexed by region with the constructed Bartik instrument.
    """
    # Align sectors
    common_sectors = shares.columns.intersection(shifts.index)
    if len(common_sectors) == 0:
        raise ValueError("No common sectors between shares and shifts.")

    shares_aligned = shares[common_sectors]
    shifts_aligned = shifts[common_sectors]

    # Bartik = sum_k (share_ik * shift_k)
    bartik = shares_aligned.values @ shifts_aligned.values
    return pd.Series(bartik, index=shares.index, name="bartik_instrument")


def run_shift_share(
    df: pd.DataFrame,
    y: str,
    shares: pd.DataFrame,
    shifts: pd.Series,
    endogenous: str | None = None,
    controls: list[str] | None = None,
    cluster: str | None = None,
    exposure_robust: bool = True,
) -> ShiftShareResult:
    """Estimate a shift-share IV regression.

    Args:
        df: DataFrame with one row per region. Must be indexed consistently
            with the shares DataFrame.
        y: Name of the outcome variable.
        shares: DataFrame with regions as rows and sectors as columns,
            containing initial shares (e.g., employment shares).
        shifts: Series indexed by sector with the aggregate shift values.
        endogenous: Name of the endogenous variable in df. If None, the
            Bartik instrument is used as the endogenous variable in a
            reduced-form regression.
        controls: List of control variable names (optional).
        cluster: Not used for region-level data (each obs is a region).
            Included for API consistency.
        exposure_robust: If True, compute Borusyak-Hull-Jaravel (2022)
            exposure-robust standard errors.

    Returns:
        ShiftShareResult with IV estimates and diagnostics.
    """
    # Construct the Bartik instrument
    bartik = _construct_bartik(shares, shifts)

    # Align everything
    common_idx = df.index.intersection(bartik.index)
    if len(common_idx) == 0:
        raise ValueError(
            "No common indices between df and shares. "
            "Ensure df and shares have matching row indices (region IDs)."
        )

    y_vals = df.loc[common_idx, y].values.astype(float)
    z_vals = bartik.loc[common_idx].values.astype(float)
    n = len(common_idx)

    # Controls
    if controls:
        X_controls = df.loc[common_idx, controls].values.astype(float)
        X_with_const = np.column_stack([np.ones(n), X_controls])
    else:
        X_with_const = np.ones((n, 1))

    if endogenous is not None:
        d_vals = df.loc[common_idx, endogenous].values.astype(float)

        # First stage: D = gamma * Z + X * delta + epsilon
        Z_first = np.column_stack([z_vals, X_with_const[:, 1:]])  # Z + controls (no const dup)
        Z_first_const = np.column_stack([np.ones(n), Z_first])
        from numpy.linalg import lstsq

        gamma_hat, _, _, _ = lstsq(Z_first_const, d_vals, rcond=None)
        d_hat = Z_first_const @ gamma_hat
        resid_first = d_vals - d_hat

        # First-stage F-stat
        ss_res_first = np.sum(resid_first**2)
        d_mean = d_vals.mean()
        ss_tot_first = np.sum((d_vals - d_mean) ** 2)
        r2_first = 1 - ss_res_first / ss_tot_first
        k_first = Z_first_const.shape[1]
        f_stat = (r2_first / 1) / ((1 - r2_first) / (n - k_first))

        # Second stage (2SLS): Y = beta * D_hat + X * theta + u
        X_second = np.column_stack([d_hat, X_with_const[:, 1:]])
        X_second_const = np.column_stack([np.ones(n), X_second])
        beta_hat, _, _, _ = lstsq(X_second_const, y_vals, rcond=None)
        iv_coef = float(beta_hat[1])  # coefficient on D_hat

        # Residuals using actual D, not D_hat
        X_actual = np.column_stack([np.ones(n), d_vals, X_with_const[:, 1:]])
        _ = X_actual @ beta_hat  # but with actual D replaced
        # Proper 2SLS residuals
        resid = y_vals - (
            beta_hat[0] + iv_coef * d_vals + X_with_const[:, 1:] @ beta_hat[2:]
            if X_with_const.shape[1] > 1
            else beta_hat[0] + iv_coef * d_vals
        )
    else:
        # Reduced form: Y = beta * Z + X * theta + u
        X_rf = np.column_stack([z_vals, X_with_const[:, 1:]])
        X_rf_const = np.column_stack([np.ones(n), X_rf])
        from numpy.linalg import lstsq

        beta_hat, _, _, _ = lstsq(X_rf_const, y_vals, rcond=None)
        iv_coef = float(beta_hat[1])

        resid = y_vals - X_rf_const @ beta_hat
        d_hat = z_vals
        d_vals = z_vals

        # F-stat for reduced form
        ss_res = np.sum(resid**2)
        ss_tot = np.sum((y_vals - y_vals.mean()) ** 2)
        r2 = 1 - ss_res / ss_tot
        k = X_rf_const.shape[1]
        f_stat = (r2 / 1) / ((1 - r2) / (n - k))

    # Standard errors
    if exposure_robust:
        # BHJ (2022) exposure-robust SEs
        # Residualize Z against controls
        if X_with_const.shape[1] > 1:
            from numpy.linalg import lstsq as np_lstsq

            X_no_const = X_with_const[:, 1:]
            X_c = np.column_stack([np.ones(n), X_no_const])
            g_z, _, _, _ = np_lstsq(X_c, z_vals, rcond=None)
            z_resid = z_vals - X_c @ g_z
        else:
            z_resid = z_vals - z_vals.mean()

        # Exposure-weighted residuals at the sector level
        common_sectors = shares.columns.intersection(shifts.index)
        shares_aligned = shares.loc[common_idx, common_sectors].values
        shifts_aligned = shifts[common_sectors].values

        # For each sector k, compute the exposure-weighted average residual
        # s_k = sum_i (share_ik * e_i * z_tilde_i) / sum_i share_ik
        # where e_i is the 2SLS residual and z_tilde is residualized instrument
        sector_scores = np.zeros(len(common_sectors))
        for k in range(len(common_sectors)):
            share_k = shares_aligned[:, k]
            total_share = share_k.sum()
            if total_share > 0:
                sector_scores[k] = np.sum(share_k * resid * z_resid) / total_share

        # Variance = sum_k (shift_k * sector_score_k)^2 / (sum_k shift_k * share_k_bar)^2
        # Simplified: use the influence function approach
        V_num = np.sum((shifts_aligned * sector_scores) ** 2)
        denom = np.sum(z_resid * d_hat)
        if abs(denom) > 1e-10:
            se = float(np.sqrt(V_num) / abs(denom / n))
        else:
            se = np.nan
    else:
        # Heteroskedasticity-robust SE (HC1)
        if endogenous is not None:
            X_second_const_actual = np.column_stack([np.ones(n), z_vals, X_with_const[:, 1:]])
        else:
            X_second_const_actual = np.column_stack([np.ones(n), z_vals, X_with_const[:, 1:]])

        bread = np.linalg.inv(X_second_const_actual.T @ X_second_const_actual)
        meat = np.zeros((X_second_const_actual.shape[1], X_second_const_actual.shape[1]))
        for i in range(n):
            x_i = X_second_const_actual[i]
            meat += resid[i] ** 2 * np.outer(x_i, x_i)
        V = n / (n - X_second_const_actual.shape[1]) * bread @ meat @ bread
        se = float(np.sqrt(V[1, 1]))

    z_stat = iv_coef / se if se > 0 and not np.isnan(se) else np.nan
    pval = float(2 * (1 - stats.norm.cdf(abs(z_stat)))) if not np.isnan(z_stat) else np.nan
    ci_lo = iv_coef - 1.96 * se
    ci_hi = iv_coef + 1.96 * se

    # Rotemberg weights
    common_sectors = shares.columns.intersection(shifts.index)
    shares_aligned = shares.loc[common_idx, common_sectors]
    shifts_aligned = shifts[common_sectors]

    rotemberg = _rotemberg_weights(shares_aligned, shifts_aligned, y_vals, z_vals, X_with_const)

    # Top contributors
    top = sorted(rotemberg.items(), key=lambda x: -abs(x[1]))[:20]
    top_contributors = [
        (sector, weight, float(shifts_aligned[sector]))
        for sector, weight in top
        if sector in shifts_aligned.index
    ]

    return ShiftShareResult(
        iv_coef=iv_coef,
        iv_se=se,
        iv_pval=pval,
        iv_ci_lower=ci_lo,
        iv_ci_upper=ci_hi,
        first_stage_f=float(f_stat),
        rotemberg_weights=rotemberg,
        top_contributors=top_contributors,
        n_obs=n,
        n_sectors=len(common_sectors),
        method="Shift-Share IV" + (" (exposure-robust SE)" if exposure_robust else ""),
        diagnostics={
            "exposure_robust": exposure_robust,
            "negative_weights": sum(1 for w in rotemberg.values() if w < 0),
            "sum_positive_weights": sum(w for w in rotemberg.values() if w > 0),
            "sum_negative_weights": sum(w for w in rotemberg.values() if w < 0),
        },
    )


def _rotemberg_weights(
    shares: pd.DataFrame,
    shifts: pd.Series,
    y: np.ndarray,
    z: np.ndarray,
    X: np.ndarray,
) -> dict[Any, float]:
    """Compute Rotemberg (1983) weights for the shift-share IV.

    The Rotemberg weight for sector k measures the contribution of sector k's
    shift to the overall IV estimate. Sectors with large shares in regions
    that also have large first-stage variation receive higher weights.

    Args:
        shares: (N x K) DataFrame of sector shares.
        shifts: (K,) Series of sector shifts.
        y: (N,) outcome values.
        z: (N,) Bartik instrument values.
        X: (N x p) control matrix (with constant).

    Returns:
        Dictionary mapping sector identifiers to their Rotemberg weights.
    """
    from numpy.linalg import lstsq

    sectors = shares.columns.tolist()
    shares_arr = shares.values
    shifts_arr = shifts.values

    # Residualize z against X
    gamma, _, _, _ = lstsq(X, z, rcond=None)
    z_tilde = z - X @ gamma

    # Denominator: z_tilde' * z
    denom = z_tilde @ z

    weights = {}
    for k, sector in enumerate(sectors):
        share_k = shares_arr[:, k]
        # Numerator: shift_k * (share_k' M_X z) where M_X = I - X(X'X)^-1 X'
        # Since z_tilde = M_X z, this simplifies to shift_k * share_k' z_tilde
        numer = shifts_arr[k] * (share_k @ z_tilde)
        if abs(denom) > 1e-10:
            weights[sector] = float(numer / denom)
        else:
            weights[sector] = 0.0

    return weights


def run_adh_balance(
    shares: pd.DataFrame,
    shifts: pd.Series,
    controls: pd.DataFrame,
) -> pd.DataFrame:
    """Adao, Kolesar, & Morales (2019) balance test.

    Tests whether the shift-share instrument is correlated with
    pre-determined regional characteristics, which would indicate
    potential violations of the identification assumptions.

    For each control variable, regresses it on the Bartik instrument
    constructed from the shares and shifts. Reports the coefficient,
    SE, and p-value.

    Args:
        shares: DataFrame with regions as rows, sectors as columns.
        shifts: Series indexed by sector with aggregate shifts.
        controls: DataFrame with regions as rows and pre-determined
            characteristics as columns.

    Returns:
        DataFrame with one row per control variable, columns:
        ['variable', 'coef', 'se', 'pval', 'significant_05'].
    """
    from numpy.linalg import lstsq

    bartik = _construct_bartik(shares, shifts)
    common_idx = bartik.index.intersection(controls.index)

    z = bartik.loc[common_idx].values.astype(float)
    n = len(common_idx)
    Z = np.column_stack([np.ones(n), z])

    results = []
    for col in controls.columns:
        x = controls.loc[common_idx, col].values.astype(float)
        mask = ~np.isnan(x)
        if mask.sum() < 3:
            results.append(
                {
                    "variable": col,
                    "coef": np.nan,
                    "se": np.nan,
                    "pval": np.nan,
                    "significant_05": False,
                }
            )
            continue

        b, _, _, _ = lstsq(Z[mask], x[mask], rcond=None)
        resid = x[mask] - Z[mask] @ b
        n_valid = int(mask.sum())

        # HC1 robust SE
        bread = np.linalg.inv(Z[mask].T @ Z[mask])
        meat = np.zeros((2, 2))
        for i in range(n_valid):
            z_i = Z[mask][i]
            meat += resid[i] ** 2 * np.outer(z_i, z_i)
        V = n_valid / (n_valid - 2) * bread @ meat @ bread
        se = float(np.sqrt(V[1, 1]))

        coef = float(b[1])
        t_stat = coef / se if se > 0 else np.nan
        pval = (
            float(2 * (1 - stats.t.cdf(abs(t_stat), df=n_valid - 2)))
            if not np.isnan(t_stat)
            else np.nan
        )

        results.append(
            {
                "variable": col,
                "coef": coef,
                "se": se,
                "pval": pval,
                "significant_05": pval < 0.05 if not np.isnan(pval) else False,
            }
        )

    return pd.DataFrame(results)
