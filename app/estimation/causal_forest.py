"""Causal/Generalized Random Forest for heterogeneous treatment effects.

Wraps econml's CausalForestDML to estimate conditional average treatment
effects (CATEs) and average treatment effects (ATEs).

References:
    Athey, S., Tibshirani, J., & Wager, S. (2019). Generalized random
    forests. The Annals of Statistics, 47(2), 1148-1178.

    Wager, S., & Athey, S. (2018). Estimation and inference of heterogeneous
    treatment effects using random forests. Journal of the American
    Statistical Association, 113(523), 1228-1242.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

try:
    from econml.dml import CausalForestDML

    _HAS_ECONML = True
except ImportError:
    _HAS_ECONML = False

try:
    import matplotlib.pyplot as plt

    _HAS_MPL = True
except ImportError:
    _HAS_MPL = False


def _check_econml_available() -> None:
    """Raise ImportError with install instructions if econml is missing."""
    if not _HAS_ECONML:
        raise ImportError(
            "econml is required for causal_forest estimation. Install with: uv add econml"
        )


@dataclass
class CausalForestResult:
    """Output from a causal forest estimation.

    Attributes:
        ate: Average treatment effect (point estimate).
        ate_se: Standard error of the ATE.
        ate_ci_lower: Lower bound of 95% CI for ATE.
        ate_ci_upper: Upper bound of 95% CI for ATE.
        cate_predictions: Array of individual-level CATE predictions.
        cate_se: Array of individual-level CATE standard errors.
        var_importance: Dictionary mapping variable names to importance scores.
        n_obs: Number of observations.
        depvar: Name of the outcome variable.
        treatment: Name of the treatment variable.
        controls: List of control variable names.
        diagnostics: Additional model diagnostics.
    """

    ate: float
    ate_se: float
    ate_ci_lower: float
    ate_ci_upper: float
    cate_predictions: np.ndarray
    cate_se: np.ndarray
    var_importance: dict[str, float]
    n_obs: int
    depvar: str = ""
    treatment: str = ""
    controls: list[str] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        lines = [
            "=== Causal Forest Results ===",
            f"Dep. variable: {self.depvar}    Treatment: {self.treatment}",
            f"N = {self.n_obs:,}",
            "",
            f"ATE: {self.ate:.4f} (SE: {self.ate_se:.4f})",
            f"95% CI: [{self.ate_ci_lower:.4f}, {self.ate_ci_upper:.4f}]",
            "",
            f"CATE distribution: "
            f"mean={np.mean(self.cate_predictions):.4f}, "
            f"std={np.std(self.cate_predictions):.4f}, "
            f"min={np.min(self.cate_predictions):.4f}, "
            f"max={np.max(self.cate_predictions):.4f}",
            "",
            "Variable importance (top 10):",
        ]
        sorted_vi = sorted(self.var_importance.items(), key=lambda x: -x[1])
        for name, score in sorted_vi[:10]:
            lines.append(f"  {name:<30} {score:.4f}")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dictionary (without large arrays)."""
        return {
            "ate": self.ate,
            "ate_se": self.ate_se,
            "ate_ci_lower": self.ate_ci_lower,
            "ate_ci_upper": self.ate_ci_upper,
            "n_obs": self.n_obs,
            "depvar": self.depvar,
            "treatment": self.treatment,
            "cate_mean": float(np.mean(self.cate_predictions)),
            "cate_std": float(np.std(self.cate_predictions)),
            "variable_importance": self.var_importance,
            "diagnostics": {k: v for k, v in self.diagnostics.items() if k != "model_object"},
        }


def run_causal_forest(
    df: pd.DataFrame,
    y: str,
    treatment: str,
    controls: list[str],
    n_trees: int = 2000,
    honest: bool = True,
    min_leaf_size: int = 5,
    max_depth: int | None = None,
    cluster: str | None = None,
    random_state: int = 42,
) -> CausalForestResult:
    """Estimate heterogeneous treatment effects using a causal forest.

    Uses econml's CausalForestDML, which combines the causal forest of
    Athey & Wager with the double ML framework of Chernozhukov et al.

    Args:
        df: DataFrame containing all variables.
        y: Name of the outcome variable.
        treatment: Name of the treatment variable.
        controls: List of control variable names.
        n_trees: Number of trees in the forest.
        honest: Whether to use honest splitting (recommended).
        min_leaf_size: Minimum number of observations per leaf.
        max_depth: Maximum depth of each tree (None for unlimited).
        cluster: Column name for cluster-robust inference (optional).
        random_state: Random seed for reproducibility.

    Returns:
        CausalForestResult with ATE, CATEs, and variable importance.

    Raises:
        ImportError: If econml is not installed.
    """
    _check_econml_available()

    all_cols = [y, treatment] + controls
    if cluster is not None:
        all_cols.append(cluster)
    sub = df[all_cols].dropna()

    Y = sub[y].values
    T = sub[treatment].values
    X = sub[controls].values

    # Build the causal forest
    cf = CausalForestDML(
        n_estimators=n_trees,
        min_samples_leaf=min_leaf_size,
        max_depth=max_depth,
        honest=honest,
        random_state=random_state,
        cv=3,  # cross-validation folds for first-stage nuisance models
    )

    # Fit
    fit_kwargs: dict[str, Any] = {}
    if cluster is not None:
        fit_kwargs["groups"] = sub[cluster].values
    cf.fit(Y, T, X=X, W=X, **fit_kwargs)

    # ATE and inference
    ate_inference = cf.ate_inference(X=X)
    ate_val = float(ate_inference.mean_point)
    ate_se_val = float(ate_inference.stderr_mean)
    ate_ci = ate_inference.conf_int_mean(alpha=0.05)
    ate_ci_lo = float(ate_ci[0])
    ate_ci_hi = float(ate_ci[1])

    # Individual CATE predictions
    cate_preds = cf.effect(X=X).flatten()
    cate_inf = cf.effect_inference(X=X)
    cate_ses = cate_inf.stderr.flatten()

    # Variable importance via feature importances on the causal tree ensemble
    raw_importance = cf.feature_importances_
    vi_dict = {controls[i]: float(raw_importance[i]) for i in range(len(controls))}

    return CausalForestResult(
        ate=ate_val,
        ate_se=ate_se_val,
        ate_ci_lower=ate_ci_lo,
        ate_ci_upper=ate_ci_hi,
        cate_predictions=cate_preds,
        cate_se=cate_ses,
        var_importance=vi_dict,
        n_obs=len(sub),
        depvar=y,
        treatment=treatment,
        controls=controls,
        diagnostics={
            "n_trees": n_trees,
            "honest": honest,
            "min_leaf_size": min_leaf_size,
            "max_depth": max_depth,
            "model_object": cf,
        },
    )


def variable_importance(result: CausalForestResult) -> pd.DataFrame:
    """Return variable importance as a sorted DataFrame.

    Args:
        result: Output from run_causal_forest.

    Returns:
        DataFrame with columns 'variable' and 'importance', sorted descending.
    """
    vi = pd.DataFrame(
        list(result.var_importance.items()),
        columns=["variable", "importance"],
    )
    return vi.sort_values("importance", ascending=False).reset_index(drop=True)


def plot_heterogeneity(
    result: CausalForestResult,
    variable: str,
    df: pd.DataFrame | None = None,
    n_bins: int = 20,
    ax=None,
) -> Any:
    """Plot CATE heterogeneity along a single variable.

    Creates a binned scatter plot showing how the conditional average
    treatment effect varies with a specified control variable.

    Args:
        result: Output from run_causal_forest.
        variable: Name of the variable to plot on the x-axis.
            Must be one of the control variables used in estimation.
        df: The original DataFrame (needed to retrieve the variable values).
            If None, a simple index-based plot of CATEs is shown.
        n_bins: Number of bins for the scatter plot.
        ax: Optional matplotlib axes to plot on.

    Returns:
        Matplotlib axes object.

    Raises:
        ImportError: If matplotlib is not installed.
    """
    if not _HAS_MPL:
        raise ImportError("matplotlib is required for plotting. Install with: uv add matplotlib")

    if ax is None:
        fig, ax = plt.subplots(figsize=(8, 5))

    cate = result.cate_predictions

    if df is not None and variable in df.columns:
        # Get the variable values aligned with the estimation sample
        all_cols = [result.depvar, result.treatment] + result.controls
        sub = df[all_cols].dropna()
        x_vals = sub[variable].values

        # Bin the x variable and compute mean CATE per bin
        bin_edges = np.percentile(x_vals, np.linspace(0, 100, n_bins + 1))
        bin_edges = np.unique(bin_edges)
        bin_idx = np.digitize(x_vals, bin_edges[1:-1])

        bin_means_x = []
        bin_means_cate = []
        bin_se_cate = []
        for b in range(len(bin_edges) - 1):
            mask = bin_idx == b
            if mask.sum() > 0:
                bin_means_x.append(np.mean(x_vals[mask]))
                bin_means_cate.append(np.mean(cate[mask]))
                bin_se_cate.append(np.std(cate[mask]) / np.sqrt(mask.sum()))

        bin_means_x = np.array(bin_means_x)
        bin_means_cate = np.array(bin_means_cate)
        bin_se_cate = np.array(bin_se_cate)

        ax.errorbar(
            bin_means_x,
            bin_means_cate,
            yerr=1.96 * bin_se_cate,
            fmt="o",
            capsize=3,
            color="steelblue",
            markersize=5,
        )
        ax.set_xlabel(variable)
    else:
        ax.scatter(range(len(cate)), cate, alpha=0.3, s=10, color="steelblue")
        ax.set_xlabel("Observation index")

    ax.axhline(y=result.ate, color="red", linestyle="--", label=f"ATE = {result.ate:.4f}")
    ax.set_ylabel("CATE")
    ax.set_title(f"Treatment effect heterogeneity by {variable}")
    ax.legend()

    return ax
