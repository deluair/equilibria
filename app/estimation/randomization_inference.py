"""Randomization inference (Fisher, 1935).

Conducts exact or approximate randomization tests by permuting treatment
assignment and comparing the observed test statistic to the permutation
distribution. Useful when the number of clusters or units is small and
asymptotic inference may be unreliable.

References:
    Fisher, R. A. (1935). The Design of Experiments.

    Imbens, G. W., & Rubin, D. B. (2015). Causal Inference for Statistics,
    Social, and Biomedical Sciences. Cambridge University Press.

    Young, A. (2019). Channeling Fisher: Randomization tests and the
    statistical insignificance of seemingly significant experimental results.
    Quarterly Journal of Economics, 134(2), 557-598.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

try:
    import matplotlib.pyplot as plt

    _HAS_MPL = True
except ImportError:
    _HAS_MPL = False


@dataclass
class RandInfResult:
    """Output from a randomization inference test.

    Attributes:
        observed_stat: The test statistic computed on the actual data.
        pvalue: Two-sided Fisher exact p-value.
        pvalue_one_sided: One-sided p-value (proportion >= observed).
        null_distribution: Array of test statistics under permuted assignments.
        n_permutations: Number of permutations performed.
        statistic_name: Name of the test statistic used.
        n_obs: Number of observations.
        n_treated: Number of treated observations.
        diagnostics: Additional diagnostics.
    """

    observed_stat: float
    pvalue: float
    pvalue_one_sided: float
    null_distribution: np.ndarray
    n_permutations: int
    statistic_name: str = "diff_means"
    n_obs: int = 0
    n_treated: int = 0
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        lines = [
            "=== Randomization Inference ===",
            f"Statistic: {self.statistic_name}",
            f"N = {self.n_obs}    N(treated) = {self.n_treated}",
            f"Permutations: {self.n_permutations:,}",
            "",
            f"Observed statistic: {self.observed_stat:.4f}",
            f"Two-sided p-value:  {self.pvalue:.4f}",
            f"One-sided p-value:  {self.pvalue_one_sided:.4f}",
            "",
            f"Null distribution: "
            f"mean={np.mean(self.null_distribution):.4f}, "
            f"std={np.std(self.null_distribution):.4f}",
        ]
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dictionary (without the large null array)."""
        return {
            "observed_stat": self.observed_stat,
            "pvalue": self.pvalue,
            "pvalue_one_sided": self.pvalue_one_sided,
            "n_permutations": self.n_permutations,
            "statistic_name": self.statistic_name,
            "n_obs": self.n_obs,
            "n_treated": self.n_treated,
            "null_mean": float(np.mean(self.null_distribution)),
            "null_std": float(np.std(self.null_distribution)),
        }


def _diff_means(y: np.ndarray, treat: np.ndarray) -> float:
    """Difference in means test statistic."""
    return float(np.mean(y[treat == 1]) - np.mean(y[treat == 0]))


def _ks_statistic(y: np.ndarray, treat: np.ndarray) -> float:
    """Kolmogorov-Smirnov test statistic."""
    from scipy.stats import ks_2samp

    stat, _ = ks_2samp(y[treat == 1], y[treat == 0])
    return float(stat)


def _rank_sum(y: np.ndarray, treat: np.ndarray) -> float:
    """Wilcoxon rank-sum test statistic."""
    from scipy.stats import rankdata

    ranks = rankdata(y)
    return float(np.sum(ranks[treat == 1]))


_STAT_FUNCTIONS: dict[str, Callable] = {
    "diff_means": _diff_means,
    "ks": _ks_statistic,
    "rank_sum": _rank_sum,
}


def randomization_test(
    df: pd.DataFrame,
    y: str,
    treat_col: str,
    statistic: str | Callable = "diff_means",
    n_permutations: int = 5000,
    cluster: str | None = None,
    seed: int = 42,
) -> RandInfResult:
    """Conduct a randomization (permutation) test.

    Permutes treatment assignment and recomputes the test statistic to
    build the null distribution. The p-value is the fraction of permuted
    statistics at least as extreme as the observed statistic.

    Args:
        df: DataFrame containing the outcome and treatment columns.
        y: Name of the outcome variable.
        treat_col: Name of the binary treatment column (0/1).
        statistic: Test statistic to use. Either a string ('diff_means',
            'ks', 'rank_sum') or a callable(y, treat) -> float.
        n_permutations: Number of permutations.
        cluster: Column for cluster-level permutation. If provided,
            treatment is permuted at the cluster level.
        seed: Random seed for reproducibility.

    Returns:
        RandInfResult with observed statistic, p-value, and null distribution.
    """
    rng = np.random.default_rng(seed)

    sub = df[[y, treat_col]].copy()
    if cluster is not None:
        sub[cluster] = df[cluster].values
    sub = sub.dropna()

    y_vals = sub[y].values.astype(float)
    treat_vals = sub[treat_col].values.astype(int)
    n = len(sub)
    n_treated = int(treat_vals.sum())

    # Resolve the statistic function
    if isinstance(statistic, str):
        if statistic not in _STAT_FUNCTIONS:
            raise ValueError(
                f"Unknown statistic '{statistic}'. Choose from: {list(_STAT_FUNCTIONS.keys())}"
            )
        stat_func = _STAT_FUNCTIONS[statistic]
        stat_name = statistic
    else:
        stat_func = statistic
        stat_name = getattr(statistic, "__name__", "custom")

    # Observed test statistic
    observed = stat_func(y_vals, treat_vals)

    # Permutation loop
    null_dist = np.zeros(n_permutations)

    if cluster is not None:
        # Cluster-level permutation
        cluster_vals = sub[cluster].values
        unique_clusters = np.unique(cluster_vals)
        cluster_treat = {cl: int(treat_vals[cluster_vals == cl][0]) for cl in unique_clusters}
        cluster_treat_arr = np.array([cluster_treat[cl] for cl in unique_clusters])
        n_treated_clusters = int(cluster_treat_arr.sum())

        for p in range(n_permutations):
            perm_cl = rng.permutation(len(unique_clusters))
            fake_treated_clusters = set(unique_clusters[perm_cl[:n_treated_clusters]])
            fake_treat = np.array([1 if cl in fake_treated_clusters else 0 for cl in cluster_vals])
            null_dist[p] = stat_func(y_vals, fake_treat)
    else:
        # Individual-level permutation
        for p in range(n_permutations):
            perm = rng.permutation(n)
            fake_treat = treat_vals[perm]
            null_dist[p] = stat_func(y_vals, fake_treat)

    # P-values
    # Two-sided: fraction of |null| >= |observed|
    pvalue_two = float(np.mean(np.abs(null_dist) >= np.abs(observed)))
    # One-sided: fraction of null >= observed
    pvalue_one = float(np.mean(null_dist >= observed))

    return RandInfResult(
        observed_stat=observed,
        pvalue=pvalue_two,
        pvalue_one_sided=pvalue_one,
        null_distribution=null_dist,
        n_permutations=n_permutations,
        statistic_name=stat_name,
        n_obs=n,
        n_treated=n_treated,
        diagnostics={
            "cluster": cluster,
        },
    )


def plot_permutation_distribution(
    result: RandInfResult,
    title: str | None = None,
    ax=None,
) -> Any:
    """Plot the permutation null distribution with the observed statistic.

    Args:
        result: Output from randomization_test.
        title: Plot title. Defaults to a descriptive title.
        ax: Optional matplotlib axes.

    Returns:
        Matplotlib axes object.

    Raises:
        ImportError: If matplotlib is not installed.
    """
    if not _HAS_MPL:
        raise ImportError("matplotlib is required for plotting. Install with: uv add matplotlib")

    if ax is None:
        fig, ax = plt.subplots(figsize=(8, 5))

    ax.hist(
        result.null_distribution,
        bins=50,
        density=True,
        alpha=0.7,
        color="steelblue",
        edgecolor="white",
        label="Null distribution",
    )
    ax.axvline(
        x=result.observed_stat,
        color="red",
        linewidth=2,
        linestyle="--",
        label=f"Observed = {result.observed_stat:.4f}",
    )

    if title is None:
        title = f"Randomization inference ({result.statistic_name}, p = {result.pvalue:.3f})"
    ax.set_title(title)
    ax.set_xlabel("Test statistic")
    ax.set_ylabel("Density")
    ax.legend()

    return ax
