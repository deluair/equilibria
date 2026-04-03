"""Double/Debiased Machine Learning (Chernozhukov et al., 2018).

Wraps the DoubleML package to estimate causal effects of a treatment variable
while using flexible ML methods to control for high-dimensional confounders.

References:
    Chernozhukov, V., Chetverikov, D., Demirer, M., Duflo, E., Hansen, C.,
    Newey, W., & Robins, J. (2018). Double/debiased machine learning for
    treatment and structural parameters. The Econometrics Journal, 21(1).
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .results import EstimationResult

try:
    import doubleml as dml
    from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
    from sklearn.linear_model import LassoCV

    _HAS_DML = True
except ImportError:
    _HAS_DML = False


def _check_dml_available() -> None:
    """Raise ImportError with install instructions if DoubleML is missing."""
    if not _HAS_DML:
        raise ImportError(
            "DoubleML is required for double_ml estimation. "
            "Install with: uv add doubleml scikit-learn"
        )


def _get_ml_learner(ml_method: str, is_classifier: bool = False):
    """Return an sklearn learner based on the method name.

    Args:
        ml_method: One of 'random_forest', 'lasso', 'gradient_boosting'.
        is_classifier: If True, return a classifier variant (for IRM nuisance).

    Returns:
        An sklearn estimator instance.
    """
    if ml_method == "random_forest":
        if is_classifier:
            from sklearn.ensemble import RandomForestClassifier

            return RandomForestClassifier(n_estimators=500, max_depth=5, random_state=42)
        return RandomForestRegressor(n_estimators=500, max_depth=5, random_state=42)
    elif ml_method == "lasso":
        if is_classifier:
            # Fallback: LogisticCV is not always available, use LogisticRegressionCV
            from sklearn.linear_model import LogisticRegressionCV

            return LogisticRegressionCV(cv=5, penalty="l1", solver="saga", max_iter=2000)
        return LassoCV(cv=5)
    elif ml_method == "gradient_boosting":
        if is_classifier:
            from sklearn.ensemble import GradientBoostingClassifier

            return GradientBoostingClassifier(
                n_estimators=500, max_depth=3, learning_rate=0.05, random_state=42
            )
        return GradientBoostingRegressor(
            n_estimators=500, max_depth=3, learning_rate=0.05, random_state=42
        )
    else:
        raise ValueError(
            f"Unknown ml_method '{ml_method}'. Choose from: random_forest, lasso, gradient_boosting"
        )


def run_double_ml(
    df: pd.DataFrame,
    y: str,
    treatment: str,
    controls: list[str],
    model: str = "partially_linear",
    ml_method: str = "random_forest",
    n_folds: int = 5,
    n_rep: int = 1,
    cluster: str | None = None,
) -> EstimationResult:
    """Estimate a causal effect using Double/Debiased Machine Learning.

    Args:
        df: DataFrame containing all variables.
        y: Name of the outcome variable.
        treatment: Name of the treatment variable.
        controls: List of control variable names (confounders).
        model: 'partially_linear' (PLR) or 'interactive' (IRM).
        ml_method: ML method for nuisance estimation. One of
            'random_forest', 'lasso', 'gradient_boosting'.
        n_folds: Number of cross-fitting folds.
        n_rep: Number of repeated cross-fitting iterations.
        cluster: Column name for cluster-level cross-fitting (optional).

    Returns:
        EstimationResult with the ATE (or partial effect) and inference.

    Raises:
        ImportError: If doubleml or sklearn is not installed.
        ValueError: If model or ml_method is unknown.
    """
    _check_dml_available()

    # Build DoubleML data backend
    all_cols = [y, treatment] + controls
    sub = df[all_cols].dropna()

    dml_data_kwargs: dict[str, Any] = {
        "data": sub,
        "y_col": y,
        "d_cols": treatment,
        "x_cols": controls,
    }
    if cluster is not None:
        dml_data_kwargs["data"] = df[[*all_cols, cluster]].dropna()
        dml_data_kwargs["cluster_cols"] = cluster

    dml_data = dml.DoubleMLData(**dml_data_kwargs)

    # Select ML learners
    ml_l = _get_ml_learner(ml_method, is_classifier=False)  # E[Y|X]
    ml_m = _get_ml_learner(ml_method, is_classifier=(model == "interactive"))  # E[D|X] or Pr(D=1|X)

    # Fit the model
    if model == "partially_linear":
        dml_model = dml.DoubleMLPLR(
            dml_data,
            ml_l=ml_l,
            ml_m=ml_m,
            n_folds=n_folds,
            n_rep=n_rep,
        )
    elif model == "interactive":
        ml_g = _get_ml_learner(ml_method, is_classifier=False)  # E[Y|D,X]
        dml_model = dml.DoubleMLIRM(
            dml_data,
            ml_g=ml_g,
            ml_m=ml_m,
            n_folds=n_folds,
            n_rep=n_rep,
        )
    else:
        raise ValueError(f"Unknown model '{model}'. Choose from: partially_linear, interactive")

    dml_model.fit()

    # Extract results
    summary = dml_model.summary
    coef_val = float(summary["coef"].iloc[0])
    se_val = float(summary["std err"].iloc[0])
    pval_val = float(summary["P>|t|"].iloc[0])
    ci = dml_model.confint(level=0.95)
    ci_lo = float(ci.iloc[0, 0])
    ci_hi = float(ci.iloc[0, 1])

    model_label = "DML-PLR" if model == "partially_linear" else "DML-IRM"

    return EstimationResult(
        coef={treatment: coef_val},
        se={treatment: se_val},
        pval={treatment: pval_val},
        ci_lower={treatment: ci_lo},
        ci_upper={treatment: ci_hi},
        n_obs=int(dml_data.n_obs),
        r_sq=np.nan,  # Not meaningful for DML
        adj_r_sq=None,
        method=model_label,
        depvar=y,
        diagnostics={
            "ml_method": ml_method,
            "n_folds": n_folds,
            "n_rep": n_rep,
            "model_type": model,
            "dml_object": dml_model,
        },
    )


def run_double_ml_sensitivity(
    result: EstimationResult,
    cf_y_values: list[float] | None = None,
    cf_d_values: list[float] | None = None,
) -> dict[str, Any]:
    """Run sensitivity analysis on a DML result (Chernozhukov et al., 2022).

    Assesses robustness of the estimated treatment effect to violations of
    the unconfoundedness assumption. Uses the DoubleML sensitivity framework
    based on the nonparametric R-squared measures cf_y (confounding in outcome)
    and cf_d (confounding in treatment).

    Args:
        result: An EstimationResult from run_double_ml. Must contain the
            DoubleML model object in diagnostics['dml_object'].
        cf_y_values: Values of cf_y to evaluate. Defaults to [0.03, 0.1, 0.2].
        cf_d_values: Values of cf_d to evaluate. Defaults to [0.03, 0.1, 0.2].

    Returns:
        Dictionary with keys:
            'theta_lower': lower bound of identified set for each (cf_y, cf_d).
            'theta_upper': upper bound of identified set.
            'summary': textual summary of the sensitivity analysis.
    """
    _check_dml_available()

    dml_model = result.diagnostics.get("dml_object")
    if dml_model is None:
        raise ValueError(
            "No DML model object found in result.diagnostics. Run run_double_ml first."
        )

    if cf_y_values is None:
        cf_y_values = [0.03, 0.1, 0.2]
    if cf_d_values is None:
        cf_d_values = [0.03, 0.1, 0.2]

    sensitivity_results = {}
    lines = ["=== DML Sensitivity Analysis ===", ""]

    for cf_y in cf_y_values:
        for cf_d in cf_d_values:
            dml_model.sensitivity_analysis(cf_y=cf_y, cf_d=cf_d)
            sens = dml_model.sensitivity_params
            theta_lo = float(sens["ci_lower"].iloc[0])
            theta_hi = float(sens["ci_upper"].iloc[0])
            sensitivity_results[(cf_y, cf_d)] = {
                "theta_lower": theta_lo,
                "theta_upper": theta_hi,
            }
            lines.append(f"cf_y={cf_y:.2f}, cf_d={cf_d:.2f}: [{theta_lo:.4f}, {theta_hi:.4f}]")

    return {
        "bounds": sensitivity_results,
        "summary": "\n".join(lines),
    }
