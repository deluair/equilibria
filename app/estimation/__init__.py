"""Econometrics estimation module.

Provides a unified interface to standard causal inference estimators
(OLS, IV/2SLS, Panel FE, DID, RDD) and modern causal inference methods
(Double ML, Causal Forest, Synthetic DID, Staggered DID, Randomization
Inference, Shift-Share IV, Bounding/Sensitivity Analysis).
"""

# --- Standard estimators ---
# Bounding and sensitivity analysis
from .bounds import (
    LeeBoundsResult,
    ManskiBoundsResult,
    OsterResult,
    lee_bounds,
    manski_bounds,
    oster_bounds,
)

# Causal / Generalized Random Forest
from .causal_forest import (
    CausalForestResult,
    plot_heterogeneity,
    run_causal_forest,
    variable_importance,
)
from .did import run_did, run_event_study

# --- Modern causal inference ---
# Double/Debiased Machine Learning
from .double_ml import run_double_ml, run_double_ml_sensitivity
from .iv import AndersonRubinResult, anderson_rubin_ci, run_iv
from .ols import run_ols
from .panel_fe import hausman_test, run_panel_fe

# Randomization inference
from .randomization_inference import (
    RandInfResult,
    plot_permutation_distribution,
    randomization_test,
)
from .rdd import mccrary_density_test, placebo_cutoff_test, run_rdd
from .results import EstimationResult, EventStudyResult, RDDResult

# Shift-share IV (Bartik instruments)
from .shift_share import ShiftShareResult, run_adh_balance, run_shift_share

# Staggered DID (Callaway-Sant'Anna, Sun-Abraham, Borusyak-Jaravel-Spiess)
from .staggered_did import (
    GroupTimeATT,
    StaggeredDIDResult,
    run_borusyak_jaravel_spiess,
    run_callaway_santanna,
    run_sun_abraham,
)

# Synthetic Difference-in-Differences
from .synthetic_did import SDIDResult, plot_sdid, run_sdid

__all__ = [
    # Standard estimators
    "run_ols",
    "run_iv",
    "anderson_rubin_ci",
    "AndersonRubinResult",
    "run_panel_fe",
    "run_did",
    "run_event_study",
    "run_rdd",
    # Standard diagnostics
    "hausman_test",
    "placebo_cutoff_test",
    "mccrary_density_test",
    # Result types
    "EstimationResult",
    "EventStudyResult",
    "RDDResult",
    # Double ML
    "run_double_ml",
    "run_double_ml_sensitivity",
    # Causal Forest
    "CausalForestResult",
    "run_causal_forest",
    "plot_heterogeneity",
    "variable_importance",
    # Synthetic DID
    "SDIDResult",
    "run_sdid",
    "plot_sdid",
    # Staggered DID
    "GroupTimeATT",
    "StaggeredDIDResult",
    "run_callaway_santanna",
    "run_sun_abraham",
    "run_borusyak_jaravel_spiess",
    # Randomization inference
    "RandInfResult",
    "randomization_test",
    "plot_permutation_distribution",
    # Shift-share IV
    "ShiftShareResult",
    "run_shift_share",
    "run_adh_balance",
    # Bounds and sensitivity
    "OsterResult",
    "LeeBoundsResult",
    "ManskiBoundsResult",
    "oster_bounds",
    "lee_bounds",
    "manski_bounds",
]
