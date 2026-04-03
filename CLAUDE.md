# Equilibria

Open-source applied economics analysis platform. 6 analytical layers, 103 modules, 12 estimators, AI-powered briefings. Apache 2.0.

## Stack
- Backend: FastAPI + aiosqlite + Python 3.11, uv
- Frontend: Next.js 16 + React 19 + Tailwind 4 + Recharts (planned)
- DB: SQLite WAL mode, single equilibria.db (pool_size=5)
- AI: Claude Sonnet 4 brain with 22 structured tools, 10-round agentic loop
- Deploy: OVH VPS, systemd, port 8003

## Commands
```bash
make setup    # Install deps, create .env, init DB
make serve    # Dev: API on 8003
make test     # Run pytest
make lint     # ruff check + format
make collect  # Run all collectors
make deploy   # Deploy to VPS via deploy.sh
```

## Structure
```
app/
  main.py              # FastAPI app, lifespan, middleware, router loader
  config.py            # Settings (pydantic-settings), layer weights, signal levels
  db.py                # aiosqlite pool, schema (6 tables, 8 indexes)
  ai/
    brain.py           # Claude Sonnet 4 agentic loop (10 rounds)
    tools.py           # 22 tools in TOOL_REGISTRY
    citations.py       # Citation formatting
  api/
    health.py          # /api/health
    trade.py           # L1 Trade endpoints
    macro.py           # L2 Macro endpoints
    labor.py           # L3 Labor endpoints
    development.py     # L4 Development endpoints
    agricultural.py    # L5 Agricultural endpoints
    integration.py     # L6 Integration endpoints
    briefings.py       # Briefing generation endpoints
    chat.py            # AI chat endpoint
  layers/
    base.py            # LayerBase ABC (compute, run, classify_signal)
    trade/             # 22 modules
    macro/             # 20 modules
    labor/             # 16 modules
    development/       # 16 modules
    agricultural/      # 18 modules
    integration/       # 10 modules
  estimation/          # 12 estimators (ported from EconAI)
    ols.py, iv.py, panel_fe.py, did.py, rdd.py,
    double_ml.py, causal_forest.py, synthetic_did.py,
    staggered_did.py, shift_share.py, bounds.py,
    randomization_inference.py
    results.py         # EstimationResult dataclass
  figures/             # Plotly figure generators
    binscatter.py, coefficient.py, distribution.py,
    event_study.py, style.py
  tables/              # LaTeX/HTML table generators
    regression.py, summary_stats.py, balance.py
  collectors/
    base.py            # BaseCollector ABC (collect, validate, store, run)
    fred.py            # FRED API collector
    wdi.py             # World Bank WDI collector
  briefings/
    base.py            # BriefingBase class
    economic_conditions.py
    trade_flash.py
    country_deep_dive.py
data/                  # gitignored, equilibria.db lives here
docs/
Makefile
pyproject.toml
deploy.sh
```

## 6 Analytical Layers (103 modules)

### L1 Trade (22 modules)
gravity, trade_elasticity, rca, terms_of_trade, trade_openness, concentration,
bilateral_decomposition, complementarity, grubel_lloyd, cbam_impact, tariff_passthrough,
product_space, trade_creation, export_survival, gvc_participation, market_diversification,
rta_evaluation, sanctions_impact, border_effect, trade_weighted_fx, currency_union, trade_cost

### L2 Macro (20 modules)
gdp_decomposition, phillips_curve, taylor_rule, yield_curve, output_gap, okuns_law,
fiscal_multiplier, debt_sustainability, twin_deficits, erpt, ppp, business_cycle,
inflation_decomposition, monetary_transmission, structural_break, recession_probability,
nowcasting, var_irf, fci, credit_impulse

### L3 Labor (16 modules)
mincer, oaxaca_blinder, returns_education, migration_gravity, remittance,
unemployment_duration, beveridge_curve, shift_share, labor_force, skill_premium,
wage_phillips, union_premium, minimum_wage, automation_exposure, labor_tightness,
sectoral_reallocation

### L4 Development (16 modules)
beta_convergence, sigma_convergence, poverty_trap, solow_residual, kuznets_curve,
hdi_decomposition, mpi, structural_transformation, inequality_decomposition,
demographic_dividend, finance_growth, institutional_quality, resource_curse,
aid_effectiveness, governance_composite, social_mobility

### L5 Agricultural (18 modules)
supply_elasticity, demand_system, food_security, food_price_volatility, price_transmission,
climate_yield, fertilizer_response, irrigation_returns, farm_size, land_use,
deforestation_trade, caloric_trade, wef_nexus, adaptation_cba, supply_chain_disruption,
market_integration, ag_competitiveness, agricultural_distortions

### L6 Integration (10 modules)
composite_score, attribution, spillover, cross_correlation, structural_break_cross,
signal_classifier, scenario_simulation, crisis_comparison, country_profile,
briefing_orchestrator

## Estimation Engine (ported from EconAI)
12 estimators: OLS, IV/2SLS, Panel FE, DiD, RDD, Double ML, Causal Forest, Synthetic DiD,
Staggered DiD, Shift-Share, Bounds, Randomization Inference

## Data Collectors
13 collectors: FRED, WDI, ILO, FAOSTAT, BLS, IMF WEO, Penn World Table, Comtrade, USDA, NOAA, V-Dem, PovcalNet. All follow BaseCollector pattern (collect -> validate -> store pipeline, httpx with retry).

## AI Brain
Claude Sonnet 4 (`claude-sonnet-4-6`) with 22 tools, 10-round agentic loop.
Tools: get_system_status, estimate_gravity, compute_rca, bilateral_decomposition,
tariff_simulation, gdp_decompose, estimate_phillips, fiscal_sustainability, cycle_dating,
wage_decomposition, returns_to_education, shift_share, convergence_test, poverty_analysis,
institutional_iv, demand_system, food_security_index, price_transmission, run_estimation,
compare_countries, query_data, generate_figure.

## Briefings
3 implemented: Economic Conditions, Trade Flash, Country Deep Dive.
All inherit from BriefingBase. Stored in briefings table with layer_scores and composite_score.

## DB Schema (equilibria.db)
6 tables: countries, data_series, data_points, analysis_results, briefings, collection_log.
WAL mode, foreign keys ON, busy_timeout 5000ms.

## Code Conventions
- Python 3.11+, async everywhere
- LayerBase subclass pattern for all layer modules (must implement `async compute(db, **kwargs) -> dict`)
- BaseCollector pattern for all collectors (must implement `async collect() -> dict | list[dict]`)
- Signal classification: STABLE (0-25), WATCH (25-50), STRESS (50-75), CRISIS (75-100)
- Layer weights: 0.20 each for L1-L5, L6 is integration/orchestration
- ruff linting (E, F, I, W), line length 100
- pytest-asyncio (asyncio_mode=auto)
- No mock data, no hallucination

## Environment Variables
```
FRED_API_KEY       # FRED economic data
ANTHROPIC_API_KEY  # Claude AI brain
COMTRADE_API_KEY   # UN Comtrade trade data
EIA_API_KEY        # Energy data
BLS_API_KEY        # Bureau of Labor Statistics
NOAA_TOKEN         # Climate/weather data
API_KEY            # Optional API auth
```

## Data Integrity
- No mock data. Every number from a real, verifiable source.
- No hallucinated AI content. Every claim grounded in tool results.
- Every displayed number traceable to source via _citation field.
- Signal levels derived from actual composite scores, not hardcoded.

## Deploy
- VPS: ubuntu@40.160.2.223, port 8003, systemd service (equilibria)
- rsync excludes: .venv, __pycache__, .git, data/, .claude, node_modules, .next
- Health check: GET /api/health
- Custom headers: X-Crafted-By, X-Origin on all responses
