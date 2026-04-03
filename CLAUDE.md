# Equilibria

Open-source applied economics analysis platform. 18 analytical layers, 257 modules, 12 estimators, AI-powered briefings. Apache 2.0.

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
    trade/             # 32 modules (L1)
    macro/             # 30 modules (L2)
    labor/             # 20 modules (L3)
    development/       # 20 modules (L4)
    agricultural/      # 22 modules (L5)
    integration/       # 10 modules (L6)
    financial/         # 12 modules (L7)
    health/            # 10 modules (L8)
    environmental/     # 12 modules (L9)
    public/            # 10 modules (L10)
    spatial/           # 10 modules (L11)
    political/         # 10 modules (L12)
    behavioral/        # 10 modules (L13)
    industrial/        # 10 modules (L14)
    monetary/          # 10 modules (L15)
    energy/            # 10 modules (L16)
    demographic/       # 6 modules  (L17)
    methods/           # 12 modules (L18)
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

## 18 Analytical Layers (257 modules)

### L1 Trade (32 modules)
gravity, trade_elasticity, rca, terms_of_trade, trade_openness, concentration,
bilateral_decomposition, complementarity, grubel_lloyd, cbam_impact, tariff_passthrough,
product_space, trade_creation, export_survival, gvc_participation, market_diversification,
rta_evaluation, sanctions_impact, border_effect, trade_weighted_fx, currency_union, trade_cost,
trade_in_services, digital_trade, wto_disputes, trade_finance, rules_of_origin, ntm_analysis,
trade_diversion, export_complexity, import_substitution, trade_policy_uncertainty

### L2 Macro (30 modules)
gdp_decomposition, phillips_curve, taylor_rule, yield_curve, output_gap, okuns_law,
fiscal_multiplier, debt_sustainability, twin_deficits, erpt, ppp, business_cycle,
inflation_decomposition, monetary_transmission, structural_break, recession_probability,
nowcasting, var_irf, fci, credit_impulse, sovereign_spread, capital_flows, exchange_rate_regime,
external_balance, liquidity_trap, secular_stagnation, hysteresis, potential_output,
inflation_expectations, macro_uncertainty

### L3 Labor (20 modules)
mincer, oaxaca_blinder, returns_education, migration_gravity, remittance,
unemployment_duration, beveridge_curve, shift_share, labor_force, skill_premium,
wage_phillips, union_premium, minimum_wage, automation_exposure, labor_tightness,
sectoral_reallocation, gig_economy, labor_market_polarization, job_quality, labor_informality

### L4 Development (20 modules)
beta_convergence, sigma_convergence, poverty_trap, solow_residual, kuznets_curve,
hdi_decomposition, mpi, structural_transformation, inequality_decomposition,
demographic_dividend, finance_growth, institutional_quality, resource_curse,
aid_effectiveness, governance_composite, social_mobility, middle_income_trap,
capability_approach, development_accounting, leapfrogging

### L5 Agricultural (22 modules)
supply_elasticity, demand_system, food_security, food_price_volatility, price_transmission,
climate_yield, fertilizer_response, irrigation_returns, farm_size, land_use,
deforestation_trade, caloric_trade, wef_nexus, adaptation_cba, supply_chain_disruption,
market_integration, ag_competitiveness, agricultural_distortions, crop_diversification,
livestock_economics, aquaculture_trade, agri_value_chain

### L6 Integration (10 modules)
composite_score, attribution, spillover, cross_correlation, structural_break_cross,
signal_classifier, scenario_simulation, crisis_comparison, country_profile,
briefing_orchestrator

### L7 Financial (12 modules)
banking_stability, credit_cycle, asset_price_bubble, systemic_risk, capital_adequacy,
nonperforming_loans, financial_inclusion, microfinance, shadow_banking, fintech_adoption,
financial_development, interest_rate_pass_through

### L8 Health (10 modules)
health_expenditure, catastrophic_health, universal_coverage, disease_burden, mortality_decomposition,
nutrition_economics, health_human_capital, pharmaceutical_access, mental_health_economics,
pandemic_economics

### L9 Environmental (12 modules)
carbon_intensity, emissions_decomposition, green_growth, environmental_kuznets,
pollution_health, biodiversity_economics, natural_capital, climate_vulnerability,
carbon_pricing, just_transition, environmental_justice, ecological_footprint

### L10 Public (10 modules)
tax_buoyancy, revenue_productivity, public_investment, social_protection, subsidy_incidence,
corruption_economics, decentralization, public_goods, bureaucratic_quality, fiscal_federalism

### L11 Spatial (10 modules)
regional_convergence, agglomeration, urban_rural_gap, geographic_concentration,
market_access, spatial_autocorrelation, migration_selection, commuting_flows,
housing_affordability, land_rent

### L12 Political (10 modules)
political_business_cycle, democracy_growth, electoral_economics, conflict_economics,
state_fragility, veto_players, redistribution_politics, populism_index, coalition_stability,
political_polarization

### L13 Behavioral (10 modules)
loss_aversion, present_bias, social_preferences, nudge_effectiveness, financial_literacy,
trust_economics, norms_enforcement, bounded_rationality, reference_dependence, anchoring_bias

### L14 Industrial (10 modules)
industrial_policy, market_concentration, markup_estimation, firm_dynamics, creative_destruction,
rnd_spillovers, fdi_productivity, sez_evaluation, cluster_effects, industry_evolution

### L15 Monetary (10 modules)
money_demand, quantity_theory, credit_channel, bank_lending, interest_parity,
inflation_targeting, exchange_rate_pass_through, sterilization, reserve_adequacy, dollarization

### L16 Energy (10 modules)
energy_intensity, fossil_subsidy, renewable_transition, energy_security, oil_price_macro,
electricity_access, energy_poverty, stranded_assets, carbon_border, energy_efficiency

### L17 Demographic (6 modules)
aging, child_development, fertility, gender_economics, human_capital, population_growth

### L18 Methods (12 modules)
synthetic_control, regression_discontinuity, difference_in_differences, instrumental_variables,
panel_data, time_series, causal_inference, meta_analysis, bayesian_estimation,
machine_learning_econ, natural_experiments, power_analysis

## Estimation Engine (ported from EconAI)
12 estimators: OLS, IV/2SLS, Panel FE, DiD, RDD, Double ML, Causal Forest, Synthetic DiD,
Staggered DiD, Shift-Share, Bounds, Randomization Inference

## Data Collectors (13)
FRED, WDI, ILO, FAOSTAT, BLS, IMF WEO, Penn World Table, Comtrade, USDA, NOAA, V-Dem, PovcalNet, WHO.
All follow BaseCollector pattern (collect -> validate -> store pipeline, httpx with retry).

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

## Tests
1,611 tests across 178 test files. pytest-asyncio (asyncio_mode=auto). Run with `make test`.
Coverage: layers, estimation, collectors, API endpoints, briefings, AI tools.

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
