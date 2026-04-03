# Equilibria: Applied Economics Analysis Platform

**Date**: 2026-04-02
**Status**: Approved
**Author**: Md Deluair Hossen, PhD

## Purpose

Open-source applied economics analysis platform. Portfolio showcase demonstrating serious econometric and data analysis capability across the full spectrum of economics. Mirrors Vigil's architecture (macro-financial risk) but for applied economics.

**Primary audience**: Hiring managers at policy shops, consulting firms, think tanks, and industry roles.

**Positioning**: Vigil proves financial risk engineering. Equilibria proves applied economics. Together they cover the full analytical range.

## Architecture

Vigil clone pattern: FastAPI + Next.js + SQLite, 6 analytical layers, AI brain with structured tools, automated briefing generation.

### Stack

- **Backend**: FastAPI + aiosqlite + Python 3.11, `uv` package manager
- **Frontend**: Next.js 16 + React 19 + Tailwind 4 + Recharts
- **Database**: SQLite WAL mode, single `equilibria.db` (~200MB)
- **AI**: Claude Sonnet 4 brain with 20 structured tools, agentic loop
- **Testing**: pytest + pytest-asyncio, ruff linting
- **License**: Apache 2.0
- **Deploy**: Shared OVH VPS, systemd, port 8003

### Project Structure

```
~/equilibria/
  app/
    layers/
      trade/          # L1: 22 modules
      macro/          # L2: 20 modules
      labor/          # L3: 16 modules
      development/    # L4: 16 modules
      agricultural/   # L5: 18 modules
      integration/    # L6: 10 modules
    collectors/       # 13 async data collectors
    briefings/        # 7 AI-generated report types
    ai/               # Claude brain + 20 tools
    api/              # FastAPI routers per layer
    estimation/       # Ported from EconAI (12 estimators)
    figures/          # Binscatter, coefficient plots, event study
    tables/           # Regression, balance, summary stats
    db.py             # SQLite schema + async CRUD
    config.py         # Pydantic settings
    main.py           # FastAPI app
  web/                # Next.js 16 frontend
  tests/              # Mirror app/ structure, 2000+ tests
  data/               # equilibria.db
  docs/               # MkDocs documentation
```

## Layer 1: Trade (22 modules)

International trade analysis grounded in real bilateral data.

- Gravity model estimation (OLS, PPML, Poisson)
- Trade elasticities (import demand, export supply)
- Revealed Comparative Advantage (RCA, RSCA, dynamic RCA)
- Terms of trade computation and decomposition
- Trade openness indices
- HHI export/import concentration
- Bilateral trade decomposition (extensive/intensive margins)
- Trade complementarity index
- Grubel-Lloyd intra-industry trade index
- CBAM impact estimation
- Tariff pass-through analysis
- Trade creation/diversion (gravity-based)
- Product space proximity and complexity
- Export survival analysis
- Market diversification scoring
- Trade-weighted exchange rate effects
- Sanction impact estimation
- Value chain positioning (GVC participation)
- Trade cost decomposition (Novy method)
- Border effect estimation
- Currency union trade effects
- Regional trade agreement evaluation

**Sources**: BACI (via trade.db symlink), UN Comtrade, WITS/TRAINS

## Layer 2: Macro (20 modules)

Macroeconomic analysis and forecasting.

- GDP decomposition (expenditure, income, production sides)
- Phillips curve estimation (traditional, expectations-augmented, NKPC)
- Okun's law estimation and stability testing
- Taylor rule estimation and deviation tracking
- Business cycle dating (HP filter, Hamilton filter, BN decomposition)
- Fiscal multiplier estimation (SVAR approach)
- Debt sustainability analysis (r-g framework)
- Twin deficits hypothesis testing
- Exchange rate pass-through (ERPT)
- Purchasing power parity deviations
- Financial conditions index (PCA-based)
- Credit impulse computation
- Yield curve analysis (term spread, inversion signal)
- Inflation decomposition (core, food, energy)
- Monetary policy transmission
- Output gap estimation (multivariate filter)
- Structural break detection (Bai-Perron)
- Recession probability model (probit)
- Nowcasting (bridge equations)
- VAR impulse response functions

**Sources**: FRED, WDI, IMF WEO/IFS

## Layer 3: Labor (16 modules)

Labor market analysis and human capital economics.

- Mincer wage equation estimation
- Oaxaca-Blinder wage decomposition (gender, race, education)
- Returns to education (OLS, IV with distance/compulsory schooling)
- Migration gravity model
- Remittance determinants and multipliers
- Unemployment duration analysis (hazard models)
- Job matching efficiency (Beveridge curve)
- Shift-share (Bartik) instrument construction and validation
- Labor force participation trends
- Skill premium dynamics
- Wage Phillips curve
- Union wage premium
- Minimum wage employment effects
- Automation exposure scoring
- Labor market tightness index
- Sectoral reallocation (Lilien index)

**Sources**: ILO SDMX, BLS (CPS, JOLTS, QCEW), ACS/Census

## Layer 4: Development (16 modules)

Development economics and institutional analysis.

- Beta convergence (unconditional and conditional)
- Sigma convergence
- Poverty trap detection (threshold estimation)
- Solow residual and TFP decomposition
- Institutional quality (IV with settler mortality, legal origins)
- Aid effectiveness (Burnside-Dollar framework)
- HDI decomposition and dynamics
- Multidimensional Poverty Index computation
- Kuznets curve estimation
- Financial development and growth
- Natural resource curse testing
- Demographic dividend estimation
- Structural transformation tracking (employment shares)
- Inequality decomposition (Theil, GE indices)
- Social mobility estimation (intergenerational elasticity)
- Governance quality composite

**Sources**: WDI, Penn World Table 10.1, PovcalNet/PIP, V-Dem

## Layer 5: Agricultural (18 modules)

Agricultural economics, food security, and climate-agriculture interactions.

- Supply elasticity estimation (Nerlove model)
- Demand system estimation (AIDS, EASI)
- Food security index (composite)
- Caloric trade balance
- Agricultural trade competitiveness (RCA for ag products)
- Land use change analysis
- Deforestation-trade nexus
- Commodity price transmission (VECM, threshold cointegration)
- Market integration testing (law of one price)
- Climate-yield modeling (panel with weather shocks)
- Adaptation cost-benefit analysis
- Fertilizer-yield response curves
- Irrigation returns estimation
- Farm size productivity (inverse relationship testing)
- Agricultural subsidies and distortion (NRA/CTE)
- Food price volatility decomposition
- Supply chain disruption impact
- Water-energy-food nexus scoring

**Sources**: FAOSTAT, USDA/ERS, NOAA Climate, EIA

## Layer 6: Integration (10 modules)

Cross-layer synthesis and AI-powered analysis.

- Composite Economic Analysis Score (CEAS): weighted average across 5 analytical layers, 0-100 scale
- Signal levels: STABLE (0-25), WATCH (25-50), STRESS (50-75), CRISIS (75-100)
- Layer attribution analysis (what drives the composite)
- Cross-layer correlation matrix
- Spillover detection (trade shock -> macro -> labor transmission)
- Structural break detection across layers
- Historical crisis comparison (Asian Crisis 1997, GFC 2008, Euro Crisis 2012, COVID 2020)
- Scenario simulation (what-if analysis)
- Country risk profile generation
- AI briefing orchestrator (coordinates all briefing generators)

## Data Collectors (13)

All async, cached, with rate limiting. Follow Vigil's collector pattern.

| Collector | Source | Est. Rows | Update Frequency |
|-----------|--------|-----------|------------------|
| FRED | Federal Reserve Economic Data | 50K | Weekly |
| WDI | World Bank Development Indicators | 500K | Monthly |
| ILO | International Labour Organization | 80K | Monthly |
| FAOSTAT | FAO Statistics | 200K | Monthly |
| BLS | Bureau of Labor Statistics | 40K | Monthly |
| IMF_WEO | IMF World Economic Outlook | 30K | Biannual |
| PWT | Penn World Table | 20K | Annual |
| Comtrade | UN Comtrade (supplement) | 50K | Quarterly |
| USDA | USDA Economic Research Service | 30K | Monthly |
| NOAA | NOAA Climate Data | 20K | Monthly |
| VDEM | Varieties of Democracy | 15K | Annual |
| PovcalNet | World Bank Poverty Data | 10K | Annual |
| BACI_Bridge | Read-only link to trade.db | 0 (symlink) | N/A |

**Target**: 1M+ real data rows in equilibria.db

## AI Brain

Claude Sonnet 4 with 20 structured tools, agentic multi-step loop.

### Tools by Domain

**Trade**: `estimate_gravity`, `compute_rca`, `bilateral_decomposition`, `tariff_simulation`
**Macro**: `gdp_decompose`, `estimate_phillips`, `fiscal_sustainability`, `cycle_dating`
**Labor**: `wage_decomposition`, `returns_to_education`, `shift_share`
**Development**: `convergence_test`, `poverty_analysis`, `institutional_iv`
**Agricultural**: `demand_system`, `food_security_index`, `price_transmission`
**Cross-cutting**: `run_estimation` (OLS/IV/Panel/DiD/RDD), `generate_table`, `generate_figure`, `query_data`, `compare_countries`

### Agentic Loop

Same as Vigil's NanoClaw pattern:
1. User asks question
2. Brain analyzes intent, selects tools
3. Executes tools in sequence (up to 12 rounds)
4. Validates tool results
5. Synthesizes answer with citations and methodology notes

## Briefings (7 types)

| Briefing | Content | Frequency |
|----------|---------|-----------|
| Economic Conditions | Cross-layer composite assessment | Weekly |
| Trade Flash | Notable bilateral/sector trade shifts | Weekly |
| Labor Market Pulse | Employment, wages, participation trends | Monthly |
| Development Tracker | Convergence, poverty, institutional changes | Quarterly |
| Agricultural Outlook | Food prices, yields, trade, climate stress | Monthly |
| Policy Alert | Significant threshold breaches or structural breaks | Event-driven |
| Country Deep Dive | Full 6-layer analysis for a specific country | On demand |

Each briefing: HTML report with Plotly charts, data tables, methodology notes, AI-written narrative grounded in module outputs.

## Frontend

Next.js 16 + React 19 + Tailwind 4 + Recharts.

### Pages

- `/` - Dashboard: composite score, sparklines per layer, recent briefings
- `/trade` - L1: gravity estimates, RCA heatmap, trade network
- `/macro` - L2: GDP decomposition, Phillips curve, cycle indicators
- `/labor` - L3: wage trends, Mincer estimates, decomposition
- `/development` - L4: convergence scatter, poverty maps, institutions
- `/agricultural` - L5: demand elasticities, food security, price transmission
- `/briefings` - All generated briefings with date filtering
- `/chat` - AI brain interface
- `/methodology` - Module documentation
- `/data` - Data sources, collection status

### Design

- Light theme (matching TradeWeave)
- CSS custom properties for all colors
- Glass-card pattern, professional publication-quality feel
- No emojis
- Every number links to source module, data source, and methodology

## Estimation Engine (ported from EconAI)

12 estimators from BDPolicyLab's EconAI toolkit:

1. OLS (with robust/clustered SE)
2. Instrumental Variables (2SLS, Anderson-Rubin CI)
3. Panel Fixed Effects
4. Difference-in-Differences
5. Regression Discontinuity Design
6. Double Machine Learning
7. Causal Forest
8. Synthetic DiD
9. Staggered DiD (Callaway-Sant'Anna)
10. Shift-Share IV (Bartik)
11. Bounds Estimation (Lee, Manski)
12. Randomization Inference

Plus figure generators (binscatter, coefficient plots, event study) and table generators (regression, balance, summary statistics).

## Testing & Quality

- **Target**: 2,000+ tests
- **Pattern**: Mirror app/ structure in tests/
- **Framework**: pytest + pytest-asyncio, asyncio_mode=auto
- **Lint**: ruff, zero errors
- **Data integrity**: No mock data in production. Test fixtures use small real datasets or clearly labeled synthetic test data
- **Estimator validation**: Each estimator tested against known published results

## Portfolio Stat Targets

| Metric | Target |
|--------|--------|
| Analytical modules | 100+ |
| Passing tests | 2,000+ |
| Real data rows | 1M+ |
| AI tools | 20+ |
| Analytical layers | 6 |
| Data sources | 13 |
| Estimators | 12+ |
| Briefing types | 7 |

## Deployment

- **VPS**: Same OVH VPS (ubuntu@40.160.2.223)
- **Port**: 8003
- **Service**: systemd (equilibria)
- **Deploy**: rsync + systemd restart via deploy.sh
- **Domain**: TBD (equilibria.org or similar)
- **Health**: GET /api/health

## Relationship to Other Projects

- **Vigil**: Architecture template. Same pattern, different domain.
- **EconAI** (BDPolicyLab): Estimation engine ported from here.
- **TradeWeave**: trade.db accessed via read-only symlink for BACI data.
- **BDPolicyLab/BDFacts**: Independent. No code sharing beyond EconAI port.
