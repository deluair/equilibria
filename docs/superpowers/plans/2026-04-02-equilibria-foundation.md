# Equilibria Foundation Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build complete Equilibria platform - applied economics analysis with 6 layers, 100+ modules, AI brain, briefings, frontend. Deployed to VPS.

**Architecture:** Vigil clone. FastAPI + aiosqlite backend, Next.js 16 frontend, Claude AI brain with 20 tools, 13 data collectors, 7 briefing types.

**Tech Stack:** Python 3.11, FastAPI, aiosqlite, Next.js 16, React 19, Tailwind 4, Recharts, Claude Sonnet 4

---

## Wave 1: Infrastructure (15 parallel agents)

### Agent 1: Project scaffold + config + db
Create pyproject.toml, app/__init__.py, app/config.py, app/db.py, app/main.py, Makefile, .env.example, .gitignore, CLAUDE.md

### Agent 2: Estimation engine (port EconAI)
Port 12 estimators from BDPolicyLab EconAI to app/estimation/

### Agent 3: Collectors base + FRED + WDI
app/collectors/base.py, app/collectors/fred.py, app/collectors/wdi.py

### Agent 4: AI brain + tools
app/ai/brain.py, app/ai/tools.py, app/ai/citations.py

### Agent 5: Briefings base + economic_conditions
app/briefings/base.py, app/briefings/economic_conditions.py

### Agent 6: L1 Trade modules 1-11
app/layers/base.py, app/layers/trade/ (gravity through tariff_passthrough)

### Agent 7: L1 Trade modules 12-22
app/layers/trade/ (trade_creation through rta_evaluation)

### Agent 8: L2 Macro modules 1-10
app/layers/macro/ (gdp_decomposition through ppp)

### Agent 9: L2 Macro modules 11-20
app/layers/macro/ (fci through var_irf)

### Agent 10: L3 Labor all 16 modules
app/layers/labor/

### Agent 11: L4 Development all 16 modules
app/layers/development/

### Agent 12: L5 Agricultural modules 1-9
app/layers/agricultural/ (supply_elasticity through market_integration)

### Agent 13: L5 Agricultural modules 10-18
app/layers/agricultural/ (climate_yield through wef_nexus)

### Agent 14: L6 Integration all 10 modules
app/layers/integration/

### Agent 15: API routes for all 6 layers + health
app/api/ (trade.py, macro.py, labor.py, development.py, agricultural.py, integration.py, health.py, briefings.py, chat.py)

## Wave 2: Frontend + Tests + Remaining Collectors + Deploy

### Agents 16-20: Frontend pages
Next.js 16 app with dashboard, 5 layer pages, briefings, chat, methodology, data

### Agents 21-25: Tests for all layers
Mirror test structure, 2000+ tests target

### Agents 26-28: Remaining collectors
ILO, FAOSTAT, BLS, IMF_WEO, PWT, Comtrade, USDA, NOAA, V-Dem, PovcalNet

### Agent 29: Deploy
deploy.sh, systemd config, nginx config, health check
