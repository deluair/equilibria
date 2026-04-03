# Equilibria: Overnight Salvo Plan

## Current State (as of 9:30pm Apr 2)
- 193 modules, 67K lines, 18 layers
- 13 collectors, 12 estimators, 22 AI tools
- 74K FRED data points on VPS
- 0 tests
- API routes are stubs (return 501)
- Frontend has 10 pages but no real data wiring

---

## Salvo 1: 1am ET - TESTS (15 agents)

Goal: 1500+ tests. Each agent writes tests for a layer.

Paste this in a new session:

```
Build tests for Equilibria at ~/equilibria. I need 15 parallel agents, each writing tests for a different layer. Use pytest-asyncio with asyncio_mode=auto. Each test file mirrors the module path (app/layers/trade/gravity.py -> tests/layers/trade/test_gravity.py). Use a db fixture with tmp_path that inits an in-memory DB. Test that each module's compute() returns a dict with 'score' key. Test edge cases (empty data, missing columns). Target: 100 tests per agent. All agents use worktree isolation. After merging all, run pytest to verify.

Agent assignments:
1. tests/layers/trade/ (28 modules)
2. tests/layers/macro/ (26 modules)
3. tests/layers/labor/ (16 modules)
4. tests/layers/development/ (16 modules)
5. tests/layers/agricultural/ (18 modules)
6. tests/layers/integration/ (10 modules)
7. tests/layers/financial/ (8 modules)
8. tests/layers/health/ (6 modules)
9. tests/layers/environmental/ (8 modules)
10. tests/layers/public/ + tests/layers/spatial/ (12 modules)
11. tests/layers/political/ + tests/layers/behavioral/ (12 modules)
12. tests/layers/industrial/ + tests/layers/monetary/ (12 modules)
13. tests/layers/energy/ + tests/layers/demographic/ (12 modules)
14. tests/layers/methods/ + tests/estimation/ (20 modules)
15. tests/api/ + tests/collectors/ + tests/ai/ + tests/briefings/ (infrastructure)
```

---

## Salvo 2: 6am ET - WIRE-UP + POLISH (15 agents)

Goal: Make the platform actually work end-to-end. Real API responses, real frontend data.

Paste this in a new session:

```
Wire up Equilibria at ~/equilibria. I need 15 parallel agents making the platform functional end-to-end. All agents use worktree isolation.

Agent assignments:
1. Wire /api/trade endpoints to real trade layer modules (gravity, rca, concentration, etc.)
2. Wire /api/macro endpoints to real macro modules (gdp, phillips, taylor, cycle, fci, recession)
3. Wire /api/labor + /api/development endpoints to real modules
4. Wire /api/agricultural + /api/integration endpoints to real modules
5. Wire /api/briefings to real briefing generators (economic_conditions, trade_flash, country_deep_dive)
6. Wire /api/chat to AI brain (import analyze from app.ai.brain, handle conversation)
7. Add frontend pages for new layers: /financial, /health, /environmental (Next.js pages)
8. Add frontend pages: /public, /spatial, /political, /behavioral (Next.js pages)
9. Add frontend pages: /industrial, /monetary, /energy, /demographic (Next.js pages)
10. Add 4 more briefing generators: labor_pulse, development_tracker, agricultural_outlook, policy_alert
11. Add CLI tool (app/cli.py): serve, collect-all, collect [source], generate-briefing [type], status
12. Run WDI + ILO + BLS collectors on VPS to populate more data
13. Update CLAUDE.md with final 193 module count, all 18 layers, deployment info
14. Update portfolio page (equilibria.html) with 193 modules, 18 layers
15. Final full deploy: rsync, restart, health check, smoke test all endpoints
```

---

## Expected Outcome

After both salvos:
- 1500+ tests passing
- All API endpoints returning real data
- Frontend showing live charts
- CLI tool for operations
- 5+ data sources populated
- Updated portfolio page
- Production-ready platform
