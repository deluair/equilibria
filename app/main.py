import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.db import close_db, init_db

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    logger.info("%s v%s started", settings.app_name, settings.app_version)
    yield
    await close_db()
    logger.info("%s shut down", settings.app_name)


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# GZip
app.add_middleware(GZipMiddleware, minimum_size=500)


# Custom header middleware
@app.middleware("http")
async def add_custom_headers(request: Request, call_next):
    response: Response = await call_next(request)
    response.headers["X-Crafted-By"] = "Md Deluair Hossen, PhD"
    response.headers["X-Origin"] = "equilibria"
    return response


# Health check
@app.get("/api/health")
async def health():
    return {"status": "ok", "app": settings.app_name, "version": settings.app_version}


# Layer API routers (optional, loaded if present)
_router_modules = [
    ("app.api.health", "/api", "Health"),
    ("app.api.trade", "/api", "L1 Trade"),
    ("app.api.macro", "/api", "L2 Macro"),
    ("app.api.labor", "/api", "L3 Labor"),
    ("app.api.development", "/api", "L4 Development"),
    ("app.api.agricultural", "/api", "L5 Agricultural"),
    ("app.api.integration", "/api", "L6 Integration"),
    ("app.api.briefings", "/api", "Briefings"),
    ("app.api.chat", "/api", "Chat"),
    ("app.api.kb", "/api", "Knowledge Base"),
    ("app.api.crosscutting", "/api", "L-CX Crosscutting"),
    ("app.api.cultural", "/api", "L-CU Cultural"),
    ("app.api.game_theory", "/api", "L-GT Game Theory"),
    ("app.api.history", "/api", "L-HI History"),
    ("app.api.international", "/api", "L-IN International"),
    ("app.api.real_estate", "/api", "L-RE Real Estate"),
    ("app.api.risk", "/api", "L-RI Risk"),
    ("app.api.technology", "/api", "L-TE Technology"),
    ("app.api.welfare", "/api", "L-WE Welfare"),
    ("app.api.behavioral", "/api", "L-BE Behavioral"),
    ("app.api.competition", "/api", "L-CO Competition"),
    ("app.api.complexity", "/api", "L-CP Complexity"),
    ("app.api.demographic", "/api", "L-DM Demographic"),
    ("app.api.digital_economy", "/api", "L-DG Digital Economy"),
    ("app.api.digital_finance", "/api", "L-DF Digital Finance"),
    ("app.api.disaster_economics", "/api", "L-DE Disaster Economics"),
    ("app.api.energy", "/api", "L-EN Energy"),
    ("app.api.environmental", "/api", "L-EV Environmental"),
    ("app.api.financial", "/api", "L-FI Financial"),
    ("app.api.fiscal_policy", "/api", "L-FP Fiscal Policy"),
    ("app.api.food_security", "/api", "L-FS Food Security"),
    ("app.api.global_value_chains", "/api", "L-VC Global Value Chains"),
    ("app.api.governance", "/api", "L-GV Governance"),
    ("app.api.income_distribution", "/api", "L-ID Income Distribution"),
    ("app.api.industrial", "/api", "L-IN Industrial"),
    ("app.api.inequality", "/api", "L-IQ Inequality"),
    ("app.api.innovation", "/api", "L-IO Innovation"),
    ("app.api.macroprudential", "/api", "L-MP Macroprudential"),
    ("app.api.methods", "/api", "L-MT Methods"),
    ("app.api.migration_econ", "/api", "L-ME Migration Economics"),
    ("app.api.migration_integration", "/api", "L-MI Migration Integration"),
    ("app.api.monetary", "/api", "L-MO Monetary"),
    ("app.api.pension_systems", "/api", "L-PS Pension Systems"),
    ("app.api.political", "/api", "L-PO Political"),
    ("app.api.public", "/api", "L-PU Public Economics"),
    ("app.api.regional_development", "/api", "L-RD Regional Development"),
    ("app.api.social_protection", "/api", "L-SP Social Protection"),
    ("app.api.spatial", "/api", "L-SL Spatial"),
    ("app.api.sustainability", "/api", "L-SU Sustainability"),
    ("app.api.trade_policy", "/api", "L-TP Trade Policy"),
    ("app.api.urban_economics", "/api", "L-UE Urban Economics"),
    ("app.api.commodity_economics", "/api", "L-CM Commodity Economics"),
    ("app.api.gender_economics", "/api", "L-GE Gender Economics"),
    ("app.api.infrastructure_economics", "/api", "L-IF Infrastructure Economics"),
    ("app.api.conflict_economics", "/api", "L-CW Conflict Economics"),
    ("app.api.housing_economics", "/api", "L-HO Housing Economics"),
    ("app.api.education_economics", "/api", "L-ED Education Economics"),
    ("app.api.capital_markets", "/api", "L-CK Capital Markets"),
    ("app.api.entrepreneurship", "/api", "L-ER Entrepreneurship"),
    ("app.api.natural_resources", "/api", "L-NR Natural Resources"),
    ("app.api.labor_institutions", "/api", "L-LI Labor Institutions"),
    ("app.api.health_financing", "/api", "L-HF Health Financing"),
    ("app.api.external_debt", "/api", "L-XD External Debt"),
    ("app.api.monetary_policy", "/api", "L-MY Monetary Policy"),
    ("app.api.climate_finance", "/api", "L-GF Climate Finance"),
    ("app.api.agricultural_policy", "/api", "L-AP Agricultural Policy"),
    ("app.api.behavioral_finance", "/api", "L-BF Behavioral Finance"),
    ("app.api.environmental_accounting", "/api", "L-EA Environmental Accounting"),
    ("app.api.social_capital", "/api", "L-SC Social Capital"),
    ("app.api.demographic_transition", "/api", "L-DT Demographic Transition"),
    ("app.api.institutional_economics", "/api", "L-IE Institutional Economics"),
    ("app.api.tourism_economics", "/api", "L-TO Tourism Economics"),
    ("app.api.supply_chain_resilience", "/api", "L-SR Supply Chain Resilience"),
    ("app.api.pharmaceutical_economics", "/api", "L-PH Pharmaceutical Economics"),
    ("app.api.poverty_measurement", "/api", "L-PM Poverty Measurement"),
    ("app.api.financial_regulation", "/api", "L-FR Financial Regulation"),
    ("app.api.law_economics", "/api", "L-LW Law Economics"),
    ("app.api.knowledge_economy", "/api", "L-KE Knowledge Economy"),
    ("app.api.trade_finance_econ", "/api", "L-TF Trade Finance Economics"),
    ("app.api.health_technology", "/api", "L-HT Health Technology"),
    ("app.api.agricultural_finance", "/api", "L-AF Agricultural Finance"),
    ("app.api.aging_economics", "/api", "L-AG Aging Economics"),
    ("app.api.platform_economics", "/api", "L-PE Platform Economics"),
    ("app.api.circular_economy", "/api", "L-CE Circular Economy"),
    ("app.api.healthcare_market", "/api", "L-HM Healthcare Market"),
    ("app.api.ocean_economics", "/api", "L-OE Ocean Economics"),
    ("app.api.financial_crisis", "/api", "L-FC Financial Crisis"),
    ("app.api.local_government_economics", "/api", "L-LG Local Government Economics"),
    ("app.api.welfare_state_economics", "/api", "L-WS Welfare State Economics"),
    ("app.api.green_transition", "/api", "L-GT Green Transition"),
    ("app.api.urban_planning_economics", "/api", "L-UP Urban Planning Economics"),
    ("app.api.geopolitical_economics", "/api", "L-GP Geopolitical Economics"),
    ("app.api.transport_economics", "/api", "L-TR Transport Economics"),
    ("app.api.water_economics", "/api", "L-WA Water Economics"),
    ("app.api.defense_economics", "/api", "L-DX Defense Economics"),
    ("app.api.ai_economics", "/api", "L-AI AI Economics"),
    ("app.api.nonprofit_economics", "/api", "L-NP Nonprofit Economics"),
    ("app.api.international_monetary_system", "/api", "L-MS International Monetary System"),
    ("app.api.energy_security", "/api", "L-ES Energy Security"),
    ("app.api.post_covid_recovery", "/api", "L-PC Post-Covid Recovery"),
    ("app.api.media_economics", "/api", "L-MD Media Economics"),
    ("app.api.labor_market_matching", "/api", "L-LM Labor Market Matching"),
    ("app.api.sports_economics", "/api", "L-SP Sports Economics"),
    ("app.api.happiness_economics", "/api", "L-HE Happiness Economics"),
    ("app.api.criminal_justice_economics", "/api", "L-CJ Criminal Justice Economics"),
    ("app.api.bioeconomy", "/api", "L-BI Bioeconomy"),
    ("app.api.disability_economics", "/api", "L-DI Disability Economics"),
    ("app.api.arts_economics", "/api", "L-AR Arts Economics"),
]

for module_path, prefix, tag in _router_modules:
    try:
        import importlib

        mod = importlib.import_module(module_path)
        app.include_router(mod.router, prefix=prefix, tags=[tag])
        logger.info("Loaded router: %s", tag)
    except (ImportError, AttributeError):
        logger.debug("Router not available: %s", module_path)

# Static files
static_dir = Path(__file__).resolve().parent.parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
