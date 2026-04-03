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
