from pathlib import Path

from pydantic_settings import BaseSettings

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"


class Settings(BaseSettings):
    app_name: str = "Equilibria"
    app_version: str = "0.1.0"
    debug: bool = False
    host: str = "127.0.0.1"
    port: int = 8003
    db_path: str = str(DATA_DIR / "equilibria.db")
    db_pool_size: int = 5
    api_key: str = ""
    allowed_origins: str = "http://localhost:3001,http://localhost:8003"
    fred_api_key: str = ""
    anthropic_api_key: str = ""
    comtrade_api_key: str = ""
    eia_api_key: str = ""
    bls_api_key: str = ""
    noaa_token: str = ""
    frontend_url: str = "http://localhost:3001"
    api_url: str = "http://localhost:8003"
    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}

    @property
    def origins_list(self) -> list[str]:
        return [o.strip() for o in self.allowed_origins.split(",") if o.strip()]


settings = Settings()

LAYER_WEIGHTS = {"l1": 0.20, "l2": 0.20, "l3": 0.20, "l4": 0.20, "l5": 0.20}

SIGNAL_LEVELS = {
    (0.0, 25.0): "STABLE",
    (25.0, 50.0): "WATCH",
    (50.0, 75.0): "STRESS",
    (75.0, 100.0): "CRISIS",
}
