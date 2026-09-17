"""
API для SIGNAL ONLY бота.

Это необязательный лёгкий health/status API.
Он НЕ имеет endpoint'ов для открытия/закрытия сделок.
Запуск:
    uvicorn api_signal:app --host 0.0.0.0 --port 8766
"""

from datetime import datetime, timezone
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Crypto Signal Bot API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "mode": "signal_only",
        "time": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/status")
def status():
    return {
        "mode": "signal_only",
        "trading_enabled": False,
        "orders_enabled": False,
        "timeframe": "1h",
        "time": datetime.now(timezone.utc).isoformat(),
    }
