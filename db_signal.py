"""
Необязательная SQLite БД для истории отправленных сигналов.

В SIGNAL ONLY версии она хранит именно сигналы, а не сделки.
"""

import os
import sqlite3
from threading import Lock
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "signals.db")
DB_LOCK = Lock()


def get_conn():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with DB_LOCK:
        conn = get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_name TEXT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                strategy TEXT,
                timeframe TEXT DEFAULT '1h',
                candle_time TEXT,
                entry_price REAL,
                tp REAL,
                sl REAL,
                natr REAL,
                vol_ratio REAL,
                vol_24h REAL,
                delta_pct REAL,
                corr_btc REAL,
                signals TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_signals_created ON signals(created_at)"
        )
        conn.commit()
        conn.close()


def insert_signal(bot_name, data, tp, sl, corr_btc):
    with DB_LOCK:
        conn = get_conn()
        conn.execute("""
            INSERT INTO signals (
                bot_name, symbol, side, strategy, timeframe, candle_time,
                entry_price, tp, sl, natr, vol_ratio, vol_24h,
                delta_pct, corr_btc, signals, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            bot_name,
            data["symbol"],
            data["side"],
            "12:4",
            "1h",
            datetime.fromtimestamp(
                data["candle_time"] / 1000, timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S"),
            data["entry"],
            tp,
            sl,
            data.get("natr"),
            data.get("vol_ratio"),
            data.get("volume_24h"),
            data.get("delta_pct"),
            corr_btc,
            ", ".join(data.get("signals", [])),
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        ))
        conn.commit()
        conn.close()
