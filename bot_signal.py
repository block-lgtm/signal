import os
import json
import time
import math
import tempfile
import traceback
from datetime import datetime, UTC
from threading import Thread, Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from dotenv import load_dotenv
from binance.client import Client

# ============================================================
# SIGNAL BOT
# Рынок только читаем. Ордера/баланс/плечо/закрытие позиций
# намеренно отсутствуют.
#
# Запуск:
#   python bot_signal.py --config conf12_4.json
# ============================================================

load_dotenv()

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--config", required=True)
args = parser.parse_args()

with open(args.config, "r", encoding="utf-8") as f:
    config = json.load(f)

BOT_NAME = config.get("NAME", "SIGNAL12_4")
STRATEGY_NAME = "12:4"

# ---------------- Telegram ----------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# ---------------- Signal settings ----------------
MIN_24H_VOLUME = float(config.get("MIN_24H_VOLUME", 40_000_000))
LOOKBACK_CANDLES = int(config.get("LOOKBACK_CANDLES", 500))
VOLUME_LOOKBACK = int(config.get("VOLUME_LOOKBACK", 10))
VOL_MULT = float(config.get("VOL_MULT", 2.0))
MIN_BODY_PCT = float(config.get("MIN_BODY_PCT", 10.0))
COOLDOWN_BARS = int(config.get("COOLDOWN_BARS", 0))
EMA_FAST = int(config.get("EMA_FAST", 20))
EMA_SLOW = int(config.get("EMA_SLOW", 200))
ATR_LEN = int(config.get("ATR_LEN", 50))
BTC_LOOKBACK = int(config.get("BTC_LOOKBACK", 50))
USE_EMA_FILTER = bool(config.get("USE_EMA_FILTER", True))
USE_VWAP_FILTER = bool(config.get("USE_VWAP_FILTER", False))

GLOBAL_SKIP_DAYS = [d.lower() for d in config.get("SKIP_DAYS", [])]
USE_CORREL_FILTER = bool(config.get("USE_CORREL_FILTER", False))
CORREL_MIN_BUY = float(config.get("CORREL_MIN_BUY", 0))
CORREL_MAX_BUY = float(config.get("CORREL_MAX_BUY", 0))
CORREL_MIN_SELL = float(config.get("CORREL_MIN_SELL", 0))
CORREL_MAX_SELL = float(config.get("CORREL_MAX_SELL", 0))

STRAT_RAW = config.get("STRATEGIES_CONFIG", {})
S124 = STRAT_RAW.get(
    STRATEGY_NAME,
    {"enabled": True, "tp": 0.12, "sl": 0.04, "BUY": {}, "SELL": {}},
)
if not S124.get("enabled", True):
    raise RuntimeError("Стратегия 12:4 отключена в конфиге")

STRAT_CFG = {
    "tp": float(S124["tp"]),
    "sl": float(S124["sl"]),
    "BUY": S124.get("BUY", {}),
    "SELL": S124.get("SELL", {}),
}

# ---------------- Runtime ----------------
BLACKLIST = {
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT",
    "XRPUSDT", "ADAUSDT", "DOGEUSDT", "LINKUSDT",
    "INTCUSDT",
}

ALLOW_BUY = True
ALLOW_SELL = True
STATE_LOCK = Lock()

# Последняя обработанная закрытая свеча по символу.
LAST_CANDLE_TIME = {}
# Чтобы не повторять один и тот же сигнал на той же свече.
LAST_SIGNAL_CANDLE = {}

# Публичные market-data endpoints Binance не требуют ключей.
client = Client(None, None)

# ============================================================
# Helpers
# ============================================================

def fmt_price(v: float) -> str:
    if v is None:
        return "—"
    v = float(v)
    if v >= 1000:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:.4f}"
    if v >= 0.01:
        return f"{v:.5f}"
    return f"{v:.8f}".rstrip("0").rstrip(".")


def fmt_volume(v: float) -> str:
    v = float(v)
    if v >= 1_000_000_000:
        return f"{v / 1_000_000_000:.2f}B"
    if v >= 1_000_000:
        return f"{v / 1_000_000:.1f}M"
    if v >= 1_000:
        return f"{v / 1_000:.1f}K"
    return f"{v:.0f}"


def apply_range_filter(value, min_val, max_val):
    if value is None:
        return False
    if min_val != 0 and value < min_val:
        return False
    if max_val != 0 and value > max_val:
        return False
    return True


def calculate_atr(df, period):
    hl = df["high"] - df["low"]
    hc = (df["high"] - df["close"].shift()).abs()
    lc = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def calculate_session_vwap(df):
    x = df.copy()
    x["date"] = pd.to_datetime(x["open_time"], unit="ms", utc=True).dt.date
    tp = (x["high"] + x["low"] + x["close"]) / 3
    x["tpv"] = tp * x["volume"]
    x["cum_tpv"] = x.groupby("date")["tpv"].cumsum()
    x["cum_vol"] = x.groupby("date")["volume"].cumsum()
    return x["cum_tpv"] / x["cum_vol"]


def calculate_delta(last_row):
    rng = last_row["high"] - last_row["low"]
    if rng == 0 or last_row["volume"] == 0:
        return 0.0
    buy_vol = last_row["volume"] * (last_row["close"] - last_row["low"]) / rng
    sell_vol = last_row["volume"] - buy_vol
    return round((buy_vol - sell_vol) / last_row["volume"] * 100, 2)


def check_strategy_filters(side, natr, delta_pct, corr):
    f = STRAT_CFG.get(side, {})
    if not f:
        return True

    today = datetime.now(UTC).strftime("%A").lower()
    if today in [d.lower() for d in f.get("SKIP_DAYS", [])]:
        return False

    if f.get("USE_NATR_FILTER"):
        if not apply_range_filter(natr, f.get("NATR_MIN", 0), f.get("NATR_MAX", 0)):
            return False

    if f.get("USE_DELTA_FILTER"):
        # Сохраняем исходную логику стратегии.
        val = delta_pct if side == "BUY" else -delta_pct
        if not apply_range_filter(val, f.get("DELTA_MIN", 0), f.get("DELTA_MAX", 0)):
            return False

    if f.get("USE_CORREL_FILTER") and corr is not None:
        try:
            if not apply_range_filter(
                float(corr),
                f.get("CORREL_MIN", 0),
                f.get("CORREL_MAX", 0),
            ):
                return False
        except (ValueError, TypeError):
            pass

    return True


# ============================================================
# Binance market data
# ============================================================

def get_liquid_futures_symbols():
    tickers = client.futures_ticker()
    symbols = []
    for t in tickers:
        sym = t.get("symbol", "")
        if not sym.endswith("USDT") or sym in BLACKLIST or "_" in sym:
            continue
        try:
            if float(t.get("quoteVolume", 0)) < MIN_24H_VOLUME:
                continue
        except (TypeError, ValueError):
            continue
        symbols.append(sym)
    return symbols


def get_klines(symbol, limit=LOOKBACK_CANDLES):
    klines = client.futures_klines(
        symbol=symbol,
        interval=Client.KLINE_INTERVAL_1HOUR,
        limit=limit,
    )
    df = pd.DataFrame(
        klines,
        columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades",
            "taker_buy_base", "taker_buy_quote", "ignore",
        ],
    )
    for c in ["open", "high", "low", "close", "volume", "quote_volume"]:
        df[c] = df[c].astype(float)
    return df


def get_btc_returns():
    try:
        df = get_klines("BTCUSDT", BTC_LOOKBACK)
        return df["close"].pct_change()
    except Exception as e:
        print(f"Ошибка BTC returns: {e}")
        return None


def get_correlation(symbol, btc_ret):
    if btc_ret is None:
        return None
    try:
        sym = get_klines(symbol, BTC_LOOKBACK)
        sym_ret = sym["close"].pct_change()
        n = min(len(btc_ret), len(sym_ret))
        if n < 5:
            return None
        corr = btc_ret.iloc[-n:].corr(sym_ret.iloc[-n:])
        return round(float(corr), 2) if pd.notna(corr) else None
    except Exception as e:
        print(f"Ошибка корреляции {symbol}: {e}")
        return None


# ============================================================
# Signal calculation
# ============================================================

def check_volume_signal(symbol):
    df = get_klines(symbol, LOOKBACK_CANDLES)

    if len(df) < max(EMA_SLOW + 5, VOLUME_LOOKBACK + 5, ATR_LEN + 5):
        return None

    df["ema_fast"] = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()
    df["atr"] = calculate_atr(df, ATR_LEN)
    df["natr"] = (df["atr"] / df["close"]) * 100
    df["vwap"] = calculate_session_vwap(df)
    df["quote_volume_calc"] = df["close"] * df["volume"]

    # Последняя строка — текущая незакрытая свеча.
    # Анализируем предыдущую, уже закрытую свечу.
    last = df.iloc[-2]
    avg_start = -(VOLUME_LOOKBACK + 2)
    avg_end = -2
    avg_vol = df["quote_volume_calc"].iloc[avg_start:avg_end].mean()

    if not math.isfinite(avg_vol) or avg_vol <= 0:
        return None

    volume_spike = last["quote_volume_calc"] >= avg_vol * VOL_MULT
    body = abs(last["close"] - last["open"])
    rng = last["high"] - last["low"]
    body_pct = 0 if rng == 0 else body / rng * 100
    bull = last["close"] > last["open"]
    bear = last["close"] < last["open"]
    strong = body_pct >= MIN_BODY_PCT

    bull_trend = last["ema_fast"] > last["ema_slow"]
    bear_trend = last["ema_fast"] < last["ema_slow"]
    ema_bull_ok = bull_trend if USE_EMA_FILTER else True
    ema_bear_ok = bear_trend if USE_EMA_FILTER else True

    below_vwap = last["close"] < last["vwap"] if USE_VWAP_FILTER else True
    above_vwap = last["close"] > last["vwap"] if USE_VWAP_FILTER else True

    recent_spike = False
    if COOLDOWN_BARS > 0:
        recent = df.iloc[-(COOLDOWN_BARS + 2):-2]
        recent_spike = (
            recent["quote_volume_calc"] >= avg_vol * VOL_MULT
        ).any()

    delta_pct = calculate_delta(last)

    signals = []
    if volume_spike and bull and strong and ema_bull_ok and below_vwap and not recent_spike:
        signals.append("BUY_TREND")
    if volume_spike and bear and strong and ema_bear_ok and above_vwap and not recent_spike:
        signals.append("SELL_TREND")

    if not signals:
        return None

    # При нормальной логике свеча не может быть одновременно bull и bear.
    side = "BUY" if "BUY_TREND" in signals else "SELL"

    try:
        ticker = client.futures_ticker(symbol=symbol)
        if isinstance(ticker, list):
            ticker = next((x for x in ticker if x.get("symbol") == symbol), ticker[0])
        volume_24h = float(ticker["quoteVolume"])
    except Exception:
        volume_24h = float(last["quote_volume_calc"])

    return {
        "symbol": symbol,
        "side": side,
        "signals": signals,
        "entry": float(last["close"]),
        "candle_time": int(last["open_time"]),
        "open": float(last["open"]),
        "high": float(last["high"]),
        "low": float(last["low"]),
        "close": float(last["close"]),
        "body_pct": round(float(body_pct), 2),
        "natr": round(float(last["natr"]), 3) if pd.notna(last["natr"]) else None,
        "vol_ratio": round(float(last["quote_volume_calc"] / avg_vol), 2),
        "volume_24h": volume_24h,
        "delta_pct": delta_pct,
        "ema_fast": float(last["ema_fast"]),
        "ema_slow": float(last["ema_slow"]),
        "vwap": float(last["vwap"]) if pd.notna(last["vwap"]) else None,
        "df": df,
    }


# ============================================================
# Matplotlib chart
# ============================================================

def _nice_decimals(price):
    if price >= 1000:
        return 2
    if price >= 100:
        return 2
    if price >= 1:
        return 4
    if price >= 0.01:
        return 6
    return 8


def make_signal_chart(res):
    """
    Создаёт PNG непосредственно в Python/matplotlib:
    свечи + EMA20/EMA200 + объём + Entry/TP/SL + маркер сигнальной свечи.
    """
    df = res["df"].copy().tail(180).reset_index(drop=True)
    entry = res["entry"]
    side = res["side"]

    if side == "BUY":
        tp = entry * (1 + STRAT_CFG["tp"])
        sl = entry * (1 - STRAT_CFG["sl"])
    else:
        tp = entry * (1 - STRAT_CFG["tp"])
        sl = entry * (1 + STRAT_CFG["sl"])

    fig = plt.figure(figsize=(13.5, 8.2), dpi=150)
    gs = fig.add_gridspec(
        5, 1,
        height_ratios=[4.8, 0.8, 0.08, 0.08, 0.08],
        hspace=0.05,
    )
    ax = fig.add_subplot(gs[0])
    ax_vol = fig.add_subplot(gs[1], sharex=ax)

    # Тёмный dashboard-подобный вид без зависимости от стороннего
    # свечного пакета.
    fig.patch.set_facecolor("#0f1117")
    ax.set_facecolor("#161b27")
    ax_vol.set_facecolor("#161b27")

    x = list(range(len(df)))
    width = 0.62
    price_dec = _nice_decimals(entry)

    for i, row in df.iterrows():
        o, h, l, c = row["open"], row["high"], row["low"], row["close"]
        up = c >= o
        body_bottom = min(o, c)
        body_height = max(abs(c - o), max(abs(entry), 1) * 1e-8)

        # Matplotlib default colors intentionally not used: the chart
        # needs a clear trading color convention.
        candle_color = "#22c55e" if up else "#ef4444"
        ax.vlines(i, l, h, color=candle_color, linewidth=0.9, alpha=0.95)
        ax.add_patch(
            Rectangle(
                (i - width / 2, body_bottom),
                width,
                body_height,
                facecolor=candle_color,
                edgecolor=candle_color,
                linewidth=0.7,
            )
        )

    ax.plot(x, df["ema_fast"], linewidth=1.2, color="#38bdf8", label=f"EMA {EMA_FAST}")
    ax.plot(x, df["ema_slow"], linewidth=1.1, color="#a78bfa", label=f"EMA {EMA_SLOW}")

    if USE_VWAP_FILTER and df["vwap"].notna().any():
        ax.plot(x, df["vwap"], linewidth=1.0, color="#eab308", alpha=0.9, label="VWAP")

    # Уровни сигнала.
    ax.axhline(entry, color="#38bdf8", linewidth=1.5, alpha=0.95, linestyle="-")
    ax.axhline(tp, color="#22c55e", linewidth=1.2, alpha=0.9, linestyle="--")
    ax.axhline(sl, color="#ef4444", linewidth=1.2, alpha=0.9, linestyle="--")

    signal_i = len(df) - 1
    ax.scatter(
        [signal_i],
        [entry],
        s=65,
        marker="^" if side == "BUY" else "v",
        color="#6366f1",
        edgecolors="white",
        linewidths=0.8,
        zorder=10,
    )

    # Подписи справа.
    x_text = len(df) + 1.5
    ax.text(x_text, entry, f" ENTRY  {fmt_price(entry)}", va="center",
            fontsize=9, color="#38bdf8", fontweight="bold")
    ax.text(x_text, tp, f" TP  {fmt_price(tp)}", va="center",
            fontsize=9, color="#22c55e", fontweight="bold")
    ax.text(x_text, sl, f" SL  {fmt_price(sl)}", va="center",
            fontsize=9, color="#ef4444", fontweight="bold")

    ax.set_xlim(-1, len(df) + 13)
    ax.grid(True, alpha=0.16, linewidth=0.6)
    ax.tick_params(colors="#94a3b8", labelsize=8)
    for spine in ax.spines.values():
        spine.set_color("#2a3045")

    title_side = "🟢 BUY" if side == "BUY" else "🔴 SELL"
    ax.set_title(
        f"{res['symbol']}  ·  1H   {title_side}   ·   {STRATEGY_NAME}",
        loc="left",
        color="#e2e8f0",
        fontsize=14,
        fontweight="bold",
        pad=12,
    )
    ax.text(
        1.0, 1.015,
        f"Vol x{res['vol_ratio']:.2f}  |  NATR {res['natr']:.3f}%  |  "
        f"Delta {res['delta_pct']:+.2f}%",
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        color="#94a3b8",
        fontsize=9,
    )
    ax.legend(
        loc="upper left",
        frameon=False,
        fontsize=8,
        labelcolor="#94a3b8",
        ncol=3,
    )
    ax.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda v, p: f"{v:.{price_dec}f}")
    )

    # Объём.
    vol_colors = [
        "#22c55e" if row["close"] >= row["open"] else "#ef4444"
        for _, row in df.iterrows()
    ]
    ax_vol.bar(x, df["quote_volume_calc"], width=0.65, color=vol_colors, alpha=0.55)
    ax_vol.set_yticks([])
    ax_vol.grid(False)
    for spine in ax_vol.spines.values():
        spine.set_color("#2a3045")
    ax_vol.tick_params(colors="#64748b", labelsize=7)

    # Подпись параметров.
    fig.text(
        0.015, 0.012,
        f"Signal candle: {datetime.fromtimestamp(res['candle_time']/1000, UTC).strftime('%Y-%m-%d %H:%M UTC')}  "
        f"|  24h Vol: {fmt_volume(res['volume_24h'])} USDT  "
        f"|  Body: {res['body_pct']:.1f}%",
        color="#64748b",
        fontsize=8,
    )

    fig.subplots_adjust(left=0.055, right=0.92, top=0.91, bottom=0.075)

    fd, path = tempfile.mkstemp(prefix="signal_", suffix=".png")
    os.close(fd)
    fig.savefig(path, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)
    return path, tp, sl


# ============================================================
# Telegram
# ============================================================

def telegram_request(method, data=None, files=None, timeout=20):
    if not BOT_TOKEN:
        print("⚠️ BOT_TOKEN не задан")
        return None

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    try:
        r = requests.post(url, data=data, files=files, timeout=timeout)
        if not r.ok:
            print(f"Telegram HTTP {r.status_code}: {r.text[:500]}")
            return None
        return r.json()
    except Exception as e:
        print(f"Ошибка Telegram {method}: {e}")
        return None


def send_message(text):
    if not CHAT_ID:
        print("⚠️ CHAT_ID не задан")
        return
    telegram_request(
        "sendMessage",
        data={"chat_id": CHAT_ID, "text": text},
    )


def send_signal_photo(path, caption):
    if not BOT_TOKEN or not CHAT_ID:
        return
    try:
        with open(path, "rb") as photo:
            telegram_request(
                "sendPhoto",
                data={
                    "chat_id": CHAT_ID,
                    "caption": caption,
                },
                files={"photo": photo},
                timeout=30,
            )
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


def signal_caption(res, tp, sl, corr):
    side_emoji = "🟢" if res["side"] == "BUY" else "🔴"
    side_word = "LONG" if res["side"] == "BUY" else "SHORT"

    f_buy = STRAT_CFG["BUY"]
    f_sell = STRAT_CFG["SELL"]
    f = f_buy if res["side"] == "BUY" else f_sell

    skip_days = ", ".join(f.get("SKIP_DAYS", [])) or "—"

    return (
        f"{side_emoji} <b>{side_word} SIGNAL</b>  ·  {BOT_NAME}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💎 <b>{res['symbol']}</b>   ·   1H   ·   {STRATEGY_NAME}\n\n"
        f"💰 Entry: <b>{fmt_price(res['entry'])}</b>\n"
        f"🎯 TP: <b>{fmt_price(tp)}</b>  (+{STRAT_CFG['tp']*100:.1f}%)\n"
        f"🛡 SL: <b>{fmt_price(sl)}</b>  (-{STRAT_CFG['sl']*100:.1f}%)\n\n"
        f"📊 Volume: <b>x{res['vol_ratio']:.2f}</b>  ·  24h {fmt_volume(res['volume_24h'])} USDT\n"
        f"📐 Body: <b>{res['body_pct']:.1f}%</b>  ·  NATR: <b>{res['natr']:.3f}%</b>\n"
        f"⚡ Delta: <b>{res['delta_pct']:+.2f}%</b>  ·  BTC Corr: <b>{corr if corr is not None else 'N/A'}</b>\n"
        f"📈 EMA{EMA_FAST}: {fmt_price(res['ema_fast'])}  ·  EMA{EMA_SLOW}: {fmt_price(res['ema_slow'])}\n"
        f"🧠 Signal: <b>{', '.join(res['signals'])}</b>\n"
        f"📅 Skip days: {skip_days}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"⏱ Candle: {datetime.fromtimestamp(res['candle_time']/1000, UTC).strftime('%d.%m.%Y %H:%M UTC')}\n"
        f"⚠️ Сигнал информационный. Ордер автоматически не открывается."
    )


def send_status():
    with STATE_LOCK:
        buy = ALLOW_BUY
        sell = ALLOW_SELL
    send_message(
        f"🤖 {BOT_NAME}\n"
        f"Статус: 🟢 работает\n"
        f"BUY: {'🟢 ON' if buy else '🔴 OFF'}\n"
        f"SELL: {'🟢 ON' if sell else '🔴 OFF'}\n"
        f"Стратегия: {STRATEGY_NAME}\n"
        f"Таймфрейм: 1H\n"
        f"Режим: только сигналы, без торговли"
    )


def telegram_commands():
    global ALLOW_BUY, ALLOW_SELL

    if not BOT_TOKEN:
        return

    # Пропускаем накопленные старые команды.
    offset = 0
    try:
        data = requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
            params={"offset": -1, "timeout": 1},
            timeout=5,
        ).json()
        updates = data.get("result", [])
        if updates:
            offset = updates[-1]["update_id"] + 1
    except Exception:
        pass

    while True:
        try:
            r = requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                params={"offset": offset, "timeout": 25},
                timeout=30,
            )
            updates = r.json().get("result", [])
            for upd in updates:
                offset = upd["update_id"] + 1
                message = upd.get("message", {})
                text = (message.get("text") or "").strip().lower()

                # При заданном CHAT_ID принимаем команды только из него.
                if CHAT_ID and str(message.get("chat", {}).get("id")) != str(CHAT_ID):
                    continue

                if text == "/status":
                    send_status()
                elif text == "/nobuy":
                    with STATE_LOCK:
                        ALLOW_BUY = False
                    send_message("🔴 BUY сигналы отключены")
                elif text == "/nosell":
                    with STATE_LOCK:
                        ALLOW_SELL = False
                    send_message("🔴 SELL сигналы отключены")
                elif text == "/buy":
                    with STATE_LOCK:
                        ALLOW_BUY = True
                    send_message("🟢 BUY сигналы включены")
                elif text == "/sell":
                    with STATE_LOCK:
                        ALLOW_SELL = True
                    send_message("🟢 SELL сигналы включены")
                elif text == "/help" or text == "/start":
                    send_message(
                        "Команды:\n"
                        "/status — статус бота\n"
                        "/nobuy — выключить BUY\n"
                        "/nosell — выключить SELL\n"
                        "/buy — включить BUY\n"
                        "/sell — включить SELL"
                    )
        except Exception as e:
            print(f"Ошибка Telegram polling: {e}")
            time.sleep(3)


# ============================================================
# Signal processing
# ============================================================

def process_symbol(symbol, btc_ret):
    try:
        res = check_volume_signal(symbol)
        if not res:
            return None

        side = res["side"]

        with STATE_LOCK:
            if side == "BUY" and not ALLOW_BUY:
                return None
            if side == "SELL" and not ALLOW_SELL:
                return None

        # Глобальные skip days.
        today = datetime.now(UTC).strftime("%A").lower()
        if today in GLOBAL_SKIP_DAYS:
            return None

        corr = get_correlation(symbol, btc_ret)

        if USE_CORREL_FILTER and corr is not None:
            mn = CORREL_MIN_BUY if side == "BUY" else CORREL_MIN_SELL
            mx = CORREL_MAX_BUY if side == "BUY" else CORREL_MAX_SELL
            if not apply_range_filter(corr, mn, mx):
                print(f"⏭ {symbol} {side}: global corr {corr} не прошёл")
                return None

        if not check_strategy_filters(side, res["natr"], res["delta_pct"], corr):
            print(f"⏭ {symbol} {side}: фильтры 12:4 не пройдены")
            return None

        candle_time = res["candle_time"]

        # Один сигнал на одну закрытую свечу.
        if LAST_SIGNAL_CANDLE.get(symbol) == candle_time:
            return None

        LAST_SIGNAL_CANDLE[symbol] = candle_time

        chart_path, tp, sl = make_signal_chart(res)
        caption = signal_caption(res, tp, sl, corr)
        send_signal_photo(chart_path, caption)

        print(
            f"🔥 SIGNAL {symbol} {side} | "
            f"entry={fmt_price(res['entry'])} "
            f"tp={fmt_price(tp)} sl={fmt_price(sl)} "
            f"vol=x{res['vol_ratio']:.2f} corr={corr}"
        )
        return res

    except Exception as e:
        print(f"Ошибка {symbol}: {e}")
        traceback.print_exc()
        return None


# ============================================================
# Main loop
# ============================================================

def main():
    print(
        f"✅ {BOT_NAME} | SIGNAL ONLY | "
        f"{STRATEGY_NAME} TP={STRAT_CFG['tp']*100:.1f}% "
        f"SL={STRAT_CFG['sl']*100:.1f}%"
    )
    print("🚫 Реальные ордера, баланс, плечо и закрытие позиций отключены.")

    if not BOT_TOKEN or not CHAT_ID:
        print("⚠️ BOT_TOKEN/CHAT_ID не найдены — сигналы будут строиться, "
              "но Telegram отправка не сработает.")

    Thread(target=telegram_commands, daemon=True).start()

    symbols = []
    last_symbols_refresh = 0
    last_heartbeat = 0

    send_message(
        f"🟢 <b>{BOT_NAME}</b> запущен\n"
        f"Режим: SIGNAL ONLY\n"
        f"Стратегия: {STRATEGY_NAME}\n"
        f"ТФ: 1H\n"
        f"TP: +{STRAT_CFG['tp']*100:.1f}% | SL: -{STRAT_CFG['sl']*100:.1f}%\n"
        f"Свечной график: matplotlib"
    )

    while True:
        try:
            now = time.time()

            if not symbols or now - last_symbols_refresh >= 3600:
                symbols = get_liquid_futures_symbols()
                last_symbols_refresh = now
                print(f"♻️ Ликвидных символов: {len(symbols)}")

            # Работаем только при появлении новой закрытой 1H свечи.
            btc_ret = get_btc_returns()

            # Ограничиваем параллелизм, чтобы не создавать лишнюю нагрузку
            # на Binance.
            with ThreadPoolExecutor(max_workers=6) as pool:
                futures = {
                    pool.submit(process_symbol, symbol, btc_ret): symbol
                    for symbol in symbols
                }
                for future in as_completed(futures):
                    symbol = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        print(f"Worker {symbol}: {e}")

            # Для определения новой свечи достаточно проверить BTC.
            try:
                btc_df = get_klines("BTCUSDT", 2)
                closed_time = int(btc_df.iloc[-2]["open_time"])
            except Exception:
                closed_time = None

            if closed_time is not None:
                # Если process_symbol получил старые данные, повторов всё равно
                # не будет благодаря LAST_SIGNAL_CANDLE.
                pass

            if time.time() - last_heartbeat > 6 * 3600:
                send_message(f"🟢 {BOT_NAME} работает. Мониторинг 1H сигналов активен.")
                last_heartbeat = time.time()

        except Exception as e:
            print(f"Ошибка main loop: {e}")
            traceback.print_exc()
            send_message(f"⚠️ {BOT_NAME}: ошибка цикла мониторинга\n{e}")

        # Полный проход достаточно делать раз в 60 секунд.
        # Внутри символ не выдаст повтор на той же закрытой свече.
        time.sleep(60)


if __name__ == "__main__":
    main()
