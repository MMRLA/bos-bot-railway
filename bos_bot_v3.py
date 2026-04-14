"""
IC Markets — EURUSD BOS Reversal Bot v6
Cambios clave respecto a v5:
  - FIX del amend SL/TP: enviar stopLoss / takeProfit en formato entero cTrader
  - Log detallado de ProtoOAOrderErrorEvent
  - No marcar sltp_sent=True de forma optimista tras enviar amend
  - FIX timeout auth cuenta: guardar/cancelar callLater correctamente
  - FIX reconnect: evitar reconnect timers duplicados
  - FIX logging Twisted: timeouts no fatales no ensucian como error crítico
  - Se preserva el resto del codigo y de la estrategia SIN CAMBIAR
"""

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, List, Dict

import numpy as np

from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAApplicationAuthReq,
    ProtoOAAccountAuthReq,
    ProtoOAGetAccountListByAccessTokenReq,
    ProtoOASubscribeSpotsReq,
    ProtoOANewOrderReq,
    ProtoOAClosePositionReq,
    ProtoOAReconcileReq,
    ProtoOAGetTrendbarsReq,
    ProtoOASymbolsListReq,
    ProtoOAAmendPositionSLTPReq,
)
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import (
    ProtoOAOrderType,
    ProtoOATradeSide,
    ProtoOATrendbarPeriod,
)
from twisted.internet import reactor

# =============================================================================
# CONFIGURACION
# =============================================================================

CLIENT_ID     = "13827_JP8vVXN0Fw7FEZBf3zRr5uaYbaDN4L8s842dKSVTvwwMIkM2n0"
CLIENT_SECRET = "oOIbm791Pb3eHQwLe5Rs8tBQGqag7v4mtSEPj1NIoL4rh9cpqc"
ACCESS_TOKEN  = "kWcPDvf-6FCoaTFiVCVPs5yH8V6_61uVNvaD4nfwS6Q"
ACCOUNT_ID    = 45971561
USE_DEMO      = True

SYMBOL_NAME   = "EURUSD"
DEFAULT_SYMBOL_ID = 1

BAR_SECONDS   = 3600
EMA_FAST_N    = 12
EMA_SLOW_N    = 48
ATR_PERIOD    = 14
ADX_PERIOD    = 14
SWING_LOOKBACK= 6
MIN_SWING_BP  = 6.0
ATR_MIN_BP    = 9.0
ADX_MIN_VAL   = 20.0
ATR_MULT      = 1.5
SL_MIN_BP     = 6.0
SL_MAX_BP     = 25.0
TP_RATIO      = 3.0
MAX_BARS_OPEN = 16
COOLDOWN_BARS = 2
RISK_PCT      = 0.005
LEVERAGE      = 30
MAX_MARGIN_PCT= 0.40
MAX_SPREAD_BP = 3.0

HISTORICAL_BARS = 500
HISTORICAL_LOOKBACK_DAYS = 14

HEARTBEAT_SECS = 30
ACCOUNT_AUTH_TIMEOUT_SECS = 10
USE_BROKER_H1_FOR_SIGNAL = False   # Fase 1: comparar solo, no usar aun para operar
BROKER_H1_FETCH_DELAY_SECS = 2.0   # Espera tras la hora en punto para que la vela cierre bien en broker
BROKER_H1_COMPARE_BARS = 200


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bos_bot_v6.log", encoding="utf-8"),
    ]
)
log = logging.getLogger("BOS_v6")



import json
import os

STATE_FILE = "/opt/bos-bot/data/state.json"
HISTORY_FILE = "/opt/bos-bot/data/history.jsonl"

def save_state():
    try:
        ind = compute_indicators() if state.is_warmed_up() else {}

        now_ts = now_utc_ts()
        secs_since_last_spot = None if state.last_spot_ts is None else now_ts - state.last_spot_ts
        secs_since_last_valid = None if state.last_valid_tick_ts is None else now_ts - state.last_valid_tick_ts

        side_txt = None
        if ind:
            if ind.get("ltf_side") == 1:
                side_txt = "UP"
            elif ind.get("ltf_side") == -1:
                side_txt = "DOWN"

        bot_status = "running"
        if secs_since_last_valid is None:
            bot_status = "starting"
        elif secs_since_last_valid > 180:
            bot_status = "disconnected"
        elif secs_since_last_valid > 60:
            bot_status = "stale_feed"
        elif secs_since_last_valid > 15:
            bot_status = "degraded"
        data = {
            "bot_status": bot_status,
            "account_id": ACCOUNT_ID,
            "use_demo": USE_DEMO,
            "symbol": SYMBOL_NAME,
            "equity": state.equity,
            "pnl_bp": state.pnl_bp_total,
            "trades_total": state.trades_total,
            "trades_win": state.trades_win,
            "trades_loss": state.trades_loss,
            "win_rate": (state.trades_win / max(state.trades_total, 1)) * 100.0,
            "open_trades_count": state.n_open_trades,
            "margin_used": state.total_margin_used,
            "margin_cap": state.equity * MAX_MARGIN_PCT,
            "bars": state.n_bars,
            "historical_loaded": state.historical_loaded,
            "current_bar_start": state.current_bar_start,
            "current_bar_valid_ticks": state.current_bar_valid_ticks,
            "last_price": state.last_mid,
            "bid": state.last_bid,
            "ask": state.last_ask,
            "spread": state.last_spread_bp,
            "last_spot_ts": state.last_spot_ts,
            "last_valid_tick_ts": state.last_valid_tick_ts,
            "last_spot_age_sec": secs_since_last_spot,
            "last_valid_tick_age_sec": secs_since_last_valid,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "indicators": {
                "atr_bp": ind.get("atr_bp") if ind else None,
                "adx": ind.get("adx") if ind else None,
                "swing_range_bp": ind.get("swing_range_bp") if ind else None,
                "side": side_txt,
            },
            "open_trades": [
                {
                    "id": t.position_id,
                    "side": t.side,
                    "entry": t.entry_price,
                    "sl": t.sl_price,
                    "tp": t.tp_price,
                    "units": t.units,
                    "entry_time": t.entry_time.isoformat() if t.entry_time else None,
                    "entry_bar_idx": t.entry_bar_idx,
                    "margin_used": t.margin_used,
                }
                for t in state.open_trades.values()
            ],
        }

        data = clean_nan(data)

        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, allow_nan=False)

    except Exception as e:
        log.warning(f"Error guardando state: {e}")


def append_history():
    try:
        ind = compute_indicators() if state.is_warmed_up() else {}

        row = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "pnl_bp": state.pnl_bp_total,
            "last_price": state.last_mid,
            "spread": state.last_spread_bp,
            "trades_total": state.trades_total,
            "open_trades": state.n_open_trades,
            "margin_used": state.total_margin_used,
            "atr_bp": ind.get("atr_bp") if ind else None,
            "adx": ind.get("adx") if ind else None,
            "swing_range_bp": ind.get("swing_range_bp") if ind else None,
            "side": ind.get("ltf_side") if ind else None,
        }

        row = clean_nan(row)

        with open(HISTORY_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, allow_nan=False) + "\n")

    except Exception as e:
        log.warning(f"Error guardando history: {e}")


# =============================================================================
# DATACLASSES
# =============================================================================

@dataclass
class OpenTrade:
    position_id:   int
    side:          str
    entry_price:   float
    sl_price:      float
    tp_price:      float
    sl_bp:         float
    tp_bp:         float
    units:         int
    entry_bar_idx: int
    margin_used:   float
    entry_time:    datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    sltp_sent:     bool = False

# =============================================================================
# ESTADO
# =============================================================================

class BotState:
    def __init__(self):
        self.bars: List[dict]                = []
        self.max_bars                        = 200

        self.current_bar: Optional[dict]     = None
        self.current_bar_start: Optional[int] = None
        self.current_bar_valid_ticks         = 0

        self.open_trades: Dict[int, OpenTrade] = {}
        self.last_bos_bar                    = -100

        self.equity                          = 2000.0
        self.symbol_id                       = DEFAULT_SYMBOL_ID
        self.symbol_resolved                 = False

        self.pip_size                        = 0.0001

        self.trades_total                    = 0
        self.trades_win                      = 0
        self.trades_loss                     = 0
        self.pnl_bp_total                    = 0.0

        self._pending: Optional[dict]        = None
        self.historical_loaded               = False

        # Feed / spots
        self.tick_count                      = 0
        self.raw_spot_count                  = 0
        self.spot_reject_spread              = 0
        self.spot_reject_not_ready           = 0
        self.spot_ts_fallback_count          = 0

        self.last_spot_ts: Optional[int]     = None
        self.last_valid_tick_ts: Optional[int] = None

        self.last_bid: Optional[float]       = None
        self.last_ask: Optional[float]       = None
        self.last_mid: Optional[float]       = None
        self.last_spread_bp: Optional[float] = None

        # Counters incrementales heartbeat
        self.hb_prev_raw_spots               = 0
        self.hb_prev_valid_ticks             = 0
        self.hb_prev_reject_spread           = 0
        self.hb_prev_reject_not_ready        = 0
        self.hb_prev_ts_fallbacks            = 0

        self.last_local_closed_bar: Optional[dict] = None
        self.pending_broker_h1_fetch = False
        self.last_broker_h1_bar_time: Optional[int] = None

    @property
    def n_bars(self):
        return len(self.bars)

    @property
    def bars_since_last_bos(self):
        return self.n_bars - self.last_bos_bar

    @property
    def n_open_trades(self):
        return len(self.open_trades)

    @property
    def total_margin_used(self):
        return sum(t.margin_used for t in self.open_trades.values())

    @property
    def margin_available(self):
        return max(0.0, self.equity * MAX_MARGIN_PCT - self.total_margin_used)

    def add_bar(self, bar: dict):
        self.bars.append(bar)
        if len(self.bars) > self.max_bars:
            self.bars.pop(0)

    def n_bars_needed(self):
        return max(EMA_SLOW_N, ADX_PERIOD * 3)

    def is_warmed_up(self):
        return self.n_bars >= self.n_bars_needed()

state = BotState()

# =============================================================================
# UTILS
# =============================================================================

def now_utc_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def fmt_dt_utc(ts: Optional[int]) -> str:
    if ts is None:
        return "NA"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def safe_float(x, default=float("nan")):
    try:
        return float(x)
    except Exception:
        return default

def clean_nan(obj):
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan(v) for v in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    return obj

def px_to_bp(diff, price):
    if price is None or price <= 0:
        return float("nan")
    return (diff / price) * 10_000.0

def bp_to_price(bp, price):
    if price is None or price <= 0:
        return float("nan")
    return price * bp / 10_000.0

def get_bar_start(ts):
    return (ts // BAR_SECONDS) * BAR_SECONDS

def normalize_spot_timestamp(raw_ts_ms: Optional[int]) -> int:
    """
    Convierte timestamp spot a segundos UTC.
    Si viene 0, invalido o fuera de rango, usa fallback con hora local UTC actual.
    """
    now_ts = now_utc_ts()

    try:
        if raw_ts_ms is None:
            state.spot_ts_fallback_count += 1
            return now_ts

        raw_ts_ms = int(raw_ts_ms)

        if raw_ts_ms <= 0:
            state.spot_ts_fallback_count += 1
            return now_ts

        ts = raw_ts_ms // 1000

        # Rango razonable: entre 2000-01-01 y ahora + 1h
        if ts < 946684800 or ts > now_ts + 3600:
            state.spot_ts_fallback_count += 1
            return now_ts

        return ts

    except Exception:
        state.spot_ts_fallback_count += 1
        return now_ts

# =============================================================================
# INDICADORES
# =============================================================================

def ema_series(values, span):
    if len(values) < span:
        return float("nan")
    k = 2.0 / (span + 1)
    ema = float(values[0])
    for v in values[1:]:
        ema = v * k + ema * (1 - k)
    return ema

def calc_atr(bars, period):
    if len(bars) < period + 1:
        return float("nan")

    trs = []
    for i in range(1, len(bars)):
        h, l, pc = bars[i]["high"], bars[i]["low"], bars[i-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))

    atr = float(np.mean(trs[:period]))
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr

def calc_adx(bars, period):
    if len(bars) < period * 2:
        return float("nan")

    plus_dms, minus_dms, trs = [], [], []

    for i in range(1, len(bars)):
        up = bars[i]["high"] - bars[i-1]["high"]
        down = bars[i-1]["low"] - bars[i]["low"]

        plus_dms.append(up if (up > down and up > 0) else 0.0)
        minus_dms.append(down if (down > up and down > 0) else 0.0)

        h, l, pc = bars[i]["high"], bars[i]["low"], bars[i-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))

    def ws(data, n):
        s = float(np.sum(data[:n]))
        result = [s]
        for v in data[n:]:
            s = s - s / n + v
            result.append(s)
        return result

    sm_tr = ws(trs, period)
    sm_p  = ws(plus_dms, period)
    sm_m  = ws(minus_dms, period)

    dxs = []
    for st, sp, sm in zip(sm_tr, sm_p, sm_m):
        if st < 1e-12:
            continue
        dip = 100 * sp / st
        dim = 100 * sm / st
        denom = dip + dim
        dxs.append(100 * abs(dip - dim) / denom if denom > 1e-12 else 0.0)

    if len(dxs) < period:
        return float("nan")

    adx = float(np.mean(dxs[:period]))
    for dx in dxs[period:]:
        adx = (adx * (period - 1) + dx) / period

    return adx

def compute_indicators():
    bars = state.bars
    n = len(bars)
    mid = bars[-1]["close"] if bars else 1.1
    nan = float("nan")

    r = {
        "ema_fast": nan,
        "ema_slow": nan,
        "ltf_side": 0,
        "atr_bp": nan,
        "adx": nan,
        "swing_hi": nan,
        "swing_lo": nan,
        "swing_range_bp": nan
    }

    if n < state.n_bars_needed():
        return r

    closes = [b["close"] for b in bars]
    r["ema_fast"] = ema_series(closes[-EMA_FAST_N:], EMA_FAST_N)
    r["ema_slow"] = ema_series(closes[-EMA_SLOW_N:], EMA_SLOW_N)

    if not math.isnan(r["ema_fast"]) and not math.isnan(r["ema_slow"]):
        r["ltf_side"] = 1 if r["ema_fast"] > r["ema_slow"] else -1

    atr_raw = calc_atr(bars[-(ATR_PERIOD + 1):], ATR_PERIOD)
    if not math.isnan(atr_raw):
        r["atr_bp"] = px_to_bp(atr_raw, mid)

    r["adx"] = calc_adx(bars[-(ADX_PERIOD * 3):], ADX_PERIOD)

    if n >= SWING_LOOKBACK + 1:
        prev = bars[-(SWING_LOOKBACK + 1):-1]
        r["swing_hi"] = max(b["high"] for b in prev)
        r["swing_lo"] = min(b["low"]  for b in prev)
        r["swing_range_bp"] = px_to_bp(r["swing_hi"] - r["swing_lo"], mid)

    return r

# =============================================================================
# LOGICA BOS (SIN CAMBIAR ESTRATEGIA)
# =============================================================================

def evaluate_bos_signal(ind, bar):
    close = bar["close"]

    for key in ["ema_fast", "ema_slow", "atr_bp", "adx", "swing_hi", "swing_lo", "swing_range_bp"]:
        if math.isnan(ind.get(key, float("nan"))):
            return None

    if ind["swing_range_bp"] < MIN_SWING_BP:
        return None
    if ind["atr_bp"] < ATR_MIN_BP:
        return None
    if ind["adx"] < ADX_MIN_VAL:
        return None
    if state.bars_since_last_bos < COOLDOWN_BARS:
        return None

    bar_dt = datetime.fromtimestamp(bar["time"], tz=timezone.utc)
    if bar_dt.weekday() == 6:
        return None
    if bar_dt.weekday() == 4 and bar_dt.hour >= 20:
        return None

    ltf = ind["ltf_side"]

    # Estrategia original: reversal del breakout
    if ltf == 1 and close > ind["swing_hi"]:
        log.info(f"  BOS ALCISTA: {close:.5f} > swing_hi={ind['swing_hi']:.5f}")
        return "sell"

    if ltf == -1 and close < ind["swing_lo"]:
        log.info(f"  BOS BAJISTA: {close:.5f} < swing_lo={ind['swing_lo']:.5f}")
        return "buy"

    return None

# =============================================================================
# RIESGO / SIZING / SLTP
# =============================================================================

def calc_sl_tp(side, entry, atr_bp):
    sl_bp = max(SL_MIN_BP, min(SL_MAX_BP, atr_bp * ATR_MULT))
    tp_bp = sl_bp * TP_RATIO

    sl_d = bp_to_price(sl_bp, entry)
    tp_d = bp_to_price(tp_bp, entry)

    if side == "sell":
        return entry + sl_d, entry - tp_d, sl_bp, tp_bp
    else:
        return entry - sl_d, entry + tp_d, sl_bp, tp_bp

def calc_units(sl_bp, entry_price):
    risk_eur = state.equity * RISK_PCT
    sl_return = sl_bp / 10_000.0

    if sl_return <= 0:
        return 0, 0.0

    units_float = risk_eur / sl_return
    units = max(1000, int(units_float / 1000) * 1000)

    margin = units * entry_price / LEVERAGE
    return units, margin

def execute_signal(signal, bar, ind, bot):
    entry = bar["ask"] if signal == "buy" else bar["bid"]

    sl_price, tp_price, sl_bp, tp_bp = calc_sl_tp(signal, entry, ind["atr_bp"])
    units, margin_needed = calc_units(sl_bp, entry)

    if units <= 0:
        log.warning("  SEÑAL OMITIDA: sizing invalido (units<=0)")
        return

    if margin_needed > state.margin_available:
        log.warning(
            f"  SEÑAL OMITIDA: margen insuficiente "
            f"(necesario={margin_needed:.0f}€, disponible={state.margin_available:.0f}€)"
        )
        return

    spread_bp = px_to_bp((bar["ask"] - bar["bid"]), bar["bid"])
    log.info(
        f"\n  *** {signal.upper()} PRE-ORDER | "
        f"EntryRef={entry:.5f} | Spread={spread_bp:.2f}bp | "
        f"SL={sl_price:.5f} ({sl_bp:.1f}bp) | "
        f"TP={tp_price:.5f} ({tp_bp:.1f}bp) | "
        f"Units={units:,} | Margen={margin_needed:.0f}€ ***"
    )

    state._pending = {
        "side": signal,
        "entry_price_ref": entry,
        "sl_bp": sl_bp,
        "tp_bp": tp_bp,
        "units": units,
        "margin": margin_needed,
        "entry_bar_idx": state.n_bars,
        "signal_bar_time": bar["time"],
        "signal_bid": bar["bid"],
        "signal_ask": bar["ask"],
    }

    state.last_bos_bar = state.n_bars
    bot.send_order(signal, units)

# =============================================================================
# BARRAS
# =============================================================================

def finalize_current_bar(bot):
    if state.current_bar is None or state.current_bar_start is None:
        return

    completed = dict(state.current_bar)
    if completed.get("time", 0) <= 0:
        log.warning("Barra invalida detectada; se omite el cierre.")
        return

    state.add_bar(completed)
    state.last_local_closed_bar = completed
    bar_dt = datetime.fromtimestamp(completed["time"], tz=timezone.utc)

    log.info(f"\n{'='*78}")
    log.info(f"BARRA CERRADA: {bar_dt.strftime('%Y-%m-%d %H:%M')} UTC")
    log.info(
        f"  O={completed['open']:.5f} H={completed['high']:.5f} "
        f"L={completed['low']:.5f} C={completed['close']:.5f} | "
        f"ticks_validos={state.current_bar_valid_ticks}"
    )
    log.info(
        f"  Barras acumuladas: {state.n_bars} | "
        f"Trades abiertos: {state.n_open_trades} | "
        f"PnL total: {state.pnl_bp_total:+.1f}bp"
    )

    if state.current_bar_valid_ticks <= 1:
        log.warning("  AVISO: la barra se ha construido con 0/1 ticks validos. Revisa feed/spread/symbolId.")

    on_bar_close(completed, bot)

def on_tick(bid, ask, ts, bot):
    state.tick_count += 1
    state.last_valid_tick_ts = ts

    mid = (bid + ask) / 2.0
    bar_start = get_bar_start(ts)

    if state.current_bar_start is None:
        state.current_bar_start = bar_start
        state.current_bar = {
            "time": bar_start,
            "open": mid,
            "high": mid,
            "low": mid,
            "close": mid,
            "bid": bid,
            "ask": ask,
        }
        state.current_bar_valid_ticks = 1
        return

    if bar_start < state.current_bar_start:
        return

    if bar_start == state.current_bar_start:
        cb = state.current_bar
        cb["high"] = max(cb["high"], mid)
        cb["low"]  = min(cb["low"], mid)
        cb["close"] = mid
        cb["bid"] = bid
        cb["ask"] = ask
        state.current_bar_valid_ticks += 1
    else:
        finalize_current_bar(bot)

        state.current_bar_start = bar_start
        state.current_bar = {
            "time": bar_start,
            "open": mid,
            "high": mid,
            "low": mid,
            "close": mid,
            "bid": bid,
            "ask": ask,
        }
        state.current_bar_valid_ticks = 1

def on_bar_close(bar, bot):
    to_close = []
    for pid, trade in state.open_trades.items():
        bars_open = state.n_bars - trade.entry_bar_idx
        if bars_open >= MAX_BARS_OPEN:
            log.warning(f"  Trade {pid}: horizonte max ({MAX_BARS_OPEN}h) — cerrando")
            to_close.append(pid)

    for pid in to_close:
        bot.close_position(pid)

    needed = state.n_bars_needed()
    if not state.is_warmed_up():
        faltan = needed - state.n_bars
        log.info(f"  Calentando: {state.n_bars}/{needed} barras (faltan {faltan} barras = {faltan}h)")
        return

    ind = compute_indicators()
    side_str = "▲UP" if ind["ltf_side"] == 1 else "▼DOWN"
    log.info(
        f"  EMA_f={ind['ema_fast']:.5f} EMA_s={ind['ema_slow']:.5f} {side_str} | "
        f"ATR={ind['atr_bp']:.1f}bp | ADX={ind['adx']:.1f} | "
        f"Swing [{ind['swing_lo']:.5f}-{ind['swing_hi']:.5f}] ({ind['swing_range_bp']:.1f}bp)"
    )

    signal = evaluate_bos_signal(ind, bar)
    if signal:
        execute_signal(signal, bar, ind, bot)
    else:
        reasons = []
        if not math.isnan(ind["atr_bp"]) and ind["atr_bp"] < ATR_MIN_BP:
            reasons.append(f"ATR={ind['atr_bp']:.1f}<{ATR_MIN_BP}")
        if not math.isnan(ind["adx"]) and ind["adx"] < ADX_MIN_VAL:
            reasons.append(f"ADX={ind['adx']:.1f}<{ADX_MIN_VAL}")
        if not math.isnan(ind["swing_range_bp"]) and ind["swing_range_bp"] < MIN_SWING_BP:
            reasons.append(f"swing={ind['swing_range_bp']:.1f}bp<{MIN_SWING_BP}")
        if state.bars_since_last_bos < COOLDOWN_BARS:
            reasons.append("cooldown")
        if not reasons:
            reasons.append("sin BOS")

        log.info(f"  Sin señal: {' | '.join(reasons)}")

    wr = state.trades_win / max(state.trades_total, 1) * 100
    log.info(
        f"  Margen: {state.total_margin_used:.0f}€/{state.equity * MAX_MARGIN_PCT:.0f}€ | "
        f"Stats: {state.trades_total}T WR={wr:.0f}%"
    )

def parse_trendbars_from_response(res):
    loaded = []
    for tb in list(res.trendbar):
        d = 100_000

        low_abs = getattr(tb, "low", None)
        delta_open = getattr(tb, "deltaOpen", 0) or 0
        delta_high = getattr(tb, "deltaHigh", 0) or 0
        delta_close = getattr(tb, "deltaClose", None)

        if low_abs is not None and low_abs > 0 and delta_close is not None:
            low = low_abs / d
            open_ = low + delta_open / d
            high = low + delta_high / d
            close = low + delta_close / d
        else:
            close = getattr(tb, "close", 0) / d if getattr(tb, "close", 0) else 0
            if close <= 0:
                continue
            open_ = close + delta_open / d
            high = close + delta_high / d
            low = close + (getattr(tb, "deltaLow", 0) or 0) / d

        if close <= 0:
            continue

        high = max(high, open_, close, low)
        low = min(low, open_, close, high)

        loaded.append({
            "time": tb.utcTimestampInMinutes * 60,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "bid": close,
            "ask": close,
        })

    loaded.sort(key=lambda x: x["time"])

    dedup = []
    seen = set()
    for bar in loaded:
        if bar["time"] not in seen:
            dedup.append(bar)
            seen.add(bar["time"])

    return dedup

# =============================================================================
# CLIENTE CTRADER
# =============================================================================

class BosBot:
    def __init__(self):
        host = EndPoints.PROTOBUF_DEMO_HOST if USE_DEMO else EndPoints.PROTOBUF_LIVE_HOST
        self.client = Client(host, EndPoints.PROTOBUF_PORT, TcpProtocol)
        self.client.setConnectedCallback(self._on_connected)
        self.client.setDisconnectedCallback(self._on_disconnected)
        self.client.setMessageReceivedCallback(self._on_message)

        self.account_auth_ok = False
        self.app_auth_ok = False
        self.heartbeat_started = False
        self.hourly_timer_started = False
        self.ready_for_ticks = False

        self.last_bid = None
        self.last_ask = None
        self.last_ts = None

        self.account_auth_timeout_call = None
        self.reconnect_call = None
        self.shutting_down = False
        self.manual_disconnect_in_progress=False

        log.info(f"Bot v6 inicializado | Servidor: {host}")

    def _connect(self):
        self.client.startService()

    def _cancel_account_auth_timeout(self):
        if self.account_auth_timeout_call is not None and self.account_auth_timeout_call.active():
            self.account_auth_timeout_call.cancel()
        self.account_auth_timeout_call = None

    def _cancel_reconnect_call(self):
        if self.reconnect_call is not None and self.reconnect_call.active():
            self.reconnect_call.cancel()
        self.reconnect_call = None

    def _reset_market_state(self):
        state.bars = []
        state.current_bar = None
        state.current_bar_start = None
        state.current_bar_valid_ticks = 0

        state.historical_loaded = False

        state.tick_count = 0
        state.raw_spot_count = 0
        state.spot_reject_spread = 0
        state.spot_reject_not_ready = 0
        state.spot_ts_fallback_count = 0

        state.last_spot_ts = None
        state.last_valid_tick_ts = None
        state.last_bid = None
        state.last_ask = None
        state.last_mid = None
        state.last_spread_bp = None

        state.hb_prev_raw_spots = 0
        state.hb_prev_valid_ticks = 0
        state.hb_prev_reject_spread = 0
        state.hb_prev_reject_not_ready = 0
        state.hb_prev_ts_fallbacks = 0

        self.ready_for_ticks = False
        self.last_bid = None
        self.last_ask = None
        self.last_ts = None

    def _seed_current_bar_after_historical(self, fallback_close):
        now_ts = self.last_ts if self.last_ts is not None else now_utc_ts()
        bar_start = get_bar_start(now_ts)

        if self.last_bid is not None and self.last_ask is not None:
            bid = self.last_bid
            ask = self.last_ask
            mid = (bid + ask) / 2.0
        else:
            bid = fallback_close
            ask = fallback_close
            mid = fallback_close

        state.current_bar_start = bar_start
        state.current_bar = {
            "time": bar_start,
            "open": mid,
            "high": mid,
            "low": mid,
            "close": mid,
            "bid": bid,
            "ask": ask,
        }
        state.current_bar_valid_ticks = 0

    def _schedule_hourly_bar_timer(self):
        if self.hourly_timer_started:
            return

        self.hourly_timer_started = True

        def schedule_next():
            now_ts = now_utc_ts()
            secs_to_next = BAR_SECONDS - (now_ts % BAR_SECONDS)
            if secs_to_next <= 0:
                secs_to_next = BAR_SECONDS
            reactor.callLater(secs_to_next + 0.2, hourly_check)

        def hourly_check():
            try:
                self._on_hour_boundary()
            finally:
                schedule_next()

        schedule_next()
        log.info("Timer horario UTC activo para cierre de barra H1.")

    def _on_hour_boundary(self):
        if not self.ready_for_ticks:
            return
        if state.current_bar is None or state.current_bar_start is None:
            return

        now_ts = now_utc_ts()
        current_hour_start = get_bar_start(now_ts)

        if state.current_bar_start < current_hour_start:
            finalize_current_bar(self)

            if self.last_bid is not None and self.last_ask is not None:
                bid = self.last_bid
                ask = self.last_ask
                mid = (bid + ask) / 2.0
            else:
                mid = state.current_bar["close"]
                bid = state.current_bar["bid"]
                ask = state.current_bar["ask"]

            state.current_bar_start = current_hour_start
            state.current_bar = {
                "time": current_hour_start,
                "open": mid,
                "high": mid,
                "low": mid,
                "close": mid,
                "bid": bid,
                "ask": ask,
            }
            state.current_bar_valid_ticks = 0
            log.info("Timer UTC: cierre/rollover horario H1 ejecutado.")
            reactor.callLater(BROKER_H1_FETCH_DELAY_SECS, self._fetch_broker_h1_after_close)

    def _on_connected(self, client):
        log.info("Conectado. Autenticando aplicacion...")

        self.app_auth_ok = False
        self.account_auth_ok = False
        self._cancel_reconnect_call()
        self._cancel_account_auth_timeout()

        req = ProtoOAApplicationAuthReq()
        req.clientId = CLIENT_ID
        req.clientSecret = CLIENT_SECRET
        client.send(req).addErrback(self._on_error)

    def _on_disconnected(self, client, reason):
        self.ready_for_ticks = False
        self.app_auth_ok = False
        self.account_auth_ok = False
        self._cancel_account_auth_timeout()

        log.warning(f"Desconectado: {reason}.")

        if self.manual_disconnect_in_progress:
            log.warning("Desconexion provocada por reinicio controlado. No se agenda reconnect duplicado.")
            self.manual_disconnect_in_progress = False
            return

        log.warning("Reconectando en 15s...")

        if not self.shutting_down:
            if self.reconnect_call is None or not self.reconnect_call.active():
                self.reconnect_call = reactor.callLater(15, self._connect)

    def _on_error(self, failure):
        try:
            failure_type = failure.type.__name__ if getattr(failure, "type", None) else str(type(failure))
            failure_msg = failure.getErrorMessage() if hasattr(failure, "getErrorMessage") else str(failure)
        except Exception:
            failure_type = "Unknown"
            failure_msg = str(failure)

        if "TimeoutError" in failure_type or "TimeoutError" in failure_msg:
            log.warning(f"Twisted timeout no fatal: {failure_msg}")
            return

        log.error(f"Error Twisted: {failure}")

    def _safe_extract(self, message):
        try:
            return Protobuf.extract(message)
        except Exception as e:
            log.warning(f"No se pudo extraer payloadType={getattr(message, 'payloadType', 'NA')}: {e}")
            return None

    def _obj_name(self, obj):
        return obj.__class__.__name__ if obj is not None else "Unknown"

    def _extract_account_ids(self, res):
        account_ids = []

        for attr in ("ctidTraderAccountId", "accountId", "accountIds"):
            if hasattr(res, attr):
                try:
                    vals = list(getattr(res, attr))
                    for v in vals:
                        try:
                            account_ids.append(int(v))
                        except Exception:
                            pass
                except Exception:
                    pass

        for attr in ("ctidTraderAccount", "traderAccount", "account", "accounts"):
            if hasattr(res, attr):
                try:
                    items = list(getattr(res, attr))
                except Exception:
                    items = []
                for item in items:
                    for key in ("ctidTraderAccountId", "accountId"):
                        if hasattr(item, key):
                            try:
                                account_ids.append(int(getattr(item, key)))
                                break
                            except Exception:
                                pass

        unique_ids = []
        seen = set()
        for x in account_ids:
            if x not in seen:
                unique_ids.append(x)
                seen.add(x)
        return unique_ids

    def _request_account_list(self):
        log.info("App autenticada. Solicitando cuentas asociadas al token...")
        req = ProtoOAGetAccountListByAccessTokenReq()
        req.accessToken = ACCESS_TOKEN
        self.client.send(req).addErrback(self._on_error)

    def _authenticate_account(self):
        log.info(f"Autenticando cuenta {ACCOUNT_ID}...")
        self.account_auth_ok = False
        self._cancel_account_auth_timeout()

        req = ProtoOAAccountAuthReq()
        req.ctidTraderAccountId = ACCOUNT_ID
        req.accessToken = ACCESS_TOKEN
        self.client.send(req).addErrback(self._on_error)

        self.account_auth_timeout_call = reactor.callLater(
            ACCOUNT_AUTH_TIMEOUT_SECS,
            self._check_account_auth_timeout
        )

    def _check_account_auth_timeout(self):
        self.account_auth_timeout_call = None
        if not self.account_auth_ok:
            log.error("Timeout esperando autenticacion de cuenta. Revisa ACCESS_TOKEN, ACCOUNT_ID y asociacion token-cuenta.")

    def _request_symbols_list(self):
        req = ProtoOASymbolsListReq()
        req.ctidTraderAccountId = ACCOUNT_ID
        req.includeArchivedSymbols = False
        self.client.send(req).addErrback(self._on_error)
        log.info(f"Solicitando lista de simbolos para resolver {SYMBOL_NAME}...")

    def _extract_symbol_text_candidates(self, sym):
        texts = []
        for attr in (
            "symbolName", "symbol", "name", "displayName",
            "description", "shortName", "title"
        ):
            if hasattr(sym, attr):
                val = getattr(sym, attr)
                if isinstance(val, str) and val.strip():
                    texts.append(val.strip())
        return texts

    def _resolve_symbol_from_response(self, res):
        symbols = []
        try:
            symbols = list(res.symbol)
        except Exception:
            symbols = []

        target = SYMBOL_NAME.upper().replace("/", "")

        exact_matches = []
        loose_matches = []

        for sym in symbols:
            symbol_id = getattr(sym, "symbolId", None)
            texts = self._extract_symbol_text_candidates(sym)

            norm_texts = [t.upper().replace("/", "").replace("_", "").replace("-", "") for t in texts]
            for nt, raw in zip(norm_texts, texts):
                if nt == target:
                    exact_matches.append((symbol_id, raw))
                elif target in nt:
                    loose_matches.append((symbol_id, raw))

        if exact_matches:
            sid, raw = exact_matches[0]
            return int(sid), raw

        if loose_matches:
            sid, raw = loose_matches[0]
            return int(sid), raw

        return None, None

    def _on_message(self, client, message):
        t = message.payloadType
        obj = self._safe_extract(message)
        obj_name = self._obj_name(obj)

        if not (t == 2131 or obj_name == "ProtoOASpotEvent"):
            if obj_name not in ("Unknown",):
                log.info(f"[MSG] payloadType={t} type={obj_name}")

        if t == 50 or obj_name == "ProtoOAErrorRes":
            log.error(
                "Error servidor | "
                f"payloadType={t} | "
                f"errorCode={getattr(obj, 'errorCode', None)} | "
                f"description={getattr(obj, 'description', None)} | "
                f"accountId={getattr(obj, 'ctidTraderAccountId', None)}"
            )
            return

        if t == 2132 or obj_name == "ProtoOAOrderErrorEvent":
            log.error(
                "OrderErrorEvent | "
                f"errorCode={getattr(obj, 'errorCode', None)} | "
                f"description={getattr(obj, 'description', None)} | "
                f"orderId={getattr(obj, 'orderId', None)} | "
                f"positionId={getattr(obj, 'positionId', None)} | "
                f"accountId={getattr(obj, 'ctidTraderAccountId', None)}"
            )
            return

        if t == 2164 or obj_name == "ProtoOAAccountDisconnectEvent":
            log.warning(
                "AccountDisconnectEvent | "
                f"accountId={getattr(obj, 'ctidTraderAccountId', None)} | "
                f"reason={getattr(obj, 'reason', None)}"
            )

            self.ready_for_ticks = False
            self.account_auth_ok = False
            self.app_auth_ok = False
            self._cancel_account_auth_timeout()
            self._reset_market_state()

            self._cancel_reconnect_call()
            self.manual_disconnect_in_progress = True

            try:
                self.client.stopService()
            except Exception as e:
                log.warning(f"No se pudo parar client tras AccountDisconnectEvent: {e}")
                self.manual_disconnect_in_progress = False

            log.warning("Reiniciando conexion completa en 5s tras AccountDisconnectEvent...")
            self.reconnect_call = reactor.callLater(5, self._connect)
            return

        if t == 2101 or obj_name == "ProtoOAApplicationAuthRes":
            self.app_auth_ok = True
            self._request_account_list()
            return

        if obj_name == "ProtoOAGetAccountListByAccessTokenRes":
            account_ids = self._extract_account_ids(obj)
            if account_ids:
                log.info(f"Cuentas asociadas al token: {account_ids}")
                if ACCOUNT_ID not in account_ids:
                    log.error(f"El ACCOUNT_ID={ACCOUNT_ID} no aparece entre las cuentas del token.")
                    log.error("Corrige ACCOUNT_ID o genera un ACCESS_TOKEN de la cuenta correcta.")
                    return
            else:
                log.warning("No se pudieron extraer cuentas del token desde la respuesta. Intentando autenticar la cuenta configurada igualmente.")

            reactor.callLater(0.5, self._authenticate_account)
            return

        if t == 2103 or obj_name == "ProtoOAAccountAuthRes":
            self.account_auth_ok = True
            self._cancel_account_auth_timeout()
            self._reset_market_state()
            log.info(f"Cuenta {ACCOUNT_ID} autenticada correctamente.")

            self._request_symbols_list()

            if not self.heartbeat_started:
                self._start_heartbeat()
                self.heartbeat_started = True

            if not self.hourly_timer_started:
                self._schedule_hourly_bar_timer()

            return

        if obj_name == "ProtoOASymbolsListRes":
            sid, raw = self._resolve_symbol_from_response(obj)
            if sid is not None:
                state.symbol_id = sid
                state.symbol_resolved = True
                log.info(f"Symbol resuelto: {SYMBOL_NAME} -> symbolId={sid} (match='{raw}')")
            else:
                state.symbol_id = DEFAULT_SYMBOL_ID
                state.symbol_resolved = False
                log.warning(
                    f"No se pudo resolver {SYMBOL_NAME} por nombre. "
                    f"Usando fallback symbolId={DEFAULT_SYMBOL_ID}. "
                    f"Si no llegan spots/historico, revisa este punto."
                )

            self._reconcile()
            self._load_historical_bars()
            self._subscribe_spots()
            return

        if t == 2138 or obj_name == "ProtoOAGetTrendbarsRes":
            self._on_historical_bars(message)
            return

        if t == 2131 or obj_name == "ProtoOASpotEvent":
            self._on_spot(message)
            return

        if t == 2126 or obj_name == "ProtoOAExecutionEvent":
            self._on_execution(message)
            return

        if t == 2125 or obj_name == "ProtoOAReconcileRes":
            self._on_reconcile(message)
            return

    def _start_heartbeat(self):
        def heartbeat():
            now = datetime.now(timezone.utc).strftime("%H:%M:%S")
            needed = state.n_bars_needed()

            hb_raw = state.raw_spot_count - state.hb_prev_raw_spots
            hb_valid = state.tick_count - state.hb_prev_valid_ticks
            hb_rej_spread = state.spot_reject_spread - state.hb_prev_reject_spread
            hb_rej_not_ready = state.spot_reject_not_ready - state.hb_prev_reject_not_ready
            hb_ts_fallbacks = state.spot_ts_fallback_count - state.hb_prev_ts_fallbacks

            state.hb_prev_raw_spots = state.raw_spot_count
            state.hb_prev_valid_ticks = state.tick_count
            state.hb_prev_reject_spread = state.spot_reject_spread
            state.hb_prev_reject_not_ready = state.spot_reject_not_ready
            state.hb_prev_ts_fallbacks = state.spot_ts_fallback_count

            now_ts = now_utc_ts()
            secs_since_last_spot = None if state.last_spot_ts is None else now_ts - state.last_spot_ts
            secs_since_last_valid = None if state.last_valid_tick_ts is None else now_ts - state.last_valid_tick_ts

            prox_bar_txt = ""
            if state.current_bar_start is not None:
                prox = state.current_bar_start + BAR_SECONDS
                secs_left = prox - now_ts
                prox_bar_txt = f"{max(0, secs_left//60)}m{max(0, secs_left%60)}s"

            mid_live = state.last_mid if state.last_mid is not None else 0.0
            bid_live = state.last_bid if state.last_bid is not None else 0.0
            ask_live = state.last_ask if state.last_ask is not None else 0.0
            spread_live = state.last_spread_bp if state.last_spread_bp is not None else float("nan")

            if state.current_bar is not None:
                cb = state.current_bar
                bar_txt = (
                    f"O={cb['open']:.5f} H={cb['high']:.5f} "
                    f"L={cb['low']:.5f} C={cb['close']:.5f} "
                    f"| ticks_bar={state.current_bar_valid_ticks}"
                )
                bar_age = now_ts - state.current_bar_start
                bar_age_txt = f"{bar_age//60}m{bar_age%60}s"
            else:
                bar_txt = "sin barra actual"
                bar_age_txt = "NA"

            flags = []
            if secs_since_last_spot is not None and secs_since_last_spot > 10:
                flags.append(f"ultimo_spot_hace_{secs_since_last_spot}s")
            if secs_since_last_valid is not None and secs_since_last_valid > 10:
                flags.append(f"ultimo_tick_valido_hace_{secs_since_last_valid}s")
            if not math.isnan(spread_live) and spread_live > MAX_SPREAD_BP:
                flags.append(f"spread_alto={spread_live:.2f}bp")
            if hb_ts_fallbacks > 0:
                flags.append(f"ts_fallbacks(+30s)={hb_ts_fallbacks}")

            log.info(
                f"[{now}] FEED | bid={bid_live:.5f} ask={ask_live:.5f} mid={mid_live:.5f} "
                f"| spread={spread_live:.2f}bp | raw_spots(+30s)={hb_raw} "
                f"| valid_ticks(+30s)={hb_valid} | rej_spread(+30s)={hb_rej_spread} "
                f"| rej_not_ready(+30s)={hb_rej_not_ready} | ts_fallbacks_total={state.spot_ts_fallback_count} "
                f"| last_spot={secs_since_last_spot if secs_since_last_spot is not None else 'NA'}s "
                f"| last_valid={secs_since_last_valid if secs_since_last_valid is not None else 'NA'}s"
            )

            log.info(
                f"[{now}] BARRA | start={fmt_dt_utc(state.current_bar_start)} | age={bar_age_txt} "
                f"| prox_cierre={prox_bar_txt} | {bar_txt}"
            )

            if state.is_warmed_up():
                ind = compute_indicators()
                side_txt = "UP" if ind["ltf_side"] == 1 else "DOWN"
                wr = state.trades_win / max(state.trades_total, 1) * 100
                log.info(
                    f"[{now}] SISTEMA | READY | bars={state.n_bars} | trades={state.n_open_trades} "
                    f"| pnl={state.pnl_bp_total:+.1f}bp | margin={state.total_margin_used:.0f}/{state.equity*MAX_MARGIN_PCT:.0f}€ "
                    f"| ATR={ind['atr_bp']:.1f}bp | ADX={ind['adx']:.1f} | swing={ind['swing_range_bp']:.1f}bp "
                    f"| side={side_txt} | WR={wr:.0f}%"
                )
            else:
                faltan = needed - state.n_bars
                log.info(
                    f"[{now}] SISTEMA | CALENTANDO {state.n_bars}/{needed} | faltan={faltan} barras "
                    f"| trades={state.n_open_trades} | pnl={state.pnl_bp_total:+.1f}bp"
                )

            if flags:
                log.warning(f"[{now}] AVISO_FEED | {' | '.join(flags)}")

            # guardar estado para dashboard
            save_state()
            append_history()

            # Reconcile periodico para capturar cierres SL/TP no detectados por execution event
            self._reconcile()

            reactor.callLater(HEARTBEAT_SECS, heartbeat)

        reactor.callLater(HEARTBEAT_SECS, heartbeat)
        log.info(f"Heartbeat activo (cada {HEARTBEAT_SECS}s)")

    def _load_historical_bars(self):
        try:
            req = ProtoOAGetTrendbarsReq()
            req.ctidTraderAccountId = ACCOUNT_ID
            req.symbolId = state.symbol_id
            req.period = ProtoOATrendbarPeriod.H1
            req.count = HISTORICAL_BARS

            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            from_ms = now_ms - HISTORICAL_LOOKBACK_DAYS * 24 * 3600 * 1000

            req.fromTimestamp = from_ms
            req.toTimestamp = now_ms

            self.client.send(req).addErrback(self._on_error)

            log.info(
                f"Solicitando historico H1 | symbolId={state.symbol_id} | "
                f"from={from_ms} to={now_ms} | lookback={HISTORICAL_LOOKBACK_DAYS} dias | count={HISTORICAL_BARS}"
            )
        except Exception as e:
            log.warning(f"Precarga no disponible: {e}. El bot calentara con datos en vivo (~{state.n_bars_needed()}h).")

    def _fetch_broker_h1_after_close(self):
        try:
            req = ProtoOAGetTrendbarsReq()
            req.ctidTraderAccountId = ACCOUNT_ID
            req.symbolId = state.symbol_id
            req.period = ProtoOATrendbarPeriod.H1
            req.count = BROKER_H1_COMPARE_BARS

            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            from_ms = now_ms - HISTORICAL_LOOKBACK_DAYS * 24 * 3600 * 1000

            req.fromTimestamp = from_ms
            req.toTimestamp = now_ms

            state.pending_broker_h1_fetch = True

            self.client.send(req).addErrback(self._on_error)

            log.info(
                f"Solicitando H1 broker post-cierre | symbolId={state.symbol_id} | "
                f"delay={BROKER_H1_FETCH_DELAY_SECS}s | count={BROKER_H1_COMPARE_BARS}"
            )
        except Exception as e:
            state.pending_broker_h1_fetch = False
            log.warning(f"No se pudo pedir H1 broker post-cierre: {e}")

    def _compare_local_vs_broker_bar(self, broker_bar):
        local_bar = state.last_local_closed_bar
        if local_bar is None or broker_bar is None:
            return

        if local_bar["time"] != broker_bar["time"]:
            log.warning(
                f"COMPARE H1 | tiempos distintos | "
                f"local={fmt_dt_utc(local_bar['time'])} | broker={fmt_dt_utc(broker_bar['time'])}"
            )
            return

        def diff_bp(a, b):
            mid = broker_bar["close"] if broker_bar["close"] > 0 else 1.0
            return px_to_bp(a - b, mid)

        log.info(
            "COMPARE H1 | "
            f"time={fmt_dt_utc(broker_bar['time'])} | "
            f"dO={diff_bp(local_bar['open'], broker_bar['open']):+.3f}bp | "
            f"dH={diff_bp(local_bar['high'], broker_bar['high']):+.3f}bp | "
            f"dL={diff_bp(local_bar['low'], broker_bar['low']):+.3f}bp | "
            f"dC={diff_bp(local_bar['close'], broker_bar['close']):+.3f}bp"
        )

    def _on_historical_bars(self, message):
        try:
            res = Protobuf.extract(message)
            bars_data = list(res.trendbar)
            if not bars_data:
                log.warning("El servidor no devolvio barras historicas.")
                return

            dedup = parse_trendbars_from_response(res)

            if not dedup:
                log.warning("No se pudieron parsear barras historicas validas.")
                return

            # -------------------------------------------------------------
            # CASO 1: fetch horario post-cierre para comparar barra broker
            # -------------------------------------------------------------
            if state.pending_broker_h1_fetch:
                state.pending_broker_h1_fetch = False

                broker_last_closed = dedup[-1]
                state.last_broker_h1_bar_time = broker_last_closed["time"]

                log.info(
                    f"H1 broker post-cierre recibida | "
                    f"time={fmt_dt_utc(broker_last_closed['time'])} | "
                    f"O={broker_last_closed['open']:.5f} H={broker_last_closed['high']:.5f} "
                    f"L={broker_last_closed['low']:.5f} C={broker_last_closed['close']:.5f}"
                )

                self._compare_local_vs_broker_bar(broker_last_closed)
                return

            # -------------------------------------------------------------
            # CASO 2: carga inicial / precarga historica normal
            # -------------------------------------------------------------
            state.bars = []
            for bar in dedup[-state.max_bars:]:
                state.add_bar(bar)

            state.historical_loaded = True

            fallback_close = dedup[-1]["close"]
            self._seed_current_bar_after_historical(fallback_close)
            self.ready_for_ticks = True

            needed = state.n_bars_needed()
            log.info(f"Precarga completada: {len(dedup)} barras H1 cargadas.")

            if len(dedup) >= 2:
                log.info(
                    f"Rango historico cargado: "
                    f"{fmt_dt_utc(dedup[0]['time'])} -> {fmt_dt_utc(dedup[-1]['time'])}"
                )

            if state.is_warmed_up():
                log.info("INDICADORES LISTOS. El bot puede operar desde la proxima barra.")
                ind = compute_indicators()
                if not math.isnan(ind.get("atr_bp", float("nan"))):
                    log.info(
                        f"  Estado actual: ATR={ind['atr_bp']:.1f}bp | ADX={ind['adx']:.1f} | "
                        f"Lado={'UP' if ind['ltf_side']==1 else 'DOWN'} | "
                        f"Swing={ind['swing_range_bp']:.1f}bp"
                    )
            else:
                faltan = needed - state.n_bars
                log.info(
                    f"Calentamiento parcial: {state.n_bars}/{needed}. "
                    f"Faltan {faltan} barras (~{faltan}h con datos en vivo)."
                )

        except Exception as e:
            state.pending_broker_h1_fetch = False
            log.error(f"Error procesando barras historicas: {e}", exc_info=True)

    def _reconcile(self):
        req = ProtoOAReconcileReq()
        req.ctidTraderAccountId = ACCOUNT_ID
        self.client.send(req).addErrback(self._on_error)

    def _subscribe_spots(self):
        req = ProtoOASubscribeSpotsReq()
        req.ctidTraderAccountId = ACCOUNT_ID
        req.symbolId.append(state.symbol_id)
        self.client.send(req).addErrback(self._on_error)
        log.info(f"Suscrito a {SYMBOL_NAME} (symbolId={state.symbol_id}, resolved={state.symbol_resolved})")

    def _on_spot(self, message):
        spot = Protobuf.extract(message)
        if spot.symbolId != state.symbol_id:
            return

        d = 100_000
        bid = spot.bid / d
        ask = spot.ask / d

        if bid <= 0 or ask <= 0:
            return

        raw_ts_ms = getattr(spot, "timestamp", None)
        ts = normalize_spot_timestamp(raw_ts_ms)

        spread_bp = px_to_bp(ask - bid, bid)
        mid = (bid + ask) / 2.0

        state.raw_spot_count += 1
        state.last_spot_ts = ts
        state.last_bid = bid
        state.last_ask = ask
        state.last_mid = mid
        state.last_spread_bp = spread_bp

        self.last_bid = bid
        self.last_ask = ask
        self.last_ts = ts

        if spread_bp > MAX_SPREAD_BP:
            state.spot_reject_spread += 1
            return

        if not self.ready_for_ticks:
            state.spot_reject_not_ready += 1
            return

        on_tick(bid, ask, ts, self)
        if state.tick_count % 20 == 0:
            save_state()


    def _finalize_trade_close(self, pos_id, close_price, reason="unknown"):
        if pos_id not in state.open_trades:
            return

        trade = state.open_trades.pop(pos_id)

        cp = trade.entry_price
        if close_price is not None and not math.isnan(close_price) and close_price > 0:
            cp = close_price

        pnl_bp = (
            ((trade.entry_price - cp) if trade.side == "sell" else (cp - trade.entry_price))
            / trade.entry_price
        ) * 10_000.0

        state.pnl_bp_total += pnl_bp

        if pnl_bp > 0:
            state.trades_win += 1
        else:
            state.trades_loss += 1

        wr = state.trades_win / max(state.trades_total, 1) * 100

        log.info(
            f"  TRADE CERRADO ID={pos_id} {trade.side.upper()} | reason={reason} | "
            f"close={cp:.5f} | P&L={pnl_bp:+.1f}bp | WR={wr:.0f}% | "
            f"Total={state.pnl_bp_total:+.1f}bp | Activos: {state.n_open_trades}"
        )

        # Update inmediato dashboard
        save_state()
        append_history()

    def _on_execution(self, message):
        ev = Protobuf.extract(message)
        pos = ev.position if hasattr(ev, "position") else None
        if not pos:
            return

        pos_id = pos.positionId
        pos_status = getattr(pos, "positionStatus", None)
        actual_price = safe_float(getattr(pos, "price", None), default=float("nan"))

        pos_volume = safe_float(getattr(pos, "volume", None), default=float("nan"))
        execution_type = getattr(ev, "executionType", None)

        log.info(
            f"  EXEC_EVENT | pos_id={pos_id} | pos_status={pos_status} | "
            f"price={actual_price} | volume={pos_volume} | execution_type={execution_type}"
        )

        if pos_status == 1 and state._pending:
            p = state._pending

            fill_price = p["entry_price_ref"]
            if not math.isnan(actual_price) and actual_price > 0:
                fill_price = actual_price

            sl_bp = p["sl_bp"]
            tp_bp = p["tp_bp"]
            sl_d = bp_to_price(sl_bp, fill_price)
            tp_d = bp_to_price(tp_bp, fill_price)

            if p["side"] == "sell":
                sl_price = fill_price + sl_d
                tp_price = fill_price - tp_d
            else:
                sl_price = fill_price - sl_d
                tp_price = fill_price + tp_d

            trade = OpenTrade(
                position_id=pos_id,
                side=p["side"],
                entry_price=fill_price,
                sl_price=sl_price,
                tp_price=tp_price,
                sl_bp=sl_bp,
                tp_bp=tp_bp,
                units=p["units"],
                entry_bar_idx=p["entry_bar_idx"],
                margin_used=p["margin"],
            )

            state.open_trades[pos_id] = trade
            state._pending = None
            state.trades_total += 1

            # Update inmediato dashboard
            save_state()
            append_history()

            log.info(
                f"  TRADE ABIERTO ID={pos_id} {trade.side.upper()} {trade.units:,}u | "
                f"fill={trade.entry_price:.5f} | SL={trade.sl_price:.5f} ({trade.sl_bp:.1f}bp) | "
                f"TP={trade.tp_price:.5f} ({trade.tp_bp:.1f}bp) | Activos: {state.n_open_trades}"
            )

            log.info(
                f"  PROGRAMANDO AMEND SLTP EN 1.0s | positionId={pos_id} | "
                f"SL={trade.sl_price:.5f} | TP={trade.tp_price:.5f}"
            )
            reactor.callLater(1.0, self.amend_position_sltp, pos_id, trade.sl_price, trade.tp_price)
            trade.sltp_sent = False
            return

        if pos_status == 2 and pos_id in state.open_trades:
            self._finalize_trade_close(pos_id, actual_price, reason="execution_event")
            return

    def _on_reconcile(self, message):
        rec = Protobuf.extract(message)

        server_positions = list(rec.position) if rec.position else []
        server_open_ids = set()

        for p in server_positions:
            try:
                server_open_ids.add(p.positionId)
            except Exception:
                pass

        local_open_ids = set(state.open_trades.keys())

        log.info(
            f"Reconcile: server_open={len(server_open_ids)} | local_open={len(local_open_ids)}"
        )

        # Si una posicion local ya no existe en servidor, asumir cerrada
        missing_ids = local_open_ids - server_open_ids

        for pos_id in list(missing_ids):
            trade = state.open_trades.get(pos_id)
            if trade is None:
                continue

            # precio fallback si no tenemos execution event de cierre
            if trade.side == "buy":
                fallback_close = state.last_bid if state.last_bid is not None else state.last_mid
            else:
                fallback_close = state.last_ask if state.last_ask is not None else state.last_mid

            log.warning(
                f"  Reconcile detecta cierre externo/no capturado | "
                f"positionId={pos_id} | fallback_close={fallback_close}"
            )

            self._finalize_trade_close(pos_id, fallback_close, reason="reconcile_missing")

    def send_order(self, side, units):
        req = ProtoOANewOrderReq()
        req.ctidTraderAccountId = ACCOUNT_ID
        req.symbolId = state.symbol_id
        req.orderType = ProtoOAOrderType.MARKET
        req.tradeSide = ProtoOATradeSide.BUY if side == "buy" else ProtoOATradeSide.SELL
        req.volume = units * 100
        req.comment = "BOS_v6"
        req.label = f"BOSv6_{SYMBOL_NAME}_{side}"

        self.client.send(req).addErrback(self._on_error)

        log.info(
            f"  MARKET ORDER ENVIADA | side={side.upper()} | units={units:,} | "
            f"volume_proto={req.volume} | symbolId={state.symbol_id}"
        )

    def amend_position_sltp(self, position_id, sl_price, tp_price):
        req = ProtoOAAmendPositionSLTPReq()
        req.ctidTraderAccountId = ACCOUNT_ID
        req.positionId = position_id

        # IMPORTANTE:
        # En este endpoint stopLoss y takeProfit van como precios absolutos,
        # no escalados por 100000.
        req.stopLoss = float(round(sl_price, 5))
        req.takeProfit = float(round(tp_price, 5))

        self.client.send(req).addErrback(self._on_error)

        log.info(
            f"  AMEND SLTP ENVIADO | positionId={position_id} | "
            f"SL={req.stopLoss:.5f} | TP={req.takeProfit:.5f}"
        )

    def close_position(self, position_id):
        if position_id not in state.open_trades:
            return

        trade = state.open_trades[position_id]
        req = ProtoOAClosePositionReq()
        req.ctidTraderAccountId = ACCOUNT_ID
        req.positionId = position_id
        req.volume = trade.units * 100
        self.client.send(req).addErrback(self._on_error)

        log.info(
            f"  CIERRE ENVIADO | positionId={position_id} | side={trade.side.upper()} | units={trade.units:,}"
        )

    def start(self):
        self._connect()
        if not reactor.running:
            reactor.run()

# =============================================================================
# MAIN
# =============================================================================

def main():
    log.info("=" * 78)
    log.info("EURUSD BOS REVERSAL BOT v6")
    log.info("=" * 78)
    log.info(f"  Cuenta: {ACCOUNT_ID} ({'DEMO' if USE_DEMO else 'LIVE'})")
    log.info(f"  Symbol: {SYMBOL_NAME} | symbolId fallback={DEFAULT_SYMBOL_ID}")
    log.info(f"  Filtros: ATR>={ATR_MIN_BP}bp + ADX>={ADX_MIN_VAL}")
    log.info(f"  Swing lookback: {SWING_LOOKBACK} barras | swing min={MIN_SWING_BP}bp")
    log.info(f"  SL: ATR*{ATR_MULT} clip[{SL_MIN_BP}-{SL_MAX_BP}bp] | TP: {TP_RATIO}:1")
    log.info(f"  Riesgo: {RISK_PCT*100:.1f}% | MarginCap: {MAX_MARGIN_PCT*100:.0f}%")
    log.info(f"  Spread max: {MAX_SPREAD_BP:.1f}bp")
    log.info(f"  Heartbeat: cada {HEARTBEAT_SECS}s")
    log.info(f"  Precarga historica: hasta {HISTORICAL_BARS} barras H1 | lookback {HISTORICAL_LOOKBACK_DAYS} dias")
    log.info("=" * 78)
    BosBot().start()

if __name__ == "__main__":
    main()
