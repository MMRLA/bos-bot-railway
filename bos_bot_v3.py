import json
import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAAccountAuthReq,
    ProtoOAAmendPositionSLTPReq,
    ProtoOAApplicationAuthReq,
    ProtoOAClosePositionReq,
    ProtoOAGetAccountListByAccessTokenReq,
    ProtoOAGetTrendbarsReq,
    ProtoOANewOrderReq,
    ProtoOAReconcileReq,
    ProtoOASubscribeSpotsReq,
    ProtoOASymbolsListReq,
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


SYMBOL_NAME = "EURUSD"
DEFAULT_SYMBOL_ID = 1

BAR_SECONDS = 3600

# --- PARAMETROS EXACTOS DEL RESEARCH / NOTEBOOK ---
EMA_FAST_N = 12
EMA_SLOW_N = 48
HTF_FAST_N = 24
HTF_SLOW_N = 120
SMA10_N = 10
ATR_PERIOD = 14
ADX_PERIOD = 14
SPREAD_Z_WINDOW = 30
SPREAD_Z_MIN_PERIODS = 10
BOS_SWING_LOOKBACK = 6
BOS_MIN_SWING_BP = 6.0
BOS_COOLDOWN_BARS = 2
BOS_ADX_MIN = 14.0
BOS_EMA_DIFF_MIN_BP = 2.0
DEPTH_FILTER_BP = 9.5923   # q75 congelado del research
TP_RATIO = 1.5
SL_MIN_BP = 6.0
SL_MAX_BP = 25.0
MAX_BARS_OPEN = 16
MARGIN_USAGE = 0.30        # sizing estilo notebook
LEVERAGE = 30
MAX_MARGIN_PCT = 0.40      # safety cap adicional
MAX_SPREAD_BP_LIVE = 3.0   # filtro de spots/ticks para construir barras
SPREAD_Z_MAX = 2.5         # filtro BOS del notebook

# overlay opcional NO activado por defecto (en research quedó secundario)
USE_PAUSE_BUCKET_3 = False
PAUSE_BUCKET_3_VALUE = 3

# cargar suficiente historia para HTF EMA 120 + ventanas de spread + warmup cómodo
HISTORICAL_BARS = 600
HISTORICAL_LOOKBACK_DAYS = 30

HEARTBEAT_SECS = 30
ACCOUNT_AUTH_TIMEOUT_SECS = 10
USE_BROKER_H1_FOR_SIGNAL = True
BROKER_H1_FETCH_DELAY_SECS = 2.0
BROKER_H1_COMPARE_BARS = 300

# dashboard / estado
STATE_FILE = "/opt/bos-bot/data/state.json"
HISTORY_FILE = "/opt/bos-bot/data/history.jsonl"

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bos_bot_v7_research_exact.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("BOS_v7_RESEARCH")

# =============================================================================
# DATACLASSES
# =============================================================================

@dataclass
class OpenTrade:
    position_id: int
    side: str
    entry_price: float
    sl_price: float
    tp_price: float
    sl_bp: float
    tp_bp: float
    units: int
    entry_bar_idx: int
    margin_used: float
    signal_ts: Optional[int] = None
    entry_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    sltp_sent: bool = False

# =============================================================================
# ESTADO
# =============================================================================

class BotState:
    def __init__(self):
        self.bars: List[dict] = []
        self.max_bars = 800

        self.current_bar: Optional[dict] = None
        self.current_bar_start: Optional[int] = None
        self.current_bar_valid_ticks = 0
        self.current_bar_spread_sum = 0.0
        self.current_bar_spread_count = 0

        self.open_trades: Dict[int, OpenTrade] = {}
        self.last_bos_bar = -10_000

        self.equity = 2000.0
        self.symbol_id = DEFAULT_SYMBOL_ID
        self.symbol_resolved = False

        self.pip_size = 0.0001

        self.trades_total = 0
        self.trades_win = 0
        self.trades_loss = 0
        self.pnl_bp_total = 0.0
        self.loss_streak_closed = 0

        self._pending: Optional[dict] = None
        self.historical_loaded = False

        # feed / spots
        self.tick_count = 0
        self.raw_spot_count = 0
        self.spot_reject_spread = 0
        self.spot_reject_not_ready = 0
        self.spot_ts_fallback_count = 0

        self.last_spot_ts: Optional[int] = None
        self.last_valid_tick_ts: Optional[int] = None

        self.last_bid: Optional[float] = None
        self.last_ask: Optional[float] = None
        self.last_mid: Optional[float] = None
        self.last_spread_bp: Optional[float] = None

        # heartbeat incremental counters
        self.hb_prev_raw_spots = 0
        self.hb_prev_valid_ticks = 0
        self.hb_prev_reject_spread = 0
        self.hb_prev_reject_not_ready = 0
        self.hb_prev_ts_fallbacks = 0

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
        base = max(
            HTF_SLOW_N,
            EMA_SLOW_N,
            SMA10_N,
            ATR_PERIOD + 2,
            ADX_PERIOD * 2,
            SPREAD_Z_WINDOW,
            BOS_SWING_LOOKBACK + 1,
        )
        return int(base + 10)

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


def safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default


def clean_nan(obj):
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_nan(v) for v in obj]
    if isinstance(obj, float):
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
        if ts < 946684800 or ts > now_ts + 3600:
            state.spot_ts_fallback_count += 1
            return now_ts
        return ts
    except Exception:
        state.spot_ts_fallback_count += 1
        return now_ts


def proto_volume_to_units(volume_proto):
    v = safe_float(volume_proto, default=float("nan"))
    if math.isnan(v) or v <= 0:
        return 0
    return int(round(v / 100.0))


def normalize_price_field(x):
    px = safe_float(x, default=float("nan"))
    if math.isnan(px) or px <= 0:
        return 0.0
    if px > 100:
        return px / 100000.0
    return px


def calc_margin_for_units(units, entry_price):
    if units <= 0 or entry_price <= 0:
        return 0.0
    return units * entry_price / LEVERAGE


# =============================================================================
# FEATURE ENGINEERING EXACTO DEL NOTEBOOK (adaptado a live H1)
# =============================================================================

def bars_to_feature_df(bars: List[dict]) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame()

    df = pd.DataFrame(bars).copy()
    df = df.sort_values("time").drop_duplicates(subset=["time"], keep="last").reset_index(drop=True)
    df["ts"] = pd.to_datetime(df["time"], unit="s", utc=True)
    df = df.set_index("ts")

    for c in ["open", "high", "low", "close", "spread"]:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")

    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    spread = df["spread"].astype(float)
    eps = 1e-12

    # EMAs / SMA exactos del notebook
    df[f"ema_{EMA_FAST_N}"] = close.ewm(span=EMA_FAST_N, adjust=False, min_periods=EMA_FAST_N).mean()
    df[f"ema_{EMA_SLOW_N}"] = close.ewm(span=EMA_SLOW_N, adjust=False, min_periods=EMA_SLOW_N).mean()
    df[f"ema_{HTF_FAST_N}"] = close.ewm(span=HTF_FAST_N, adjust=False, min_periods=HTF_FAST_N).mean()
    df[f"ema_{HTF_SLOW_N}"] = close.ewm(span=HTF_SLOW_N, adjust=False, min_periods=HTF_SLOW_N).mean()
    df[f"sma_{SMA10_N}"] = close.rolling(SMA10_N, min_periods=SMA10_N).mean()

    ltf_side = np.where(df[f"ema_{EMA_FAST_N}"] > df[f"ema_{EMA_SLOW_N}"], 1,
               np.where(df[f"ema_{EMA_FAST_N}"] < df[f"ema_{EMA_SLOW_N}"], -1, 0))
    htf_side = np.where(df[f"ema_{HTF_FAST_N}"] > df[f"ema_{HTF_SLOW_N}"], 1,
               np.where(df[f"ema_{HTF_FAST_N}"] < df[f"ema_{HTF_SLOW_N}"], -1, 0))
    df["ltf_side"] = pd.Series(ltf_side, index=df.index).astype("int8")
    df["htf_side"] = pd.Series(htf_side, index=df.index).astype("int8")

    # ATR usada en el notebook para sizing/SL (atr_14)
    tr = pd.concat([
        (high - low),
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs(),
    ], axis=1).max(axis=1)
    df["atr_14"] = tr.rolling(ATR_PERIOD, min_periods=max(7, ATR_PERIOD // 2)).mean()
    df["atr_bp"] = (df["atr_14"] / close.replace(0, np.nan).abs()) * 1e4

    # ADX exacto de SEC4 builder (Wilder EMA con alpha=1/n)
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    atr_wilder = tr.ewm(alpha=1.0 / ADX_PERIOD, adjust=False, min_periods=ADX_PERIOD).mean()
    plus_di = 100.0 * pd.Series(plus_dm, index=df.index).ewm(alpha=1.0 / ADX_PERIOD, adjust=False, min_periods=ADX_PERIOD).mean() / (atr_wilder + eps)
    minus_di = 100.0 * pd.Series(minus_dm, index=df.index).ewm(alpha=1.0 / ADX_PERIOD, adjust=False, min_periods=ADX_PERIOD).mean() / (atr_wilder + eps)
    dx = 100.0 * (plus_di - minus_di).abs() / ((plus_di + minus_di) + eps)
    df["adx_14"] = dx.ewm(alpha=1.0 / ADX_PERIOD, adjust=False, min_periods=ADX_PERIOD).mean()

    # swing ref exacto SEC11
    df["swing_hi_ref"] = high.shift(1).rolling(BOS_SWING_LOOKBACK, min_periods=max(1, BOS_SWING_LOOKBACK // 2)).max()
    df["swing_lo_ref"] = low.shift(1).rolling(BOS_SWING_LOOKBACK, min_periods=max(1, BOS_SWING_LOOKBACK // 2)).min()
    df["swing_range_ref_bp"] = ((df["swing_hi_ref"] - df["swing_lo_ref"]) / (close.abs() + eps)) * 1e4

    # ema diff signed gate exacto SEC11
    ema_diff_bp = ((df[f"ema_{EMA_FAST_N}"] - df[f"ema_{EMA_SLOW_N}"]) / (close.abs() + eps)) * 1e4
    df["ema_diff_bp_signed"] = ema_diff_bp * df["ltf_side"].astype(float)

    # spread z exacto aproximado desde spread medio H1
    sp_ma30 = spread.rolling(SPREAD_Z_WINDOW, min_periods=SPREAD_Z_MIN_PERIODS).mean()
    sp_std30 = spread.rolling(SPREAD_Z_WINDOW, min_periods=SPREAD_Z_MIN_PERIODS).std()
    df["spread_z_30"] = (spread - sp_ma30) / (sp_std30 + eps)
    df["spread_bp"] = spread * 1e4

    # depth vs SMA10 exacto SEC5
    sma10 = df[f"sma_{SMA10_N}"]
    df["close_vs_sma10_bp"] = 10000.0 * (close / (sma10 + eps) - 1.0)
    cv = df["close_vs_sma10_bp"].astype(float)
    depth_up = (-cv).rolling(10, min_periods=1).max()
    depth_dn = (cv).rolling(10, min_periods=1).max()
    df["pullback_depth_vs_sma10_bp"] = np.where(
        df["ltf_side"] > 0,
        depth_up,
        np.where(df["ltf_side"] < 0, depth_dn, 0.0),
    )

    # Distancia de ruptura para logging
    bos_long_raw = (df["ltf_side"] == 1) & (close > df["swing_hi_ref"])
    bos_short_raw = (df["ltf_side"] == -1) & (close < df["swing_lo_ref"])
    df["bos_break_dist_bp"] = np.where(
        bos_long_raw,
        ((close - df["swing_hi_ref"]) / (close.abs() + eps)) * 1e4,
        np.where(
            bos_short_raw,
            ((df["swing_lo_ref"] - close) / (close.abs() + eps)) * 1e4,
            0.0,
        ),
    )

    # time features
    df["hour"] = df.index.hour.astype(int)
    df["dow"] = df.index.dayofweek.astype(int)

    return df


def compute_strategy_snapshot() -> dict:
    df = bars_to_feature_df(state.bars)
    if df.empty:
        return {}
    row = df.iloc[-1]
    return {
        "close": safe_float(row.get("close")),
        "ema_fast": safe_float(row.get(f"ema_{EMA_FAST_N}")),
        "ema_slow": safe_float(row.get(f"ema_{EMA_SLOW_N}")),
        "ema_htf_fast": safe_float(row.get(f"ema_{HTF_FAST_N}")),
        "ema_htf_slow": safe_float(row.get(f"ema_{HTF_SLOW_N}")),
        "ltf_side": safe_int(row.get("ltf_side"), 0),
        "htf_side": safe_int(row.get("htf_side"), 0),
        "atr_bp": safe_float(row.get("atr_bp")),
        "adx": safe_float(row.get("adx_14")),
        "swing_hi_ref": safe_float(row.get("swing_hi_ref")),
        "swing_lo_ref": safe_float(row.get("swing_lo_ref")),
        "swing_range_bp": safe_float(row.get("swing_range_ref_bp")),
        "spread_z_30": safe_float(row.get("spread_z_30")),
        "spread_bp_bar": safe_float(row.get("spread_bp")),
        "ema_diff_bp_signed": safe_float(row.get("ema_diff_bp_signed")),
        "depth_bp": safe_float(row.get("pullback_depth_vs_sma10_bp")),
        "bos_break_dist_bp": safe_float(row.get("bos_break_dist_bp")),
    }

# =============================================================================
# PERSISTENCIA PARA DASHBOARD
# =============================================================================

def save_state():
    try:
        ind = compute_strategy_snapshot() if state.is_warmed_up() else {}

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
            "loss_streak_closed": state.loss_streak_closed,
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
            "strategy": {
                "depth_filter_bp": DEPTH_FILTER_BP,
                "tp_ratio": TP_RATIO,
                "max_bars_open": MAX_BARS_OPEN,
                "pause_bucket_3_enabled": USE_PAUSE_BUCKET_3,
            },
            "indicators": {
                "atr_bp": ind.get("atr_bp") if ind else None,
                "adx": ind.get("adx") if ind else None,
                "swing_range_bp": ind.get("swing_range_bp") if ind else None,
                "ema_diff_bp_signed": ind.get("ema_diff_bp_signed") if ind else None,
                "spread_z_30": ind.get("spread_z_30") if ind else None,
                "depth_bp": ind.get("depth_bp") if ind else None,
                "bos_break_dist_bp": ind.get("bos_break_dist_bp") if ind else None,
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
                    "signal_ts": t.signal_ts,
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
        ind = compute_strategy_snapshot() if state.is_warmed_up() else {}
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
            "ema_diff_bp_signed": ind.get("ema_diff_bp_signed") if ind else None,
            "depth_bp": ind.get("depth_bp") if ind else None,
            "side": ind.get("ltf_side") if ind else None,
        }
        row = clean_nan(row)
        with open(HISTORY_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, allow_nan=False) + "\n")
    except Exception as e:
        log.warning(f"Error guardando history: {e}")

# =============================================================================
# ESTRATEGIA EXACTA DEL NOTEBOOK
# =============================================================================

def evaluate_research_signal(signal_bar: dict) -> Tuple[Optional[str], dict]:
    if not state.is_warmed_up():
        return None, {"reject": ["warmup"]}

    df = bars_to_feature_df(state.bars)
    if df.empty:
        return None, {"reject": ["sin_df_features"]}

    row = df.iloc[-1]
    close = safe_float(row.get("close"))
    ltf_side = safe_int(row.get("ltf_side"), 0)
    htf_side = safe_int(row.get("htf_side"), 0)
    swing_hi_ref = safe_float(row.get("swing_hi_ref"))
    swing_lo_ref = safe_float(row.get("swing_lo_ref"))
    swing_range_bp = safe_float(row.get("swing_range_ref_bp"))
    adx = safe_float(row.get("adx_14"))
    ema_diff_bp_signed = safe_float(row.get("ema_diff_bp_signed"))
    spread_z_30 = safe_float(row.get("spread_z_30"))
    depth_bp = safe_float(row.get("pullback_depth_vs_sma10_bp"))
    bos_break_dist_bp = safe_float(row.get("bos_break_dist_bp"))

    reasons = []

    # exact notebook filters
    critical = [close, swing_hi_ref, swing_lo_ref, swing_range_bp, adx, ema_diff_bp_signed]
    if any(math.isnan(x) for x in critical):
        reasons.append("features_nan")

    swing_ok = (not math.isnan(swing_range_bp)) and (swing_range_bp >= BOS_MIN_SWING_BP)
    adx_ok = (not math.isnan(adx)) and (adx >= BOS_ADX_MIN)
    ema_diff_ok = (not math.isnan(ema_diff_bp_signed)) and (ema_diff_bp_signed >= BOS_EMA_DIFF_MIN_BP)
    spread_ok = math.isnan(spread_z_30) or (spread_z_30 <= SPREAD_Z_MAX)
    htf_align_ok = (htf_side == 0) or (ltf_side == 0) or (htf_side * ltf_side >= 0)

    bar_dt = datetime.fromtimestamp(signal_bar["time"], tz=timezone.utc)
    session_ok = not ((bar_dt.weekday() == 4 and bar_dt.hour >= 20) or (bar_dt.weekday() == 6))

    cooldown_ok = state.bars_since_last_bos > BOS_COOLDOWN_BARS
    one_trade_only_ok = state.n_open_trades == 0
    pause_bucket_ok = (not USE_PAUSE_BUCKET_3) or (state.loss_streak_closed != PAUSE_BUCKET_3_VALUE)
    depth_ok = (not math.isnan(depth_bp)) and (depth_bp <= DEPTH_FILTER_BP)

    bos_long_raw = (ltf_side == 1) and (not math.isnan(swing_hi_ref)) and (close > swing_hi_ref)
    bos_short_raw = (ltf_side == -1) and (not math.isnan(swing_lo_ref)) and (close < swing_lo_ref)
    bos_raw = bos_long_raw or bos_short_raw

    if not bos_raw:
        reasons.append("sin_bos")
    if not swing_ok:
        reasons.append(f"swing={swing_range_bp:.2f}<{BOS_MIN_SWING_BP}" if not math.isnan(swing_range_bp) else "swing_nan")
    if not adx_ok:
        reasons.append(f"adx={adx:.2f}<{BOS_ADX_MIN}" if not math.isnan(adx) else "adx_nan")
    if not ema_diff_ok:
        reasons.append(f"ema_diff={ema_diff_bp_signed:.2f}<{BOS_EMA_DIFF_MIN_BP}" if not math.isnan(ema_diff_bp_signed) else "ema_diff_nan")
    if not spread_ok:
        reasons.append(f"spread_z={spread_z_30:.2f}>{SPREAD_Z_MAX}")
    if not session_ok:
        reasons.append("session_block")
    if not htf_align_ok:
        reasons.append("htf_misaligned")
    if not cooldown_ok:
        reasons.append(f"cooldown<{BOS_COOLDOWN_BARS+1}b")
    if not one_trade_only_ok:
        reasons.append("trade_activo")
    if not pause_bucket_ok:
        reasons.append(f"pause_bucket_3(loss_streak={state.loss_streak_closed})")
    if not depth_ok:
        reasons.append(f"depth={depth_bp:.4f}>{DEPTH_FILTER_BP:.4f}" if not math.isnan(depth_bp) else "depth_nan")

    decision = None
    if bos_raw and swing_ok and adx_ok and ema_diff_ok and spread_ok and session_ok and htf_align_ok and cooldown_ok and one_trade_only_ok and pause_bucket_ok and depth_ok:
        # inversion exacta del BOS: BOS alcista -> sell, BOS bajista -> buy
        decision = "sell" if bos_long_raw else "buy"

    info = {
        "ltf_side": ltf_side,
        "htf_side": htf_side,
        "swing_range_bp": swing_range_bp,
        "adx": adx,
        "ema_diff_bp_signed": ema_diff_bp_signed,
        "spread_z_30": spread_z_30,
        "depth_bp": depth_bp,
        "bos_break_dist_bp": bos_break_dist_bp,
        "bos_long_raw": bos_long_raw,
        "bos_short_raw": bos_short_raw,
        "reject": reasons,
    }
    return decision, info

# =============================================================================
# RIESGO / SIZING / SLTP (alineado al notebook final)
# =============================================================================

def calc_sl_tp(side: str, entry: float, atr_bp: float):
    sl_bp = float(np.clip(atr_bp, SL_MIN_BP, SL_MAX_BP))
    tp_bp = float(sl_bp * TP_RATIO)

    sl_d = bp_to_price(sl_bp, entry)
    tp_d = bp_to_price(tp_bp, entry)

    if side == "sell":
        return entry + sl_d, entry - tp_d, sl_bp, tp_bp
    return entry - sl_d, entry + tp_d, sl_bp, tp_bp


def calc_units_notional(entry_price: float):
    if entry_price <= 0 or state.equity <= 0:
        return 0, 0.0, 0.0

    target_notional = state.equity * LEVERAGE * MARGIN_USAGE
    units_float = target_notional / entry_price
    units = max(1000, int(units_float / 1000) * 1000)
    margin = calc_margin_for_units(units, entry_price)
    notional = units * entry_price
    return units, margin, notional


def execute_signal(signal: str, signal_bar: dict, snapshot: dict, bot):
    # entrada real usando último bid/ask live, no el close sintético del trendbar broker
    if state.last_bid is None or state.last_ask is None:
        log.warning("SEÑAL OMITIDA: no hay bid/ask live para ejecutar")
        return

    entry = state.last_ask if signal == "buy" else state.last_bid
    current_spread_bp = px_to_bp(state.last_ask - state.last_bid, state.last_bid)
    atr_bp = snapshot.get("atr_bp", float("nan"))

    if math.isnan(atr_bp) or atr_bp <= 0:
        log.warning("SEÑAL OMITIDA: atr_bp invalido")
        return

    sl_price, tp_price, sl_bp, tp_bp = calc_sl_tp(signal, entry, atr_bp)
    units, margin_needed, notional = calc_units_notional(entry)

    if units <= 0:
        log.warning("SEÑAL OMITIDA: sizing invalido (units<=0)")
        return

    if margin_needed > state.margin_available:
        log.warning(
            f"SEÑAL OMITIDA: margen insuficiente (necesario={margin_needed:.0f}€, disponible={state.margin_available:.0f}€)"
        )
        return

    if current_spread_bp > MAX_SPREAD_BP_LIVE:
        log.warning(f"SEÑAL OMITIDA: spread live demasiado alto ({current_spread_bp:.2f}bp > {MAX_SPREAD_BP_LIVE:.2f}bp)")
        return

    log.info(
        f"\n  *** {signal.upper()} RESEARCH-EXACT PRE-ORDER | EntryRef={entry:.5f} | SpreadLive={current_spread_bp:.2f}bp | "
        f"ATR={atr_bp:.2f}bp | SL={sl_price:.5f} ({sl_bp:.2f}bp) | TP={tp_price:.5f} ({tp_bp:.2f}bp) | "
        f"Units={units:,} | Notional={notional:,.0f} | Margen={margin_needed:.0f}€ ***"
    )

    state._pending = {
        "side": signal,
        "entry_price_ref": entry,
        "sl_bp": sl_bp,
        "tp_bp": tp_bp,
        "units": units,
        "margin": margin_needed,
        "entry_bar_idx": state.n_bars,
        "signal_bar_time": signal_bar["time"],
        "signal_bid": state.last_bid,
        "signal_ask": state.last_ask,
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

    completed["spread"] = (
        state.current_bar_spread_sum / state.current_bar_spread_count
        if state.current_bar_spread_count > 0 else np.nan
    )
    state.add_bar(completed)
    state.last_local_closed_bar = completed

    bar_dt = datetime.fromtimestamp(completed["time"], tz=timezone.utc)
    spread_bp_bar = safe_float(completed.get("spread"), float("nan")) * 1e4 if completed.get("spread") is not None else float("nan")

    log.info(f"\n{'='*78}")
    log.info(f"BARRA CERRADA: {bar_dt.strftime('%Y-%m-%d %H:%M')} UTC")
    log.info(
        f"  O={completed['open']:.5f} H={completed['high']:.5f} L={completed['low']:.5f} C={completed['close']:.5f} | "
        f"ticks_validos={state.current_bar_valid_ticks} | spread_mean={spread_bp_bar:.2f}bp"
    )
    log.info(
        f"  Barras acumuladas: {state.n_bars} | Trades abiertos: {state.n_open_trades} | "
        f"PnL total: {state.pnl_bp_total:+.1f}bp | loss_streak_closed={state.loss_streak_closed}"
    )

    if state.current_bar_valid_ticks <= 1:
        log.warning("  AVISO: la barra se ha construido con 0/1 ticks validos. Revisa feed/spread/symbolId.")

    if not USE_BROKER_H1_FOR_SIGNAL:
        on_bar_close(completed, bot)
    else:
        log.info("Cierre local H1 registrado; la señal esperará a la barra broker confirmada.")


def on_tick(bid, ask, ts, bot):
    state.tick_count += 1
    state.last_valid_tick_ts = ts

    mid = (bid + ask) / 2.0
    spread_abs = max(0.0, ask - bid)
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
        state.current_bar_spread_sum = spread_abs
        state.current_bar_spread_count = 1
        return

    if bar_start < state.current_bar_start:
        return

    if bar_start == state.current_bar_start:
        cb = state.current_bar
        cb["high"] = max(cb["high"], mid)
        cb["low"] = min(cb["low"], mid)
        cb["close"] = mid
        cb["bid"] = bid
        cb["ask"] = ask
        state.current_bar_valid_ticks += 1
        state.current_bar_spread_sum += spread_abs
        state.current_bar_spread_count += 1
        return

    if USE_BROKER_H1_FOR_SIGNAL:
        return

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
    state.current_bar_spread_sum = spread_abs
    state.current_bar_spread_count = 1


def on_bar_close(bar, bot):
    # cierre vertical exacto del notebook a 16 barras
    to_close = []
    for pid, trade in state.open_trades.items():
        bars_open = state.n_bars - trade.entry_bar_idx
        if bars_open >= MAX_BARS_OPEN:
            log.warning(f"  Trade {pid}: horizonte vertical max ({MAX_BARS_OPEN} barras) — cerrando")
            to_close.append(pid)
    for pid in to_close:
        bot.close_position(pid)

    needed = state.n_bars_needed()
    if not state.is_warmed_up():
        faltan = needed - state.n_bars
        log.info(f"  Calentando: {state.n_bars}/{needed} barras (faltan {faltan} barras = {faltan}h)")
        return

    snapshot = compute_strategy_snapshot()
    side_str = "▲UP" if snapshot.get("ltf_side") == 1 else "▼DOWN" if snapshot.get("ltf_side") == -1 else "·FLAT"

    log.info(
        f"  EMA_f={snapshot.get('ema_fast', float('nan')):.5f} EMA_s={snapshot.get('ema_slow', float('nan')):.5f} {side_str} | "
        f"ATR={snapshot.get('atr_bp', float('nan')):.2f}bp | ADX={snapshot.get('adx', float('nan')):.2f} | "
        f"SwingRef={snapshot.get('swing_range_bp', float('nan')):.2f}bp | depth={snapshot.get('depth_bp', float('nan')):.2f}bp | "
        f"emaDiffSigned={snapshot.get('ema_diff_bp_signed', float('nan')):.2f}bp | spreadZ={snapshot.get('spread_z_30', float('nan')):.2f}"
    )

    signal, info = evaluate_research_signal(bar)
    if signal:
        if info.get("bos_long_raw"):
            log.info(
                f"  BOS ALCISTA detectado sobre tendencia UP: reversal exacto => SELL | break_dist={info.get('bos_break_dist_bp', float('nan')):.2f}bp"
            )
        elif info.get("bos_short_raw"):
            log.info(
                f"  BOS BAJISTA detectado sobre tendencia DOWN: reversal exacto => BUY | break_dist={info.get('bos_break_dist_bp', float('nan')):.2f}bp"
            )
        execute_signal(signal, bar, snapshot, bot)
    else:
        if info.get("reject"):
            log.info(f"  Sin señal: {' | '.join(info['reject'])}")
        else:
            log.info("  Sin señal")

    wr = state.trades_win / max(state.trades_total, 1) * 100.0
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
            "spread": np.nan,
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
        self.manual_disconnect_in_progress = False

        log.info(f"Bot v7 research-exact inicializado | Servidor: {host}")

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
        state.current_bar_spread_sum = 0.0
        state.current_bar_spread_count = 0

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

        state.last_local_closed_bar = None
        state.pending_broker_h1_fetch = False
        state.last_broker_h1_bar_time = None

    def _seed_current_bar_after_historical(self, fallback_close):
        now_ts = self.last_ts if self.last_ts is not None else now_utc_ts()
        bar_start = get_bar_start(now_ts)

        if self.last_bid is not None and self.last_ask is not None:
            bid = self.last_bid
            ask = self.last_ask
            mid = (bid + ask) / 2.0
            spread_abs = max(0.0, ask - bid)
            spread_count = 1
        else:
            bid = fallback_close
            ask = fallback_close
            mid = fallback_close
            spread_abs = 0.0
            spread_count = 0

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
        state.current_bar_spread_sum = spread_abs
        state.current_bar_spread_count = spread_count

    def _schedule_broker_h1_fetch_after_close(self):
        if state.pending_broker_h1_fetch:
            log.info("Fetch H1 broker ya pendiente; no se duplica.")
            return
        state.pending_broker_h1_fetch = True
        log.info(f"PROGRAMANDO FETCH H1 BROKER EN {BROKER_H1_FETCH_DELAY_SECS}s TRAS CIERRE HORARIO")
        reactor.callLater(BROKER_H1_FETCH_DELAY_SECS, self._fetch_broker_h1_after_close)

    def _schedule_hourly_bar_timer(self):
        if self.hourly_timer_started:
            return
        self.hourly_timer_started = True

        def hourly_check_loop():
            try:
                self._on_hour_boundary()
            except Exception as e:
                log.error(f"Error en hourly_check_loop: {e}", exc_info=True)
            finally:
                reactor.callLater(1.0, hourly_check_loop)

        reactor.callLater(1.0, hourly_check_loop)
        log.info("Timer horario UTC activo (check cada 1s).")

    def _on_hour_boundary(self):
        if not self.ready_for_ticks:
            return
        if state.current_bar is None or state.current_bar_start is None:
            return

        now_ts = now_utc_ts()
        current_hour_start = get_bar_start(now_ts)

        if state.current_bar_start is not None and state.current_bar_start != current_hour_start:
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
            state.current_bar_spread_sum = 0.0
            state.current_bar_spread_count = 0

            log.info("Timer UTC: cierre/rollover horario H1 ejecutado.")
            self._schedule_broker_h1_fetch_after_close()

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

        if not self.shutting_down:
            log.warning("Reconectando en 15s...")
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

        unique_ids, seen = [], set()
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
            self._check_account_auth_timeout,
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
        for attr in ("symbolName", "symbol", "name", "displayName", "description", "shortName", "title"):
            if hasattr(sym, attr):
                val = getattr(sym, attr)
                if isinstance(val, str) and val.strip():
                    texts.append(val.strip())
        return texts

    def _resolve_symbol_from_response(self, res):
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
                f"payloadType={t} | errorCode={getattr(obj, 'errorCode', None)} | "
                f"description={getattr(obj, 'description', None)} | accountId={getattr(obj, 'ctidTraderAccountId', None)}"
            )
            return

        if t == 2132 or obj_name == "ProtoOAOrderErrorEvent":
            log.error(
                "OrderErrorEvent | "
                f"errorCode={getattr(obj, 'errorCode', None)} | description={getattr(obj, 'description', None)} | "
                f"orderId={getattr(obj, 'orderId', None)} | positionId={getattr(obj, 'positionId', None)} | "
                f"accountId={getattr(obj, 'ctidTraderAccountId', None)}"
            )
            return

        if t == 2164 or obj_name == "ProtoOAAccountDisconnectEvent":
            log.warning(
                "AccountDisconnectEvent | "
                f"accountId={getattr(obj, 'ctidTraderAccountId', None)} | reason={getattr(obj, 'reason', None)}"
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
                    f"No se pudo resolver {SYMBOL_NAME} por nombre. Usando fallback symbolId={DEFAULT_SYMBOL_ID}. "
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
                    f"O={cb['open']:.5f} H={cb['high']:.5f} L={cb['low']:.5f} C={cb['close']:.5f} | "
                    f"ticks_bar={state.current_bar_valid_ticks}"
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
            if not math.isnan(spread_live) and spread_live > MAX_SPREAD_BP_LIVE:
                flags.append(f"spread_alto={spread_live:.2f}bp")
            if hb_ts_fallbacks > 0:
                flags.append(f"ts_fallbacks(+30s)={hb_ts_fallbacks}")

            log.info(
                f"[{now}] FEED | bid={bid_live:.5f} ask={ask_live:.5f} mid={mid_live:.5f} | spread={spread_live:.2f}bp | "
                f"raw_spots(+30s)={hb_raw} | valid_ticks(+30s)={hb_valid} | rej_spread(+30s)={hb_rej_spread} | "
                f"rej_not_ready(+30s)={hb_rej_not_ready} | ts_fallbacks_total={state.spot_ts_fallback_count} | "
                f"last_spot={secs_since_last_spot if secs_since_last_spot is not None else 'NA'}s | "
                f"last_valid={secs_since_last_valid if secs_since_last_valid is not None else 'NA'}s"
            )

            log.info(
                f"[{now}] BARRA | start={fmt_dt_utc(state.current_bar_start)} | age={bar_age_txt} | prox_cierre={prox_bar_txt} | {bar_txt}"
            )

            if state.is_warmed_up():
                ind = compute_strategy_snapshot()
                side_txt = "UP" if ind.get("ltf_side") == 1 else "DOWN" if ind.get("ltf_side") == -1 else "FLAT"
                wr = state.trades_win / max(state.trades_total, 1) * 100
                log.info(
                    f"[{now}] SISTEMA | READY | bars={state.n_bars} | trades={state.n_open_trades} | pnl={state.pnl_bp_total:+.1f}bp | "
                    f"margin={state.total_margin_used:.0f}/{state.equity*MAX_MARGIN_PCT:.0f}€ | ATR={ind.get('atr_bp', float('nan')):.2f}bp | "
                    f"ADX={ind.get('adx', float('nan')):.2f} | swing={ind.get('swing_range_bp', float('nan')):.2f}bp | "
                    f"depth={ind.get('depth_bp', float('nan')):.2f}bp | emaDiff={ind.get('ema_diff_bp_signed', float('nan')):.2f}bp | "
                    f"spreadZ={ind.get('spread_z_30', float('nan')):.2f} | side={side_txt} | WR={wr:.0f}% | LS={state.loss_streak_closed}"
                )
            else:
                faltan = needed - state.n_bars
                log.info(
                    f"[{now}] SISTEMA | CALENTANDO {state.n_bars}/{needed} | faltan={faltan} barras | trades={state.n_open_trades} | pnl={state.pnl_bp_total:+.1f}bp"
                )

            if flags:
                log.warning(f"[{now}] AVISO_FEED | {' | '.join(flags)}")

            save_state()
            append_history()
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
                f"Solicitando historico H1 | symbolId={state.symbol_id} | from={from_ms} to={now_ms} | lookback={HISTORICAL_LOOKBACK_DAYS} dias | count={HISTORICAL_BARS}"
            )
        except Exception as e:
            log.warning(f"Precarga no disponible: {e}. El bot calentara con datos en vivo (~{state.n_bars_needed()}h).")

    def _fetch_broker_h1_after_close(self):
        log.info("ENTRANDO EN _fetch_broker_h1_after_close()")
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

            self.client.send(req).addErrback(self._on_error)
            log.info(
                f"Solicitando H1 broker post-cierre | symbolId={state.symbol_id} | delay={BROKER_H1_FETCH_DELAY_SECS}s | count={BROKER_H1_COMPARE_BARS}"
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
                f"COMPARE H1 | tiempos distintos | local={fmt_dt_utc(local_bar['time'])} | broker={fmt_dt_utc(broker_bar['time'])}"
            )
            return

        def diff_bp(a, b):
            mid = broker_bar["close"] if broker_bar["close"] > 0 else 1.0
            return px_to_bp(a - b, mid)

        log.info(
            "COMPARE H1 | "
            f"time={fmt_dt_utc(broker_bar['time'])} | dO={diff_bp(local_bar['open'], broker_bar['open']):+.3f}bp | "
            f"dH={diff_bp(local_bar['high'], broker_bar['high']):+.3f}bp | dL={diff_bp(local_bar['low'], broker_bar['low']):+.3f}bp | "
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

            # fetch horario post-cierre para comparar y ejecutar con broker OHLC + spread local
            if state.pending_broker_h1_fetch:
                state.pending_broker_h1_fetch = False
                current_hour_start = get_bar_start(now_utc_ts())
                closed_candidates = [b for b in dedup if b["time"] < current_hour_start]
                if not closed_candidates:
                    log.warning(f"No hay barras broker cerradas validas para comparar | current_hour_start={fmt_dt_utc(current_hour_start)}")
                    return

                broker_last_closed = closed_candidates[-1]
                state.last_broker_h1_bar_time = broker_last_closed["time"]
                log.info(
                    f"H1 broker post-cierre recibida | time={fmt_dt_utc(broker_last_closed['time'])} | "
                    f"O={broker_last_closed['open']:.5f} H={broker_last_closed['high']:.5f} L={broker_last_closed['low']:.5f} C={broker_last_closed['close']:.5f}"
                )

                if state.last_local_closed_bar is None:
                    log.warning("H1 broker recibida pero no existe last_local_closed_bar para comparar.")
                    return

                self._compare_local_vs_broker_bar(broker_last_closed)

                if USE_BROKER_H1_FOR_SIGNAL:
                    log.info("USANDO H1 BROKER PARA SEÑAL (OHLC broker + spread local)")
                    merged_bar = dict(broker_last_closed)
                    merged_bar["bid"] = state.last_local_closed_bar.get("bid", broker_last_closed["close"])
                    merged_bar["ask"] = state.last_local_closed_bar.get("ask", broker_last_closed["close"])
                    merged_bar["spread"] = state.last_local_closed_bar.get("spread", np.nan)

                    if state.bars and state.bars[-1]["time"] == merged_bar["time"]:
                        state.bars[-1] = merged_bar
                    else:
                        state.add_bar(merged_bar)

                    on_bar_close(merged_bar, self)
                return

            # carga inicial / precarga histórica
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
                log.info(f"Rango historico cargado: {fmt_dt_utc(dedup[0]['time'])} -> {fmt_dt_utc(dedup[-1]['time'])}")

            if state.is_warmed_up():
                snap = compute_strategy_snapshot()
                log.info("INDICADORES LISTOS. El bot puede operar desde la proxima barra.")
                log.info(
                    f"  Estado actual: ATR={snap.get('atr_bp', float('nan')):.2f}bp | ADX={snap.get('adx', float('nan')):.2f} | "
                    f"Lado={'UP' if snap.get('ltf_side')==1 else 'DOWN' if snap.get('ltf_side')==-1 else 'FLAT'} | "
                    f"Swing={snap.get('swing_range_bp', float('nan')):.2f}bp | depth={snap.get('depth_bp', float('nan')):.2f}bp"
                )
            else:
                faltan = needed - state.n_bars
                log.info(f"Calentamiento parcial: {state.n_bars}/{needed}. Faltan {faltan} barras (~{faltan}h con datos en vivo).")
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

        if spread_bp > MAX_SPREAD_BP_LIVE:
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

        if close_price is not None and not math.isnan(close_price) and close_price > 0:
            cp = close_price
            px_source = "event_price"
        else:
            if trade.side == "buy":
                cp = state.last_bid if state.last_bid is not None else state.last_mid
                px_source = "fallback_bid"
            else:
                cp = state.last_ask if state.last_ask is not None else state.last_mid
                px_source = "fallback_ask"
            if cp is None or (isinstance(cp, float) and (math.isnan(cp) or cp <= 0)):
                cp = trade.entry_price
                px_source = "entry_fallback"

        pnl_bp = (((trade.entry_price - cp) if trade.side == "sell" else (cp - trade.entry_price)) / trade.entry_price) * 10_000.0
        state.pnl_bp_total += pnl_bp

        if pnl_bp > 0:
            state.trades_win += 1
            state.loss_streak_closed = 0
        else:
            state.trades_loss += 1
            state.loss_streak_closed += 1

        wr = state.trades_win / max(state.trades_total, 1) * 100
        log.info(
            f"  TRADE CERRADO ID={pos_id} {trade.side.upper()} | reason={reason} | px_source={px_source} | close={cp:.5f} | "
            f"P&L={pnl_bp:+.1f}bp | WR={wr:.0f}% | Total={state.pnl_bp_total:+.1f}bp | Activos: {state.n_open_trades} | "
            f"loss_streak_closed={state.loss_streak_closed}"
        )
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
            f"  EXEC_EVENT | pos_id={pos_id} | pos_status={pos_status} | price={actual_price} | volume={pos_volume} | execution_type={execution_type}"
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
                signal_ts=p.get("signal_bar_time"),
            )

            state.open_trades[pos_id] = trade
            state._pending = None
            state.trades_total += 1
            save_state()
            append_history()

            log.info(
                f"  TRADE ABIERTO ID={pos_id} {trade.side.upper()} {trade.units:,}u | fill={trade.entry_price:.5f} | "
                f"SL={trade.sl_price:.5f} ({trade.sl_bp:.2f}bp) | TP={trade.tp_price:.5f} ({trade.tp_bp:.2f}bp) | Activos: {state.n_open_trades}"
            )

            log.info(
                f"  PROGRAMANDO AMEND SLTP EN 1.0s | positionId={pos_id} | SL={trade.sl_price:.5f} | TP={trade.tp_price:.5f}"
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
        log.info(f"Reconcile: server_open={len(server_open_ids)} | local_open={len(local_open_ids)}")

        missing_ids = local_open_ids - server_open_ids
        for pos_id in list(missing_ids):
            trade = state.open_trades.get(pos_id)
            if trade is None:
                continue
            if trade.side == "buy":
                fallback_close = state.last_bid if state.last_bid is not None else state.last_mid
            else:
                fallback_close = state.last_ask if state.last_ask is not None else state.last_mid
            log.warning(
                f"  Reconcile detecta cierre externo/no capturado | positionId={pos_id} | fallback_close={fallback_close}"
            )
            self._finalize_trade_close(pos_id, fallback_close, reason="reconcile_missing")

        lost_open_ids = server_open_ids - local_open_ids
        for p in server_positions:
            try:
                pos_id = p.positionId
            except Exception:
                continue
            if pos_id not in lost_open_ids:
                continue

            trade_side_raw = getattr(p, "tradeSide", None)
            if trade_side_raw == ProtoOATradeSide.BUY:
                side = "buy"
            elif trade_side_raw == ProtoOATradeSide.SELL:
                side = "sell"
            else:
                side = "unknown"

            if side == "unknown" and hasattr(p, "tradeData"):
                td_side = getattr(p.tradeData, "tradeSide", None)
                if td_side == ProtoOATradeSide.BUY:
                    side = "buy"
                elif td_side == ProtoOATradeSide.SELL:
                    side = "sell"

            entry_price = normalize_price_field(getattr(p, "price", None))
            if entry_price <= 0:
                entry_price = state.last_mid if state.last_mid is not None else 0.0

            volume_raw = getattr(p, "volume", None)
            if (volume_raw is None or volume_raw == 0) and hasattr(p, "tradeData"):
                volume_raw = getattr(p.tradeData, "volume", None)
            units = proto_volume_to_units(volume_raw)
            if units <= 0:
                log.warning(f"Reconcile: no se pudo reconstruir units | positionId={pos_id}")
                continue

            margin_used_raw = safe_float(getattr(p, "usedMargin", None), default=float("nan"))
            money_digits = safe_int(getattr(p, "moneyDigits", None), default=2)
            if not math.isnan(margin_used_raw) and margin_used_raw > 0:
                margin_used = margin_used_raw / (10 ** money_digits)
            else:
                margin_used = calc_margin_for_units(units, entry_price)
            if math.isnan(margin_used) or margin_used <= 0:
                margin_used = calc_margin_for_units(units, entry_price)

            sl_price = normalize_price_field(getattr(p, "stopLoss", None))
            tp_price = normalize_price_field(getattr(p, "takeProfit", None))

            if side == "unknown" and entry_price > 0 and sl_price > 0 and tp_price > 0:
                if sl_price > entry_price and tp_price < entry_price:
                    side = "sell"
                elif sl_price < entry_price and tp_price > entry_price:
                    side = "buy"
            if entry_price <= 0:
                log.warning(f"Reconcile: entry_price invalido | positionId={pos_id}")
                continue

            sl_bp = abs(px_to_bp(sl_price - entry_price, entry_price)) if sl_price > 0 else 0.0
            tp_bp = abs(px_to_bp(tp_price - entry_price, entry_price)) if tp_price > 0 else 0.0

            recovered_trade = OpenTrade(
                position_id=pos_id,
                side=side,
                entry_price=entry_price,
                sl_price=sl_price,
                tp_price=tp_price,
                sl_bp=sl_bp,
                tp_bp=tp_bp,
                units=units,
                entry_bar_idx=state.n_bars,
                margin_used=margin_used,
            )
            state.open_trades[pos_id] = recovered_trade
            log.warning(
                f"  Reconcile recupera apertura perdida | positionId={pos_id} | side={side} | entry={entry_price:.5f} | "
                f"units={units:,} | SL={sl_price:.5f} | TP={tp_price:.5f} | margin={margin_used:.0f}€"
            )
            save_state()
            append_history()

        save_state()

    def send_order(self, side, units):
        req = ProtoOANewOrderReq()
        req.ctidTraderAccountId = ACCOUNT_ID
        req.symbolId = state.symbol_id
        req.orderType = ProtoOAOrderType.MARKET
        req.tradeSide = ProtoOATradeSide.BUY if side == "buy" else ProtoOATradeSide.SELL
        req.volume = units * 100
        req.comment = "BOS_v7_research_exact"
        req.label = f"BOSv7Exact_{SYMBOL_NAME}_{side}"
        self.client.send(req).addErrback(self._on_error)
        log.info(
            f"  MARKET ORDER ENVIADA | side={side.upper()} | units={units:,} | volume_proto={req.volume} | symbolId={state.symbol_id}"
        )

    def amend_position_sltp(self, position_id, sl_price, tp_price):
        req = ProtoOAAmendPositionSLTPReq()
        req.ctidTraderAccountId = ACCOUNT_ID
        req.positionId = position_id
        req.stopLoss = float(round(sl_price, 5))
        req.takeProfit = float(round(tp_price, 5))
        self.client.send(req).addErrback(self._on_error)
        log.info(
            f"  AMEND SLTP ENVIADO | positionId={position_id} | SL={req.stopLoss:.5f} | TP={req.takeProfit:.5f}"
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
        log.info(f"  CIERRE ENVIADO | positionId={position_id} | side={trade.side.upper()} | units={trade.units:,}")

    def start(self):
        self._connect()
        if not reactor.running:
            reactor.run()

# =============================================================================
# MAIN
# =============================================================================

def main():
    log.info("=" * 78)
    log.info("EURUSD BOS REVERSAL BOT v7 — RESEARCH EXACT")
    log.info("=" * 78)
    log.info(f"  Cuenta: {ACCOUNT_ID} ({'DEMO' if USE_DEMO else 'LIVE'})")
    log.info(f"  Symbol: {SYMBOL_NAME} | symbolId fallback={DEFAULT_SYMBOL_ID}")
    log.info(f"  EMAs LTF: {EMA_FAST_N}/{EMA_SLOW_N} | HTF: {HTF_FAST_N}/{HTF_SLOW_N} | SMA10={SMA10_N}")
    log.info(f"  BOS swing lookback: {BOS_SWING_LOOKBACK} barras | swing min={BOS_MIN_SWING_BP}bp")
    log.info(f"  Gates: ADX>={BOS_ADX_MIN} | emaDiffSigned>={BOS_EMA_DIFF_MIN_BP}bp | spreadZ<={SPREAD_Z_MAX}")
    log.info(f"  Depth filter: pullback_depth_vs_sma10_bp <= {DEPTH_FILTER_BP:.4f}bp")
    log.info(f"  Gestión: SL=clip(ATR14,{SL_MIN_BP},{SL_MAX_BP}) | TP={TP_RATIO}xSL | horizonte={MAX_BARS_OPEN} barras")
    log.info(f"  Sizing: notional = equity * leverage * margin_usage = equity * {LEVERAGE:.0f} * {MARGIN_USAGE:.2f}")
    log.info(f"  Safety margin cap: {MAX_MARGIN_PCT*100:.0f}% | 1 trade a la vez")
    log.info(f"  Pause bucket 3: {'ON' if USE_PAUSE_BUCKET_3 else 'OFF'}")
    log.info(f"  Spread max live tick: {MAX_SPREAD_BP_LIVE:.1f}bp")
    log.info(f"  Heartbeat: cada {HEARTBEAT_SECS}s")
    log.info(f"  Precarga histórica: hasta {HISTORICAL_BARS} barras H1 | lookback {HISTORICAL_LOOKBACK_DAYS} días")
    log.info("=" * 78)
    BosBot().start()

if __name__ == "__main__":
    main()
