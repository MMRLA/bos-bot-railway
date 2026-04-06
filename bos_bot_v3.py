"""
IC Markets — EURUSD BOS Reversal Bot v3
Mejoras sobre v2:
  - Precarga de 60 barras historicas al arrancar (sin espera de 48h)
  - Heartbeat cada 30 segundos para ver que el bot esta vivo
  - Log mas detallado del estado en cada barra
"""

import logging
import math
import time
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
ACCESS_TOKEN  = "gXLkNuEHXOM5UPJ8zDUEstkY8CkFxaLkZN2X441Omn0"
ACCOUNT_ID    = 45971561
USE_DEMO      = True

# Parametros del sistema
SYMBOL_NAME   = "EURUSD"
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

# Cuantas barras historicas pedir al arrancar
# 60 barras = 60 horas — suficiente para todos los indicadores
HISTORICAL_BARS = 500
HISTORICAL_LOOKBACK_DAYS = 14

# Heartbeat: imprimir estado cada N segundos
HEARTBEAT_SECS = 30

# Timeout para auth de cuenta
ACCOUNT_AUTH_TIMEOUT_SECS = 10

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bos_bot_v3.log", encoding="utf-8"),
    ]
)
log = logging.getLogger("BOS_v3")

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
    entry_time:    datetime = field(
        default_factory=lambda: datetime.now(timezone.utc))

# =============================================================================
# ESTADO
# =============================================================================

class BotState:
    def __init__(self):
        self.bars: List[dict]              = []
        self.max_bars                      = 200
        self.current_bar: Optional[dict]   = None
        self.current_bar_start: Optional[int] = None
        self.open_trades: Dict[int, OpenTrade] = {}
        self.last_bos_bar                  = -100
        self.equity                        = 2000.0
        self.symbol_id                     = 1
        self.pip_size                      = 0.0001
        self.trades_total                  = 0
        self.trades_win                    = 0
        self.trades_loss                   = 0
        self.pnl_bp_total                  = 0.0
        self._pending: Optional[dict]      = None
        self.historical_loaded             = False
        self.tick_count                    = 0   # para heartbeat

    @property
    def n_bars(self): return len(self.bars)

    @property
    def bars_since_last_bos(self): return self.n_bars - self.last_bos_bar

    @property
    def n_open_trades(self): return len(self.open_trades)

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
# INDICADORES
# =============================================================================

def ema_series(values, span):
    if len(values) < span: return float("nan")
    k = 2.0 / (span + 1)
    ema = float(values[0])
    for v in values[1:]: ema = v * k + ema * (1 - k)
    return ema

def calc_atr(bars, period):
    if len(bars) < period + 1: return float("nan")
    trs = []
    for i in range(1, len(bars)):
        h, l, pc = bars[i]["high"], bars[i]["low"], bars[i-1]["close"]
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    atr = float(np.mean(trs[:period]))
    for tr in trs[period:]: atr = (atr*(period-1)+tr)/period
    return atr

def calc_adx(bars, period):
    if len(bars) < period * 2: return float("nan")
    plus_dms, minus_dms, trs = [], [], []
    for i in range(1, len(bars)):
        up   = bars[i]["high"] - bars[i-1]["high"]
        down = bars[i-1]["low"] - bars[i]["low"]
        plus_dms.append(up   if (up>down and up>0)   else 0.0)
        minus_dms.append(down if (down>up and down>0) else 0.0)
        h, l, pc = bars[i]["high"], bars[i]["low"], bars[i-1]["close"]
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    def ws(data, n):
        s = float(np.sum(data[:n])); result = [s]
        for v in data[n:]: s=s-s/n+v; result.append(s)
        return result
    sm_tr=ws(trs,period); sm_p=ws(plus_dms,period); sm_m=ws(minus_dms,period)
    dxs=[]
    for st,sp,sm in zip(sm_tr,sm_p,sm_m):
        if st<1e-12: continue
        dip,dim=100*sp/st,100*sm/st
        denom=dip+dim
        dxs.append(100*abs(dip-dim)/denom if denom>1e-12 else 0.0)
    if len(dxs)<period: return float("nan")
    adx=float(np.mean(dxs[:period]))
    for dx in dxs[period:]: adx=(adx*(period-1)+dx)/period
    return adx

def px_to_bp(diff, mid): return (diff/mid)*10_000

def compute_indicators():
    bars=state.bars; n=len(bars); mid=bars[-1]["close"] if bars else 1.1
    nan=float("nan")
    r={"ema_fast":nan,"ema_slow":nan,"ltf_side":0,"atr_bp":nan,"adx":nan,
       "swing_hi":nan,"swing_lo":nan,"swing_range_bp":nan}
    if n < state.n_bars_needed(): return r
    closes=[b["close"] for b in bars]
    r["ema_fast"]=ema_series(closes[-EMA_FAST_N:],EMA_FAST_N)
    r["ema_slow"]=ema_series(closes[-EMA_SLOW_N:],EMA_SLOW_N)
    if not math.isnan(r["ema_fast"]) and not math.isnan(r["ema_slow"]):
        r["ltf_side"]=1 if r["ema_fast"]>r["ema_slow"] else -1
    atr_raw=calc_atr(bars[-(ATR_PERIOD+1):],ATR_PERIOD)
    if not math.isnan(atr_raw): r["atr_bp"]=px_to_bp(atr_raw,mid)
    r["adx"]=calc_adx(bars[-(ADX_PERIOD*3):],ADX_PERIOD)
    if n>=SWING_LOOKBACK+1:
        prev=bars[-(SWING_LOOKBACK+1):-1]
        r["swing_hi"]=max(b["high"] for b in prev)
        r["swing_lo"]=min(b["low"]  for b in prev)
        r["swing_range_bp"]=px_to_bp(r["swing_hi"]-r["swing_lo"],mid)
    return r

# =============================================================================
# LOGICA BOS
# =============================================================================

def evaluate_bos_signal(ind, bar):
    close=bar["close"]
    for key in ["ema_fast","ema_slow","atr_bp","adx","swing_hi","swing_lo","swing_range_bp"]:
        if math.isnan(ind.get(key,float("nan"))): return None
    if ind["swing_range_bp"] < MIN_SWING_BP: return None
    if ind["atr_bp"]         < ATR_MIN_BP:   return None
    if ind["adx"]            < ADX_MIN_VAL:  return None
    if state.bars_since_last_bos < COOLDOWN_BARS: return None
    bar_dt=datetime.fromtimestamp(bar["time"],tz=timezone.utc)
    if bar_dt.weekday()==6: return None
    if bar_dt.weekday()==4 and bar_dt.hour>=20: return None
    ltf=ind["ltf_side"]
    if ltf==1  and close>ind["swing_hi"]:
        log.info(f"  BOS ALCISTA: {close:.5f} > swing_hi={ind['swing_hi']:.5f}")
        return "sell"
    if ltf==-1 and close<ind["swing_lo"]:
        log.info(f"  BOS BAJISTA: {close:.5f} < swing_lo={ind['swing_lo']:.5f}")
        return "buy"
    return None

def calc_sl_tp(side, entry, atr_bp):
    sl_bp=max(SL_MIN_BP,min(SL_MAX_BP,atr_bp*ATR_MULT))
    tp_bp=sl_bp*TP_RATIO
    sl_d=sl_bp*state.pip_size; tp_d=tp_bp*state.pip_size
    if side=="sell": return entry+sl_d, entry-tp_d, sl_bp, tp_bp
    else:            return entry-sl_d, entry+tp_d, sl_bp, tp_bp

def calc_units(sl_bp, entry_price):
    risk_eur=state.equity*RISK_PCT
    units=max(1000,int(risk_eur/(sl_bp*state.pip_size)/1000)*1000)
    margin=units*entry_price/LEVERAGE
    return units, margin

def execute_signal(signal, bar, ind, bot):
    entry=bar["ask"] if signal=="buy" else bar["bid"]
    sl_price,tp_price,sl_bp,tp_bp=calc_sl_tp(signal,entry,ind["atr_bp"])
    units,margin_needed=calc_units(sl_bp,entry)
    if margin_needed>state.margin_available:
        log.warning(f"  SEÑAL OMITIDA: margen insuficiente "
                    f"(necesario={margin_needed:.0f}€, disponible={state.margin_available:.0f}€)")
        return
    log.info(f"\n  *** {signal.upper()} | Entry={entry:.5f} "
             f"SL={sl_price:.5f}({sl_bp:.1f}bp) TP={tp_price:.5f}({tp_bp:.1f}bp) "
             f"Units={units:,} Margen={margin_needed:.0f}€ ***")
    state._pending={
        "side":signal,"entry_price":entry,"sl_price":sl_price,"tp_price":tp_price,
        "sl_bp":sl_bp,"tp_bp":tp_bp,"units":units,"margin":margin_needed,
        "entry_bar_idx":state.n_bars}
    state.last_bos_bar=state.n_bars
    bot.send_order(signal,units,sl_price,tp_price)

# =============================================================================
# BARRAS
# =============================================================================

def get_bar_start(ts): return (ts//BAR_SECONDS)*BAR_SECONDS

def finalize_current_bar(bot):
    if state.current_bar is None or state.current_bar_start is None:
        return

    completed=dict(state.current_bar)
    if completed.get("time", 0) <= 0:
        log.warning("Barra invalida detectada; se omite el cierre.")
        return

    state.add_bar(completed)
    bar_dt=datetime.fromtimestamp(completed["time"],tz=timezone.utc)

    log.info(f"\n{'='*65}")
    log.info(f"BARRA CERRADA: {bar_dt.strftime('%Y-%m-%d %H:%M')} UTC")
    log.info(f"  O={completed['open']:.5f} H={completed['high']:.5f} "
             f"L={completed['low']:.5f} C={completed['close']:.5f}")
    log.info(f"  Barras acumuladas: {state.n_bars} | "
             f"Trades abiertos: {state.n_open_trades} | "
             f"PnL: {state.pnl_bp_total:+.1f}bp")

    on_bar_close(completed, bot)

def on_tick(bid, ask, ts, bot):
    state.tick_count += 1
    mid=(bid+ask)/2
    bar_start=get_bar_start(ts)

    if state.current_bar_start is None:
        state.current_bar_start=bar_start
        state.current_bar={"time":bar_start,"open":mid,"high":mid,
                           "low":mid,"close":mid,"bid":bid,"ask":ask}
        return

    if bar_start < state.current_bar_start:
        return

    if bar_start==state.current_bar_start:
        cb=state.current_bar
        cb["high"]=max(cb["high"],mid); cb["low"]=min(cb["low"],mid)
        cb["close"]=mid; cb["bid"]=bid; cb["ask"]=ask
    else:
        finalize_current_bar(bot)

        state.current_bar_start=bar_start
        state.current_bar={"time":bar_start,"open":mid,"high":mid,
                           "low":mid,"close":mid,"bid":bid,"ask":ask}

def on_bar_close(bar, bot):
    # Cerrar trades por horizonte maximo
    to_close=[]
    for pid,trade in state.open_trades.items():
        bars_open=state.n_bars-trade.entry_bar_idx
        if bars_open>=MAX_BARS_OPEN:
            log.warning(f"  Trade {pid}: horizonte max ({MAX_BARS_OPEN}h) — cerrando")
            to_close.append(pid)
    for pid in to_close:
        bot.close_position(pid)

    # Evaluar señal
    needed=state.n_bars_needed()
    if not state.is_warmed_up():
        faltan=needed-state.n_bars
        log.info(f"  Calentando: {state.n_bars}/{needed} barras "
                 f"(faltan {faltan} barras = {faltan}h)")
        return

    ind=compute_indicators()
    side_str="▲UP" if ind["ltf_side"]==1 else "▼DOWN"
    log.info(f"  EMA_f={ind['ema_fast']:.5f} EMA_s={ind['ema_slow']:.5f} {side_str} | "
             f"ATR={ind['atr_bp']:.1f}bp | ADX={ind['adx']:.1f} | "
             f"Swing [{ind['swing_lo']:.5f}-{ind['swing_hi']:.5f}]")

    signal=evaluate_bos_signal(ind,bar)
    if signal:
        execute_signal(signal,bar,ind,bot)
    else:
        # Mostrar por que no se opera (si los indicadores son validos)
        if not math.isnan(ind["atr_bp"]):
            reasons=[]
            if ind["atr_bp"]         < ATR_MIN_BP:   reasons.append(f"ATR={ind['atr_bp']:.1f}<{ATR_MIN_BP}")
            if ind["adx"]            < ADX_MIN_VAL:  reasons.append(f"ADX={ind['adx']:.1f}<{ADX_MIN_VAL}")
            if ind["swing_range_bp"] < MIN_SWING_BP: reasons.append(f"swing={ind['swing_range_bp']:.1f}bp<{MIN_SWING_BP}")
            if state.bars_since_last_bos < COOLDOWN_BARS: reasons.append("cooldown")
            if not reasons: reasons.append("sin BOS")
            log.info(f"  Sin señal: {' | '.join(reasons)}")

    log.info(f"  Margen: {state.total_margin_used:.0f}€/{state.equity*MAX_MARGIN_PCT:.0f}€ "
             f"| Stats: {state.trades_total}T WR={state.trades_win/max(state.trades_total,1)*100:.0f}%")

# =============================================================================
# CLIENTE CTRADER
# =============================================================================

class BosBot:
    def __init__(self):
        host=EndPoints.PROTOBUF_DEMO_HOST if USE_DEMO else EndPoints.PROTOBUF_LIVE_HOST
        self.client=Client(host,EndPoints.PROTOBUF_PORT,TcpProtocol)
        self.client.setConnectedCallback(self._on_connected)
        self.client.setDisconnectedCallback(self._on_disconnected)
        self.client.setMessageReceivedCallback(self._on_message)
        self.account_auth_ok=False
        self.heartbeat_started=False
        self.hourly_timer_started=False
        self.ready_for_ticks=False
        self.last_bid=None
        self.last_ask=None
        self.last_ts=None
        log.info(f"Bot v3 inicializado | Servidor: {host}")

    def _connect(self):
        self.client.startService()

    def _reset_market_state(self):
        state.bars = []
        state.current_bar = None
        state.current_bar_start = None
        state.historical_loaded = False
        state.tick_count = 0
        self.ready_for_ticks = False
        self.last_bid = None
        self.last_ask = None
        self.last_ts = None

    def _seed_current_bar_after_historical(self, fallback_close):
        now_ts = self.last_ts if self.last_ts is not None else int(datetime.now(timezone.utc).timestamp())
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

    def _schedule_hourly_bar_timer(self):
        if self.hourly_timer_started:
            return

        self.hourly_timer_started = True

        def schedule_next():
            now_ts = int(datetime.now(timezone.utc).timestamp())
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

        now_ts = int(datetime.now(timezone.utc).timestamp())
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
            log.info("Timer UTC: cierre/rollover horario H1 ejecutado.")

    def _on_connected(self, client):
        log.info("Conectado. Autenticando aplicacion...")
        req=ProtoOAApplicationAuthReq()
        req.clientId=CLIENT_ID
        req.clientSecret=CLIENT_SECRET
        client.send(req).addErrback(self._on_error)

    def _on_disconnected(self, client, reason):
        self.ready_for_ticks = False
        log.warning(f"Desconectado: {reason}. Reconectando en 15s...")
        reactor.callLater(15, self._connect)

    def _on_error(self, failure):
        log.error(f"Error: {failure}")

    def _safe_extract(self, message):
        try:
            return Protobuf.extract(message)
        except Exception as e:
            log.warning(f"No se pudo extraer payloadType={getattr(message, 'payloadType', 'NA')}: {e}")
            return None

    def _obj_name(self, obj):
        return obj.__class__.__name__ if obj is not None else "Unknown"

    def _extract_account_ids(self, res):
        account_ids=[]

        for attr in ("ctidTraderAccountId", "accountId", "accountIds"):
            if hasattr(res, attr):
                try:
                    vals=list(getattr(res, attr))
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
                    items=list(getattr(res, attr))
                except Exception:
                    items=[]
                for item in items:
                    for key in ("ctidTraderAccountId", "accountId"):
                        if hasattr(item, key):
                            try:
                                account_ids.append(int(getattr(item, key)))
                                break
                            except Exception:
                                pass

        unique_ids=[]
        seen=set()
        for x in account_ids:
            if x not in seen:
                unique_ids.append(x)
                seen.add(x)
        return unique_ids

    def _request_account_list(self):
        log.info("App autenticada. Solicitando cuentas asociadas al token...")
        req=ProtoOAGetAccountListByAccessTokenReq()
        req.accessToken=ACCESS_TOKEN
        self.client.send(req).addErrback(self._on_error)

    def _authenticate_account(self):
        log.info(f"Autenticando cuenta {ACCOUNT_ID}...")
        self.account_auth_ok=False
        req=ProtoOAAccountAuthReq()
        req.ctidTraderAccountId=ACCOUNT_ID
        req.accessToken=ACCESS_TOKEN
        self.client.send(req).addErrback(self._on_error)
        reactor.callLater(ACCOUNT_AUTH_TIMEOUT_SECS, self._check_account_auth_timeout)

    def _check_account_auth_timeout(self):
        if not self.account_auth_ok:
            log.error("Timeout esperando autenticacion de cuenta. "
                      "Revisa ACCESS_TOKEN, ACCOUNT_ID y asociacion token-cuenta.")

    def _on_message(self, client, message):
        t=message.payloadType
        obj=self._safe_extract(message)
        obj_name=self._obj_name(obj)

        log.info(f"[MSG] payloadType={t} type={obj_name}")

        if t==50 or obj_name=="ProtoOAErrorRes":
            log.error(
                "Error servidor | "
                f"payloadType={t} | "
                f"errorCode={getattr(obj, 'errorCode', None)} | "
                f"description={getattr(obj, 'description', None)} | "
                f"accountId={getattr(obj, 'ctidTraderAccountId', None)}"
            )
            return

        if t==2101 or obj_name=="ProtoOAApplicationAuthRes":
            self._request_account_list()
            return

        if obj_name=="ProtoOAGetAccountListByAccessTokenRes":
            account_ids=self._extract_account_ids(obj)
            if account_ids:
                log.info(f"Cuentas asociadas al token: {account_ids}")
                if ACCOUNT_ID not in account_ids:
                    log.error(f"El ACCOUNT_ID={ACCOUNT_ID} no aparece entre las cuentas del token.")
                    log.error("Corrige ACCOUNT_ID o genera un ACCESS_TOKEN de la cuenta correcta.")
                    return
            else:
                log.warning("No se pudieron extraer cuentas del token desde la respuesta. "
                            "Intentando autenticar la cuenta configurada igualmente.")
            self._authenticate_account()
            return

        if t==2103 or obj_name=="ProtoOAAccountAuthRes":
            self.account_auth_ok=True
            self._reset_market_state()
            log.info(f"Cuenta {ACCOUNT_ID} autenticada correctamente.")
            if state.symbol_id == 1:
                log.warning("state.symbol_id sigue valiendo 1. "
                            "Si no llegan spots/historico, revisa que el symbolId de EURUSD sea el correcto en esta cuenta.")
            self._reconcile()
            self._load_historical_bars()
            self._subscribe_spots()
            if not self.heartbeat_started:
                self._start_heartbeat()
                self.heartbeat_started=True
            if not self.hourly_timer_started:
                self._schedule_hourly_bar_timer()
            return

        if t==2138 or obj_name=="ProtoOAGetTrendbarsRes":
            self._on_historical_bars(message)
            return

        if t==2131 or obj_name=="ProtoOASpotEvent":
            self._on_spot(message)
            return

        if t==2126 or obj_name=="ProtoOAExecutionEvent":
            self._on_execution(message)
            return

        if t==2125 or obj_name=="ProtoOAReconcileRes":
            self._on_reconcile(message)
            return

    # ------------------------------------------------------------------
    # HEARTBEAT — imprime estado cada 30 segundos
    # ------------------------------------------------------------------
    def _start_heartbeat(self):
        def heartbeat():
            now=datetime.now(timezone.utc).strftime("%H:%M:%S")
            needed=state.n_bars_needed()
            mid=state.current_bar["close"] if state.current_bar else 0.0
            barra_actual=""
            if state.current_bar_start is not None:
                prox=state.current_bar_start+BAR_SECONDS
                secs_left=prox-int(datetime.now(timezone.utc).timestamp())
                barra_actual=f" | Prox.barra en {max(0,secs_left//60)}m{max(0,secs_left%60)}s"

            if not state.is_warmed_up():
                faltan=needed-state.n_bars
                log.info(f"[{now}] CALENTANDO {state.n_bars}/{needed} barras "
                         f"(~{faltan}h para operar) | "
                         f"Precio={mid:.5f} | Ticks={state.tick_count}{barra_actual}")
            else:
                log.info(f"[{now}] ACTIVO | Precio={mid:.5f} | "
                         f"Barras={state.n_bars} | Trades={state.n_open_trades} | "
                         f"PnL={state.pnl_bp_total:+.1f}bp{barra_actual}")
            reactor.callLater(HEARTBEAT_SECS, heartbeat)

        reactor.callLater(HEARTBEAT_SECS, heartbeat)
        log.info(f"Heartbeat activo (cada {HEARTBEAT_SECS}s)")

    # ------------------------------------------------------------------
    # PRECARGA HISTORICA — pide las ultimas N barras de H1
    # ------------------------------------------------------------------
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
                f"Solicitando historico H1 | from={from_ms} to={now_ms} "
                f"| lookback={HISTORICAL_LOOKBACK_DAYS} dias | count={HISTORICAL_BARS}"
            )
        except Exception as e:
            log.warning(f"Precarga no disponible: {e}. "
                        f"El bot calentara con datos en vivo (~{state.n_bars_needed()}h).")

    def _on_historical_bars(self, message):
        try:
            res=Protobuf.extract(message)
            bars_data=list(res.trendbar)
            if not bars_data:
                log.warning("El servidor no devolvio barras historicas.")
                return

            loaded=[]
            for tb in bars_data:
                d=100_000

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
                    "time":  tb.utcTimestampInMinutes * 60,
                    "open":  open_,
                    "high":  high,
                    "low":   low,
                    "close": close,
                    "bid":   close,
                    "ask":   close,
                })

            loaded.sort(key=lambda x: x["time"])
            for bar in loaded:
                state.add_bar(bar)
            state.historical_loaded=True

            fallback_close = loaded[-1]["close"]
            self._seed_current_bar_after_historical(fallback_close)
            self.ready_for_ticks = True

            needed=state.n_bars_needed()
            log.info(f"Precarga completada: {len(loaded)} barras H1 cargadas.")
            if state.is_warmed_up():
                log.info(f"INDICADORES LISTOS. El bot puede operar desde la proxima barra.")
                ind=compute_indicators()
                if not math.isnan(ind.get("atr_bp",float("nan"))):
                    log.info(f"  Estado actual: ATR={ind['atr_bp']:.1f}bp | "
                             f"ADX={ind['adx']:.1f} | "
                             f"Lado={'UP' if ind['ltf_side']==1 else 'DOWN'}")
            else:
                faltan=needed-state.n_bars
                log.info(f"Calentamiento parcial: {state.n_bars}/{needed}. "
                         f"Faltan {faltan} barras (~{faltan}h con datos en vivo).")
        except Exception as e:
            log.error(f"Error procesando barras historicas: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # SPOTS, ORDENES, RECONCILE
    # ------------------------------------------------------------------
    def _reconcile(self):
        req=ProtoOAReconcileReq()
        req.ctidTraderAccountId=ACCOUNT_ID
        self.client.send(req).addErrback(self._on_error)

    def _subscribe_spots(self):
        req=ProtoOASubscribeSpotsReq()
        req.ctidTraderAccountId=ACCOUNT_ID
        req.symbolId.append(state.symbol_id)
        self.client.send(req).addErrback(self._on_error)
        log.info(f"Suscrito a {SYMBOL_NAME} (symbolId={state.symbol_id})")

    def _on_spot(self, message):
        spot=Protobuf.extract(message)
        if spot.symbolId!=state.symbol_id:
            return

        d=100_000
        bid=spot.bid/d
        ask=spot.ask/d
        if bid<=0 or ask<=0:
            return

        ts=int(spot.timestamp/1000)

        self.last_bid = bid
        self.last_ask = ask
        self.last_ts = ts

        if (ask-bid)/bid*10_000>MAX_SPREAD_BP:
            return

        if not self.ready_for_ticks:
            return

        on_tick(bid,ask,ts,self)

    def _on_execution(self, message):
        ev=Protobuf.extract(message)
        pos=ev.position if hasattr(ev,"position") else None
        if not pos: return
        pos_id=pos.positionId

        if pos.positionStatus==1 and state._pending:  # ABIERTO
            p=state._pending
            trade=OpenTrade(
                position_id=pos_id, side=p["side"],
                entry_price=p["entry_price"], sl_price=p["sl_price"],
                tp_price=p["tp_price"], sl_bp=p["sl_bp"], tp_bp=p["tp_bp"],
                units=p["units"], entry_bar_idx=p["entry_bar_idx"],
                margin_used=p["margin"])
            state.open_trades[pos_id]=trade
            state._pending=None
            state.trades_total+=1
            log.info(f"  TRADE ABIERTO ID={pos_id} {trade.side.upper()} "
                     f"{trade.units:,}u | Activos: {state.n_open_trades}")

        elif pos.positionStatus==2 and pos_id in state.open_trades:  # CERRADO
            trade=state.open_trades.pop(pos_id)
            cp=getattr(pos,"price",trade.entry_price)
            pnl_bp=((trade.entry_price-cp) if trade.side=="sell"
                    else (cp-trade.entry_price))/trade.entry_price*10_000
            state.pnl_bp_total+=pnl_bp
            if pnl_bp>0: state.trades_win+=1
            else:        state.trades_loss+=1
            wr=state.trades_win/max(state.trades_total,1)*100
            log.info(f"  TRADE CERRADO ID={pos_id} {trade.side.upper()} | "
                     f"P&L={pnl_bp:+.1f}bp | WR={wr:.0f}% | "
                     f"Total={state.pnl_bp_total:+.1f}bp | Activos: {state.n_open_trades}")

    def _on_reconcile(self, message):
        rec=Protobuf.extract(message)
        n=len(rec.position) if rec.position else 0
        log.info(f"Reconcile: {n} posiciones abiertas al conectar.")

    def send_order(self, side, units, sl_price, tp_price):
        req=ProtoOANewOrderReq()
        req.ctidTraderAccountId=ACCOUNT_ID
        req.symbolId=state.symbol_id
        req.orderType=ProtoOAOrderType.MARKET
        req.tradeSide=ProtoOATradeSide.BUY if side=="buy" else ProtoOATradeSide.SELL
        req.volume=units*100
        d=100_000
        req.stopLoss=int(sl_price*d)
        req.takeProfit=int(tp_price*d)
        self.client.send(req).addErrback(self._on_error)

    def close_position(self, position_id):
        if position_id not in state.open_trades: return
        trade=state.open_trades[position_id]
        req=ProtoOAClosePositionReq()
        req.ctidTraderAccountId=ACCOUNT_ID
        req.positionId=position_id
        req.volume=trade.units*100
        self.client.send(req).addErrback(self._on_error)

    def start(self):
        self._connect()
        if not reactor.running:
            reactor.run()

# =============================================================================
# MAIN
# =============================================================================

def main():
    log.info("="*65)
    log.info("EURUSD BOS REVERSAL BOT v3")
    log.info("="*65)
    log.info(f"  Cuenta: {ACCOUNT_ID} ({'DEMO' if USE_DEMO else 'LIVE'})")
    log.info(f"  Filtros: ATR>={ATR_MIN_BP}bp + ADX>={ADX_MIN_VAL}")
    log.info(f"  SL: ATR*{ATR_MULT} clip[{SL_MIN_BP}-{SL_MAX_BP}bp] | TP: {TP_RATIO}:1")
    log.info(f"  Riesgo: {RISK_PCT*100:.1f}% | MarginCap: {MAX_MARGIN_PCT*100:.0f}%")
    log.info(f"  Heartbeat: cada {HEARTBEAT_SECS}s")
    log.info(f"  Precarga historica: hasta {HISTORICAL_BARS} barras H1 | lookback {HISTORICAL_LOOKBACK_DAYS} dias")
    log.info("="*65)
    BosBot().start()

if __name__=="__main__":
    main()