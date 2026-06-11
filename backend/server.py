"""
XAUBot — FastAPI backend
Maintains a persistent WebSocket connection to Deriv API for 24/7 auto-trading.
Exposes REST endpoints for the PWA dashboard.
"""
import asyncio
import json
import os
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Optional, Any
from urllib.parse import quote

import websockets
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field

# ───────────────── ENV ─────────────────
load_dotenv()
MONGO_URL = os.environ["MONGO_URL"]
DB_NAME = os.environ["DB_NAME"]
DEFAULT_APP_ID = os.environ.get("DERIV_DEFAULT_APP_ID", "1089")

# ───────────────── MONGO ─────────────────
mongo_client: AsyncIOMotorClient = AsyncIOMotorClient(MONGO_URL)
db = mongo_client[DB_NAME]

# ───────────────── INDICATORS (port from JS) ─────────────────
def ema(arr, n):
    if not arr:
        return None
    k = 2 / (n + 1)
    e = arr[0]
    for v in arr[1:]:
        e = v * k + e * (1 - k)
    return e

def rsi(closes, n=14):
    if len(closes) < n + 1:
        return 50.0
    g = l = 0.0
    for i in range(len(closes) - n, len(closes)):
        d = closes[i] - closes[i - 1]
        if d > 0:
            g += d
        else:
            l += abs(d)
    rs = (g / n) / ((l / n) or 0.001)
    return 100 - 100 / (1 + rs)

def atr(cs, n=14):
    if len(cs) < 2:
        return 0.0
    trs = []
    for i in range(1, len(cs)):
        H, L, C = cs[i]["high"], cs[i]["low"], cs[i - 1]["close"]
        trs.append(max(H - L, abs(H - C), abs(L - C)))
    last = trs[-n:]
    return sum(last) / len(last) if last else 0.0

def bollinger(closes, n=20, k=2):
    sl = closes[-n:]
    m = sum(sl) / len(sl)
    std = (sum((b - m) ** 2 for b in sl) / len(sl)) ** 0.5
    return {"up": m + k * std, "mid": m, "dn": m - k * std}

def stoch_k(cs, n=5):
    sl = cs[-n:]
    h = max(c["high"] for c in sl)
    l = min(c["low"] for c in sl)
    if h == l:
        return 50.0
    return (sl[-1]["close"] - l) / (h - l) * 100

def compute_indicators(cs):
    if not cs or len(cs) < 30:
        return None
    closes = [c["close"] for c in cs]
    E9 = ema(closes[-30:], 9)
    E21 = ema(closes[-40:], 21)
    E50 = ema(closes[-60:], 50) if len(closes) >= 60 else E21
    RSI = rsi(closes)
    MACD = E9 - E21
    ms = []
    for i in range(10, len(closes)):
        e9_i = ema(closes[max(0, i - 30):i], 9)
        e21_i = ema(closes[max(0, i - 40):i], 21)
        ms.append(e9_i - e21_i)
    macd_hist = MACD - (ema(ms[-9:], 9) if len(ms) >= 9 else MACD)
    MOM = closes[-1] - closes[-6] if len(closes) >= 6 else 0
    ATR = atr(cs)
    BB = bollinger(closes)
    SK = stoch_k(cs)
    return {
        "E9": E9, "E21": E21, "E50": E50,
        "RSI": RSI, "MACD": MACD, "macdHist": macd_hist,
        "MOM": MOM, "ATR": ATR, "BB": BB, "SK": SK,
        "price": closes[-1],
    }

def score_signal(ind):
    s = 0
    r = ind["RSI"]
    if r < 30: s += 3
    elif r < 40: s += 2
    elif r < 48: s += 1
    elif r > 70: s -= 3
    elif r > 60: s -= 2
    elif r > 52: s -= 1
    s += 2 if ind["E9"] > ind["E21"] else -2
    s += 1 if ind["E21"] > ind["E50"] else -1
    mh = ind["macdHist"]
    if mh > 0.10: s += 2
    elif mh > 0: s += 1
    elif mh < -0.10: s -= 2
    elif mh < 0: s -= 1
    if ind["MOM"] > 0.5: s += 1
    if ind["MOM"] < -0.5: s -= 1
    if ind["price"] < ind["BB"]["dn"]: s += 1
    if ind["price"] > ind["BB"]["up"]: s -= 1
    direction = "BUY" if s >= 4 else "SELL" if s <= -4 else "WAIT"
    conf = min(100, round(abs(s) / 11 * 100))
    return {"dir": direction, "score": s, "conf": conf}

def in_session():
    h = datetime.now(timezone.utc).hour
    return 7 <= h < 17

def check_filters(atr_val):
    if atr_val < 0.5:
        return False, "Mercato piatto (ATR basso)"
    if not in_session():
        return False, f"Fuori sessione UTC {datetime.now(timezone.utc).hour}h"
    return True, ""

# ───────────────── DERIV CLIENT ─────────────────
class DerivClient:
    """Persistent Deriv WebSocket client. Single-user MVP."""

    def __init__(self):
        self.ws = None
        self.task: Optional[asyncio.Task] = None
        self.token: Optional[str] = None
        self.app_id: str = DEFAULT_APP_ID
        self.env: str = "demo"
        self.symbol = "frxXAUUSD"
        self.req_id = 100
        self.pending: dict[int, asyncio.Future] = {}
        self.connected = False
        self.authorized = False
        self.last_error: Optional[str] = None
        self.loginid: Optional[str] = None
        self.currency: str = "USD"
        self.balance: float = 0.0
        self.account_type: str = "demo"
        # Market state
        self.bid = 0.0
        self.ask = 0.0
        self.price = 0.0
        self.spread = 0.0
        self.candles: list[dict] = []
        self.minute_buffer: list[float] = []
        self.last_minute_ts: int = 0
        # Signal state
        self.indicators: dict = {}
        self.signal: dict = {"dir": "WAIT", "score": 0, "conf": 0, "confirmed": False, "pending": 0, "raw_dir": "WAIT"}
        self.pending_dir = "WAIT"
        self.pending_count = 0
        self.confirm_need = 5
        self.entry = self.tp = self.sl = 0.0
        self.filter_ok = False
        self.filter_reason = "Connessione..."
        # Trading state
        self.auto_mode = False
        self.last_auto_dir: Optional[str] = None
        self.open_contracts: dict[int, dict] = {}
        self.contracts_subscribed: set[int] = set()
        # Stats
        self.session_pnl = 0.0
        self.trades_total = 0
        self.trades_win = 0
        self.profit_total = 0.0
        # Logs (ring buffer)
        self.logs = deque(maxlen=200)

    def log(self, level: str, msg: str):
        ts = datetime.now(timezone.utc).isoformat()
        entry = {"ts": ts, "level": level, "msg": msg}
        self.logs.appendleft(entry)
        print(f"[{level}] {msg}")

    async def configure(self, token: str, app_id: str, env: str):
        self.token = token
        self.app_id = app_id or DEFAULT_APP_ID
        self.env = env
        self.last_error = None
        # Persist
        await db.config.update_one(
            {"_id": "main"},
            {"$set": {"token": token, "app_id": self.app_id, "env": env}},
            upsert=True,
        )
        await self.restart()

    async def load_persisted_config(self):
        cfg = await db.config.find_one({"_id": "main"})
        if cfg and cfg.get("token"):
            self.token = cfg["token"]
            self.app_id = cfg.get("app_id") or DEFAULT_APP_ID
            self.env = cfg.get("env", "demo")
            self.auto_mode = cfg.get("auto_mode", False)
            self.log("I", f"Config caricata da DB (env={self.env})")
            asyncio.create_task(self._run_loop())

    async def restart(self):
        # Stop existing
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass
        self.connected = False
        self.authorized = False
        # Start new
        self.task = asyncio.create_task(self._run_loop())

    async def _run_loop(self):
        backoff = 2
        while True:
            if not self.token:
                await asyncio.sleep(2)
                continue
            try:
                url = f"wss://ws.derivws.com/websockets/v3?app_id={quote(self.app_id)}"
                self.log("I", f"Connessione a {url}")
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    self.ws = ws
                    self.connected = True
                    self.last_error = None
                    backoff = 2
                    # Authorize
                    auth_resp = await self._send({"authorize": self.token})
                    if auth_resp.get("error"):
                        self.last_error = auth_resp["error"]["message"]
                        self.log("E", f"Auth fallita: {self.last_error}")
                        self.authorized = False
                        await asyncio.sleep(30)
                        continue
                    auth = auth_resp.get("authorize", {})
                    self.authorized = True
                    self.loginid = auth.get("loginid")
                    self.currency = auth.get("currency", "USD")
                    self.balance = float(auth.get("balance", 0))
                    is_virtual = bool(auth.get("is_virtual"))
                    self.account_type = "demo" if is_virtual else "real"
                    self.log("S", f"Autenticato {self.loginid} ({self.account_type}) saldo={self.balance}{self.currency}")
                    # Subscriptions
                    await self._send_no_wait({"balance": 1, "subscribe": 1})
                    await self._send_no_wait({"proposal_open_contract": 1, "subscribe": 1})
                    await self._send_no_wait({"ticks": self.symbol, "subscribe": 1})
                    # Candle history (no subscribe — we update from ticks)
                    hist = await self._send({
                        "ticks_history": self.symbol,
                        "adjust_start_time": 1,
                        "count": 120,
                        "end": "latest",
                        "start": 1,
                        "style": "candles",
                        "granularity": 60,
                    })
                    if hist.get("candles"):
                        self.candles = [
                            {"open": c["open"], "high": c["high"], "low": c["low"], "close": c["close"], "volume": 1}
                            for c in hist["candles"]
                        ]
                        self.log("S", f"{len(self.candles)} candele M1 caricate")
                    # Receive loop
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        await self._handle(msg)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.connected = False
                self.authorized = False
                self.last_error = str(e)
                self.log("E", f"WS disconnesso: {e}")
                await asyncio.sleep(backoff)
                backoff = min(60, backoff * 2)

    async def _send(self, payload: dict, timeout: float = 15):
        if not self.ws:
            raise RuntimeError("WS non connesso")
        self.req_id += 1
        rid = self.req_id
        payload["req_id"] = rid
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self.pending[rid] = fut
        await self.ws.send(json.dumps(payload))
        try:
            return await asyncio.wait_for(fut, timeout)
        finally:
            self.pending.pop(rid, None)

    async def _send_no_wait(self, payload: dict):
        if not self.ws:
            return
        self.req_id += 1
        payload["req_id"] = self.req_id
        await self.ws.send(json.dumps(payload))

    async def _handle(self, msg: dict):
        rid = msg.get("req_id")
        if rid in self.pending:
            self.pending[rid].set_result(msg)
            # Continue: subscription messages also carry req_id (resolves once + keeps coming)
        if msg.get("error"):
            err_msg = msg["error"].get("message", "?")
            self.log("E", f"API err: {err_msg}")
        mt = msg.get("msg_type")
        if mt == "tick":
            t = msg.get("tick", {})
            self.bid = float(t.get("bid", 0))
            self.ask = float(t.get("ask", 0))
            self.price = (self.bid + self.ask) / 2 if self.bid and self.ask else float(t.get("quote", 0))
            self.spread = self.ask - self.bid
            self._update_pseudo_candle(self.price)
            await self._maybe_signal()
        elif mt == "balance":
            b = msg.get("balance", {})
            self.balance = float(b.get("balance", self.balance))
            self.currency = b.get("currency", self.currency)
        elif mt == "proposal_open_contract":
            poc = msg.get("proposal_open_contract")
            if poc and poc.get("contract_id"):
                cid = poc["contract_id"]
                if poc.get("is_sold"):
                    if cid in self.open_contracts:
                        profit = float(poc.get("profit", 0))
                        self.trades_total = max(self.trades_total, self.trades_total)  # already counted on buy
                        if profit >= 0:
                            self.trades_win += 1
                        self.profit_total = round(self.profit_total + profit, 2)
                        self.session_pnl = round(self.session_pnl + profit, 2)
                        self.log("S" if profit >= 0 else "E", f"Chiuso {cid}: {profit:+.2f} {self.currency}")
                        await db.trades.insert_one({
                            "_id": str(uuid.uuid4()),
                            "contract_id": cid,
                            "profit": profit,
                            "ts": datetime.now(timezone.utc).isoformat(),
                            "details": poc,
                        })
                        self.open_contracts.pop(cid, None)
                else:
                    self.open_contracts[cid] = poc

    def _update_pseudo_candle(self, price: float):
        now_ms = int(time.time() * 1000)
        minute_ts = (now_ms // 60000) * 60000
        if not self.last_minute_ts or minute_ts > self.last_minute_ts:
            if self.last_minute_ts and self.minute_buffer:
                self.candles.append({
                    "open": self.minute_buffer[0],
                    "high": max(self.minute_buffer),
                    "low": min(self.minute_buffer),
                    "close": self.minute_buffer[-1],
                    "volume": len(self.minute_buffer),
                })
                if len(self.candles) > 200:
                    self.candles.pop(0)
            self.last_minute_ts = minute_ts
            self.minute_buffer = [price]
        else:
            self.minute_buffer.append(price)
            if self.candles:
                last = self.candles[-1]
                last["high"] = max(last["high"], price)
                last["low"] = min(last["low"], price)
                last["close"] = price

    async def _maybe_signal(self):
        if len(self.candles) < 30:
            return
        ind = compute_indicators(self.candles)
        if not ind:
            return
        self.indicators = ind
        sig = score_signal(ind)
        d = sig["dir"]
        if d == self.pending_dir:
            self.pending_count += 1
        else:
            self.pending_dir = d
            self.pending_count = 1
        confirmed = self.pending_count >= self.confirm_need
        ok, reason = check_filters(ind["ATR"])
        self.filter_ok = ok
        self.filter_reason = reason
        self.entry = self.price if d != "WAIT" else 0.0
        tp_p, sl_p = 10, 5
        self.tp = (self.entry + tp_p * 0.01) if d == "BUY" else (self.entry - tp_p * 0.01) if d == "SELL" else 0.0
        self.sl = (self.entry - sl_p * 0.01) if d == "BUY" else (self.entry + sl_p * 0.01) if d == "SELL" else 0.0
        self.signal = {
            "dir": d if confirmed else self.signal.get("dir", "WAIT"),
            "raw_dir": d,
            "score": sig["score"],
            "conf": sig["conf"],
            "confirmed": confirmed,
            "pending": self.pending_count,
        }
        # Auto trade
        if self.auto_mode and confirmed and self.filter_ok and d != "WAIT" and d != self.last_auto_dir:
            if len(self.open_contracts) < 3:
                self.log("S", f"AUTO trigger: {d} (score={sig['score']}, conf={sig['conf']}%)")
                try:
                    await self.place_order(d)
                    self.last_auto_dir = d
                except Exception as e:
                    self.log("E", f"AUTO order fallito: {e}")

    async def place_order(self, direction: str, stake: float = 1.0, multiplier: int = 10):
        if not self.authorized:
            raise RuntimeError("Non autenticato")
        contract_type = "MULTUP" if direction == "BUY" else "MULTDOWN"
        payload = {
            "buy": 1,
            "price": stake,
            "parameters": {
                "amount": stake,
                "basis": "stake",
                "contract_type": contract_type,
                "currency": self.currency,
                "symbol": self.symbol,
                "multiplier": multiplier,
            },
        }
        resp = await self._send(payload, timeout=20)
        if resp.get("error"):
            raise RuntimeError(resp["error"].get("message", "Errore ordine"))
        buy = resp.get("buy", {})
        cid = buy.get("contract_id")
        self.trades_total += 1
        self.log("S", f"Ordine {direction} #{cid} aperto a ${buy.get('buy_price')}")
        # Subscribe to specific contract for updates
        if cid:
            await self._send_no_wait({"proposal_open_contract": 1, "contract_id": cid, "subscribe": 1})
        return {"contract_id": cid, "buy_price": buy.get("buy_price")}

    async def close_contract(self, contract_id: int):
        resp = await self._send({"sell": int(contract_id), "price": 0}, timeout=20)
        if resp.get("error"):
            raise RuntimeError(resp["error"]["message"])
        return resp.get("sell", {})

    async def close_all(self):
        ids = list(self.open_contracts.keys())
        results = []
        for cid in ids:
            try:
                r = await self.close_contract(cid)
                results.append({"contract_id": cid, "ok": True, "sold_for": r.get("sold_for")})
            except Exception as e:
                results.append({"contract_id": cid, "ok": False, "error": str(e)})
        self.last_auto_dir = None
        return results

    def get_state(self):
        return {
            "connected": self.connected,
            "authorized": self.authorized,
            "configured": bool(self.token),
            "last_error": self.last_error,
            "loginid": self.loginid,
            "currency": self.currency,
            "balance": self.balance,
            "env": self.env,
            "account_type": self.account_type,
            "symbol": self.symbol,
            "bid": self.bid, "ask": self.ask, "price": self.price, "spread": self.spread,
            "candles_count": len(self.candles),
            "indicators": self.indicators,
            "signal": self.signal,
            "entry": self.entry, "tp": self.tp, "sl": self.sl,
            "filter_ok": self.filter_ok,
            "filter_reason": self.filter_reason,
            "confirm_need": self.confirm_need,
            "auto_mode": self.auto_mode,
            "positions": [
                {
                    "contract_id": p.get("contract_id"),
                    "contract_type": p.get("contract_type"),
                    "buy_price": p.get("buy_price"),
                    "profit": p.get("profit", 0),
                    "current_spot": p.get("current_spot"),
                    "entry_spot": p.get("entry_spot"),
                }
                for p in self.open_contracts.values()
            ],
            "stats": {
                "trades_total": self.trades_total,
                "trades_win": self.trades_win,
                "profit_total": self.profit_total,
                "session_pnl": self.session_pnl,
            },
            "logs": list(self.logs)[:60],
        }


client = DerivClient()

# ───────────────── FASTAPI ─────────────────
app = FastAPI(title="XAUBot API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def on_startup():
    await client.load_persisted_config()


class ConfigBody(BaseModel):
    token: str = Field(..., min_length=4)
    app_id: str = Field(default=DEFAULT_APP_ID)
    env: str = Field(default="demo")


class OrderBody(BaseModel):
    direction: str  # BUY / SELL
    stake: float = 1.0
    multiplier: int = 10


class AutoBody(BaseModel):
    enabled: bool


@app.get("/api/")
async def root():
    return {"service": "XAUBot", "status": "ok"}


@app.get("/api/state")
async def get_state():
    return client.get_state()


@app.post("/api/config")
async def set_config(body: ConfigBody):
    if body.env not in ("demo", "real"):
        raise HTTPException(400, "env must be demo or real")
    await client.configure(body.token.strip(), body.app_id.strip(), body.env)
    # Allow some time for handshake
    await asyncio.sleep(2.5)
    return client.get_state()


@app.post("/api/order")
async def place_order(body: OrderBody):
    if body.direction not in ("BUY", "SELL"):
        raise HTTPException(400, "direction must be BUY or SELL")
    if not client.authorized:
        raise HTTPException(400, "Non autenticato — configura prima il token")
    try:
        r = await client.place_order(body.direction, body.stake, body.multiplier)
        return {"ok": True, **r}
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/close_all")
async def close_all():
    if not client.authorized:
        raise HTTPException(400, "Non autenticato")
    return {"results": await client.close_all()}


@app.post("/api/auto")
async def set_auto(body: AutoBody):
    client.auto_mode = bool(body.enabled)
    if not body.enabled:
        client.last_auto_dir = None
    await db.config.update_one(
        {"_id": "main"}, {"$set": {"auto_mode": client.auto_mode}}, upsert=True
    )
    client.log("I", f"AUTO {'ON' if client.auto_mode else 'OFF'}")
    return {"auto_mode": client.auto_mode}


@app.get("/api/trades")
async def get_trades(limit: int = 50):
    cursor = db.trades.find().sort("ts", -1).limit(limit)
    out = []
    async for d in cursor:
        d.pop("details", None)
        out.append(d)
    return out


@app.post("/api/disconnect")
async def disconnect():
    """Drop saved config and disconnect."""
    await db.config.delete_one({"_id": "main"})
    client.token = None
    client.authorized = False
    await client.restart()
    return {"ok": True}
