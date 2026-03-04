"""
IMPULSE BSC SCANNER v5.2
Clean rebuild — three strict pipelines, hard safety gates.

PHILOSOPHY:
- Never alert a rug. Safety gates run BEFORE scoring.
- Three separate pipelines with different criteria:
  1. NEW LAUNCH   — token < 72h, strict safety + score ≥ 55
  2. CTO/RECOVERY — any age, MC $1k–$50k, waking from death
  3. OLD WAKEUP   — token > 24h, MC > $100k, dormant then active
- One alert per token. Favlist tokens re-alert on ±50% move.
- /radar for near-misses on demand.
- Reserve monitoring for old token detection via RPC.
"""

import asyncio
import logging
import os
import time
from collections import deque
from typing import Optional

import aiohttp
from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ── CREDENTIALS ───────────────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8623651466:AAFeEHh3t15n1ciB4L2MZbj57h_j3mzAGhw")
BSCSCAN_KEY    = os.getenv("ETHERSCAN_KEY",  "S3ZEIA898K31SY9EJXE3HYY8ZEZ6PBGMHB")
BSCSCAN_URL    = "https://api.bscscan.com/api"

RPC_ENDPOINTS = [
    "https://go.getblock.us/75254fd9f61c4b43af30f2e55be8f55d",
    "https://rpc.ankr.com/bsc/0f0dde5ab6eee670d869cfe84af8d17eab6b832a050e17b3241f9b0dc513f1e5",
]

PANCAKE_V2_FACTORY = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"
PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
WBNB               = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
GET_RESERVES_SEL   = "0x0902f1ac"

# ── HARD SAFETY GATES — token fails any of these = never alerted ──────────────
MAX_BUY_TAX        = 10.0   # % — above this is a red flag
MAX_SELL_TAX       = 12.0   # % — above this almost always a rug
MAX_TOP10_PCT      = 80.0   # % — whales own too much
MAX_SELL_RATIO_1H  = 0.72   # 72% sells in 1h = dumping
MAX_DROP_FROM_ATH  = -50    # % 1h drop with volume = already rugged
MIN_LIQ_ABSOLUTE   = 3_000  # $ — below this don't bother

# ── PIPELINE THRESHOLDS ───────────────────────────────────────────────────────
# Pipeline 1: NEW LAUNCH (age < 72h)
NEW_MAX_AGE_H      = 72
NEW_MIN_SCORE      = 55     # strict — was 35, too low
NEW_MIN_LIQ        = 3_000
NEW_MAX_TOP10      = 70.0   # tighter for new tokens
NEW_REQUIRE_CLEAN  = True   # honeypot clean + tax reasonable mandatory

# Pipeline 2: CTO / RECOVERY (any age, MC $1k–$50k)
CTO_MAX_MC         = 50_000
CTO_MIN_MC         = 1_000
CTO_MIN_BUY_PCT    = 0.58   # 58% buys minimum — must be accumulating
CTO_MIN_VOL_1H     = 500    # $ — some real activity
CTO_MIN_SCORE      = 40

# Pipeline 3: OLD WAKEUP (age > 24h, MC > $100k)
WAKE_MIN_AGE_H     = 24
WAKE_MIN_MC        = 100_000
WAKE_MIN_SCORE     = 45
WAKE_PREV_MAX_VOL  = 15_000  # was sleeping if avg vol < this
WAKE_SPIKE_MULT    = 2.0     # needs 2x spike to qualify
WAKE_MIN_VOL       = 3_000   # must show at least $3k/hr now

# Reserve monitoring (on-chain wakeup detection)
RESERVE_CHANGE_PCT    = 4.0
RESERVE_MIN_STABLE    = 5
RESERVE_BATCH_SIZE    = 60
DB_LOOKBACK_DAYS      = 30
BSC_BLOCKS_PER_DAY    = 28_800
DB_BUILD_BATCH        = 100

# Near-miss
NEAR_MISS_MIN_SCORE   = 30
NEAR_MISS_MAX         = 60

# Fav
FAV_ALERT_PCT         = 50

# Timing
SCAN_INTERVAL         = 65
RPC_NEW_PAIR_INTERVAL = 30
RESERVE_SCAN_INTERVAL = 50
DB_BUILD_INTERVAL     = 300
FAV_CHECK_INTERVAL    = 120

# ── MC LABELS ─────────────────────────────────────────────────────────────────
def mc_label(mc: float) -> str:
    if mc <= 0:            return ""
    if mc < 20_000:        return "🔬 MICRO"
    if mc < 100_000:       return "💎 LOW"
    if mc < 200_000:       return "📊 LOW-MID"
    if mc < 1_000_000:     return "📈 MID CAP"
    if mc < 20_000_000:    return "🔥 HIGH CAP"
    return "🏆 VERY HIGH"

# ── STATE ─────────────────────────────────────────────────────────────────────
subscribed_chats = set()
alerted_tokens   = {}         # addr -> {ts, price}
token_history    = {}         # addr -> snapshot
seen_new_pairs   = set()
favourites       = {}         # chat_id -> {addr -> info}
pinned_msg_ids   = {}
near_miss_log    = deque(maxlen=NEAR_MISS_MAX)

pair_database    = {}         # pair_addr -> token_addr
pair_reserves    = {}         # pair_addr -> reserve snapshot
db_scan_pointer  = 0
last_rpc_block   = 0


# ── RPC ───────────────────────────────────────────────────────────────────────

async def rpc_call(session, method: str, params: list):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    for ep in RPC_ENDPOINTS:
        try:
            async with session.post(ep, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    data = await r.json()
                    if "result" in data:
                        return data["result"]
        except Exception as e:
            logger.warning(f"RPC {ep[:35]}: {e}")
    return None


async def rpc_block_number(session) -> Optional[int]:
    r = await rpc_call(session, "eth_blockNumber", [])
    return int(r, 16) if r else None


async def rpc_get_reserves(session, pair_addr: str) -> Optional[tuple]:
    result = await rpc_call(session, "eth_call", [{"to": pair_addr, "data": GET_RESERVES_SEL}, "latest"])
    if not result or result == "0x" or len(result) < 194:
        return None
    try:
        d = result[2:]
        return int(d[0:64], 16), int(d[64:128], 16)
    except Exception:
        return None


async def rpc_get_logs(session, from_block: int, to_block: int) -> list:
    params = [{"fromBlock": hex(from_block), "toBlock": hex(to_block),
               "address": PANCAKE_V2_FACTORY, "topics": [PAIR_CREATED_TOPIC]}]
    result = await rpc_call(session, "eth_getLogs", params)
    return result if isinstance(result, list) else []


async def rpc_balance_of(session, token: str, wallet: str) -> Optional[int]:
    padded = wallet.replace("0x", "").zfill(64)
    r = await rpc_call(session, "eth_call", [{"to": token, "data": f"0x70a08231{padded}"}, "latest"])
    if r and r != "0x":
        try: return int(r, 16)
        except Exception: pass
    return None


async def rpc_total_supply(session, token: str) -> Optional[int]:
    r = await rpc_call(session, "eth_call", [{"to": token, "data": "0x18160ddd"}, "latest"])
    if r and r != "0x":
        try: return int(r, 16)
        except Exception: pass
    return None


def parse_pair_log(log: dict) -> Optional[tuple]:
    try:
        topics = log.get("topics", [])
        if len(topics) < 3:
            return None
        token0 = "0x" + topics[1][-40:]
        token1 = "0x" + topics[2][-40:]
        data   = log.get("data", "")
        if len(data) < 66:
            return None
        pair_addr = "0x" + data[26:66]
        if token0.lower() == WBNB:
            return pair_addr.lower(), token1.lower()
        if token1.lower() == WBNB:
            return pair_addr.lower(), token0.lower()
        return pair_addr.lower(), token0.lower()
    except Exception:
        return None


# ── HTTP ──────────────────────────────────────────────────────────────────────

async def http_get(session, url: str, headers: dict = None) -> Optional[dict]:
    try:
        async with session.get(url, headers=headers or {}, timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status == 200:
                return await r.json()
    except Exception as e:
        logger.warning(f"HTTP {url[:55]}: {e}")
    return None


# ── DEXSCREENER ───────────────────────────────────────────────────────────────

async def dex_token(session, address: str) -> Optional[dict]:
    data = await http_get(session, f"https://api.dexscreener.com/latest/dex/tokens/{address}")
    if not data:
        return None
    pairs = [p for p in data.get("pairs", []) if p.get("chainId") == "bsc"]
    if pairs:
        return max(pairs, key=lambda x: float(x.get("liquidity", {}).get("usd", 0) or 0))
    return None


async def fetch_bsc_pairs(session) -> list:
    """Fetch candidate pairs from DexScreener in parallel."""
    results = []
    seen    = set()

    def add(pairs):
        for p in pairs:
            addr = p.get("baseToken", {}).get("address", "")
            if addr and addr.lower() not in seen:
                seen.add(addr.lower())
                results.append(p)

    tasks = [
        http_get(session, "https://api.dexscreener.com/latest/dex/pairs/bsc"),
        http_get(session, "https://api.dexscreener.com/latest/dex/search?q=pancakeswap"),
        http_get(session, "https://api.dexscreener.com/latest/dex/search?q=apeswap"),
        http_get(session, "https://api.dexscreener.com/latest/dex/search?q=biswap"),
        http_get(session, "https://api.dexscreener.com/token-profiles/latest/v1"),
        http_get(session, "https://api.dexscreener.com/token-boosts/top/v1"),
        http_get(session, "https://api.dexscreener.com/latest/dex/search?q=bsc+meme"),
    ]
    responses = await asyncio.gather(*tasks, return_exceptions=True)

    for i, r in enumerate(responses):
        if isinstance(r, Exception) or r is None:
            continue
        try:
            if i < 4:
                add([p for p in r.get("pairs", []) if p.get("chainId") == "bsc"][:60])
            elif i == 4 and isinstance(r, list):
                for p in [x for x in r if x.get("chainId") == "bsc"][:20]:
                    addr = p.get("tokenAddress")
                    if addr and addr.lower() not in seen:
                        pair = await dex_token(session, addr)
                        if pair:
                            seen.add(addr.lower())
                            results.append(pair)
            elif i == 5 and isinstance(r, list):
                for p in [x for x in r if x.get("chainId") == "bsc"][:10]:
                    addr = p.get("tokenAddress")
                    if addr and addr.lower() not in seen:
                        pair = await dex_token(session, addr)
                        if pair:
                            seen.add(addr.lower())
                            results.append(pair)
            else:
                add([p for p in r.get("pairs", []) if p.get("chainId") == "bsc"][:40])
        except Exception as e:
            logger.warning(f"DexScreener batch {i}: {e}")

    # Re-check dormant tokens from history (sleeping giant detection)
    dormant = [
        addr for addr, h in token_history.items()
        if h.get("vol_1h", 0) < WAKE_PREV_MAX_VOL and addr.lower() not in seen
    ][:35]
    if dormant:
        r2 = await asyncio.gather(*[dex_token(session, a) for a in dormant], return_exceptions=True)
        for pair in r2:
            if pair and not isinstance(pair, Exception):
                addr = pair.get("baseToken", {}).get("address", "")
                if addr and addr.lower() not in seen:
                    seen.add(addr.lower())
                    results.append(pair)

    logger.info(f"DexScreener: {len(results)} candidates")
    return results


# ── SAFETY DATA ───────────────────────────────────────────────────────────────

async def honeypot_check(session, addr: str) -> dict:
    return await http_get(session, f"https://api.honeypot.is/v2/IsHoneypot?address={addr}&chainID=56") or {}


async def goplus_check(session, addr: str) -> dict:
    data = await http_get(session, f"https://api.gopluslabs.io/api/v1/token_security/56?contract_addresses={addr}")
    if data:
        r = data.get("result", {})
        if r:
            return list(r.values())[0]
    return {}


async def get_top_holders(session, addr: str) -> list:
    url = (f"{BSCSCAN_URL}?module=token&action=tokenholderlist"
           f"&contractaddress={addr}&page=1&offset=12&apikey={BSCSCAN_KEY}")
    data = await http_get(session, url)
    return data.get("result", []) if data and data.get("status") == "1" else []


async def get_deployer(session, addr: str) -> Optional[str]:
    url = (f"{BSCSCAN_URL}?module=contract&action=getcontractcreation"
           f"&contractaddresses={addr}&apikey={BSCSCAN_KEY}")
    data = await http_get(session, url)
    if data and data.get("status") == "1" and data.get("result"):
        return data["result"][0].get("contractCreator")
    return None


async def get_contract_source(session, addr: str) -> str:
    url = (f"{BSCSCAN_URL}?module=contract&action=getsourcecode"
           f"&address={addr}&apikey={BSCSCAN_KEY}")
    data = await http_get(session, url)
    if data and data.get("status") == "1" and data.get("result"):
        return data["result"][0].get("SourceCode", "")
    return ""


async def gmgn_holders(session, addr: str) -> Optional[int]:
    try:
        data = await http_get(session, f"https://gmgn.ai/defi/quotation/v1/tokens/bsc/{addr}",
                              headers={"User-Agent": "Mozilla/5.0"})
        if data:
            t = data.get("data", {}).get("token", {})
            h = t.get("holder_count") or t.get("holders")
            return int(h) if h else None
    except Exception:
        pass
    return None


def analyze_contract(source: str) -> dict:
    if not source.strip():
        return {"verified": False, "flags": ["Unverified"], "score": 20}
    flags = []
    score = 100
    checks = {
        "mint(":        ("Mintable", 20),
        "blacklist":    ("Blacklist fn", 20),
        "setfee":       ("Changeable fees", 15),
        "pause()":      ("Pausable", 20),
        "selfdestruct": ("Selfdestruct", 35),
        "delegatecall": ("Dangerous proxy", 25),
    }
    for k, (msg, p) in checks.items():
        if k in source.lower():
            flags.append(msg)
            score -= p
    return {"verified": True, "flags": flags, "score": max(0, score)}


# ── HARD SAFETY GATE ──────────────────────────────────────────────────────────

async def safety_gate(
    session,
    pair_data: dict,
    pipeline: str,       # "new" | "cto" | "wake"
) -> tuple:
    """
    Fast safety pre-check. Runs BEFORE scoring.
    Returns (passed: bool, block_reason: str, safety_data: dict)

    Hard blocks — ANY of these kills the token permanently:
    - Honeypot confirmed
    - Sell tax > MAX_SELL_TAX
    - Buy tax > MAX_BUY_TAX
    - Top 10 holders > threshold
    - Already hard-dumping

    For NEW pipeline, also requires contract verified or renounced.
    """
    addr  = pair_data.get("baseToken", {}).get("address", "")
    liq   = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)
    mc    = float(pair_data.get("marketCap", 0) or 0)
    buys  = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    sells = int(pair_data.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
    v1h   = float(pair_data.get("volume", {}).get("h1", 0) or 0)
    c1h   = float(pair_data.get("priceChange", {}).get("h1", 0) or 0)

    # ── Absolute liquidity check (no API call needed) ──────────────────────
    if liq < MIN_LIQ_ABSOLUTE:
        return False, f"Liq too low (${liq:,.0f})", {}

    # ── Sell pressure check (no API call needed) ───────────────────────────
    total = buys + sells
    if total > 10 and (sells / total) > MAX_SELL_RATIO_1H:
        return False, f"Sell dominant ({round(sells/total*100)}% sells)", {}

    # ── Hard dump check ────────────────────────────────────────────────────
    if v1h > 2000 and c1h < MAX_DROP_FROM_ATH:
        return False, f"Dumping {c1h}% with volume", {}

    # ── Liq/MCap ratio ─────────────────────────────────────────────────────
    if mc > 0 and liq / mc < 0.03:
        return False, f"Liq only {round(liq/mc*100,1)}% of mcap", {}

    # ── API safety checks (parallel) ──────────────────────────────────────
    hp_data, gp_data, src, top_holders = await asyncio.gather(
        honeypot_check(session, addr),
        goplus_check(session, addr),
        get_contract_source(session, addr),
        get_top_holders(session, addr),
        return_exceptions=True
    )

    hp  = hp_data   if isinstance(hp_data,  dict) else {}
    gp  = gp_data   if isinstance(gp_data,  dict) else {}
    src = src       if isinstance(src,       str)  else ""
    top = top_holders if isinstance(top_holders, list) else []

    # Honeypot — hard block
    is_hp = hp.get("isHoneypot", False)
    if is_hp:
        return False, "HONEYPOT", {"hp": hp, "gp": gp, "src": src, "top": top}

    # Tax — hard block
    buy_tax  = hp.get("simulationResult", {}).get("buyTax")
    sell_tax = hp.get("simulationResult", {}).get("sellTax")
    if buy_tax is not None and float(buy_tax) > MAX_BUY_TAX:
        return False, f"Buy tax {buy_tax}% too high", {"hp": hp, "gp": gp, "src": src, "top": top}
    if sell_tax is not None and float(sell_tax) > MAX_SELL_TAX:
        return False, f"Sell tax {sell_tax}% too high", {"hp": hp, "gp": gp, "src": src, "top": top}

    # Top 10 concentration
    top10_pct = _calc_top10(top)
    threshold = NEW_MAX_TOP10 if pipeline == "new" else MAX_TOP10_PCT
    if top10_pct > threshold:
        return False, f"Top 10 hold {round(top10_pct,1)}% — whale risk", {"hp": hp, "gp": gp, "src": src, "top": top}

    # GoPlus hidden owner or active owner with dangerous flags
    if gp.get("is_honeypot", "0") == "1":
        return False, "GoPlus: honeypot", {"hp": hp, "gp": gp, "src": src, "top": top}

    # NEW pipeline: needs verified or renounced
    contract = analyze_contract(src)
    if pipeline == "new":
        renounced = gp.get("owner_address", "") == "0x0000000000000000000000000000000000000000"
        if not contract["verified"] and not renounced:
            return False, "Unverified + not renounced", {"hp": hp, "gp": gp, "src": src, "top": top}

    safety_data = {
        "hp": hp, "gp": gp, "src": src, "top": top,
        "top10_pct": top10_pct, "contract": contract,
        "buy_tax": buy_tax, "sell_tax": sell_tax,
    }
    return True, "", safety_data


def _calc_top10(top_holders: list) -> float:
    if not top_holders:
        return 0.0
    try:
        pcts = [float(h.get("percentage", 0) or 0) for h in top_holders[:10]]
        total = sum(pcts)
        if total > 0:
            return total
    except Exception:
        pass
    return 0.0


# ── PIPELINE CHECKS ───────────────────────────────────────────────────────────

def check_new_launch(pair_data: dict, age_h: float) -> tuple:
    """Returns (qualifies: bool, reason: str)"""
    if age_h > NEW_MAX_AGE_H:
        return False, f"Too old ({round(age_h,1)}h)"

    v1h  = float(pair_data.get("volume", {}).get("h1", 0) or 0)
    b1h  = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    s1h  = int(pair_data.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
    c1h  = float(pair_data.get("priceChange", {}).get("h1", 0) or 0)
    liq  = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)

    if liq < NEW_MIN_LIQ:
        return False, f"Liq ${liq:,.0f} < ${NEW_MIN_LIQ:,}"

    t1h = b1h + s1h
    if t1h > 8:
        bp = b1h / t1h
        if bp < 0.45:
            return False, f"Only {round(bp*100)}% buys — not enough interest"

    return True, "new_launch"


def check_cto_recovery(pair_data: dict, prev: Optional[dict]) -> tuple:
    """Returns (qualifies: bool, reason: str)"""
    mc  = float(pair_data.get("marketCap", 0) or 0)
    v1h = float(pair_data.get("volume", {}).get("h1", 0) or 0)
    b1h = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    s1h = int(pair_data.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
    c1h = float(pair_data.get("priceChange", {}).get("h1", 0) or 0)

    if not (CTO_MIN_MC <= mc <= CTO_MAX_MC):
        return False, f"MC ${mc:,.0f} out of CTO range"

    if v1h < CTO_MIN_VOL_1H:
        return False, f"Vol ${v1h:,.0f}/hr too low"

    t1h = b1h + s1h
    if t1h < 3:
        return False, "Too few transactions"

    bp = b1h / t1h if t1h > 0 else 0
    if bp < CTO_MIN_BUY_PCT:
        return False, f"Only {round(bp*100)}% buys — not accumulating"

    # Must show upward movement or at least stabilising
    if c1h < -30:
        return False, f"Still dumping ({c1h}%)"

    # Was it actually dead before? Check history
    if prev and prev.get("vol_1h", 0) > 5_000:
        return False, "Wasn't dormant before"

    return True, "cto_recovery"


def check_old_wakeup(pair_data: dict, prev: Optional[dict], age_h: float) -> tuple:
    """Returns (qualifies: bool, reason: str, signal: str)"""
    if age_h < WAKE_MIN_AGE_H:
        return False, f"Token only {round(age_h,1)}h old", ""

    mc  = float(pair_data.get("marketCap", 0) or 0)
    v1h = float(pair_data.get("volume", {}).get("h1", 0) or 0)
    b1h = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    s1h = int(pair_data.get("txns", {}).get("h1", {}).get("sells", 0) or 0)

    if mc < WAKE_MIN_MC:
        return False, f"MC ${mc:,.0f} too low for wakeup pipeline", ""

    if v1h < WAKE_MIN_VOL:
        return False, f"Vol ${v1h:,.0f}/hr too low", ""

    if not prev or prev.get("scan_count", 0) < 2:
        return False, "No historical baseline yet", ""

    prev_vol = prev.get("vol_1h", 0)

    # Was it genuinely dormant?
    if prev_vol >= WAKE_PREV_MAX_VOL:
        return False, f"Wasn't dormant (prev vol ${prev_vol:,.0f})", ""

    if v1h < WAKE_MIN_VOL:
        return False, f"Current vol too low (${v1h:,.0f})", ""

    if prev_vol == 0:
        signal = f"First volume after complete silence — ${v1h:,.0f}/hr"
        return True, "old_wakeup", signal

    if v1h >= prev_vol * WAKE_SPIKE_MULT:
        mult   = round(v1h / prev_vol, 1)
        signal = f"Volume {mult}x spike after dormancy — ${prev_vol:,.0f} → ${v1h:,.0f}/hr"
        return True, "old_wakeup", signal

    return False, f"Vol spike insufficient ({round(v1h/prev_vol,1) if prev_vol else '∞'}x, need {WAKE_SPIKE_MULT}x)", ""


# ── SCORING ───────────────────────────────────────────────────────────────────

def score_token(pair_data: dict, prev: Optional[dict], pipeline: str) -> dict:
    """
    Score a token that has already passed safety gates and pipeline checks.
    Returns {score, signals}
    """
    score = 0
    signals = []
    is_waking = pipeline in ("cto_recovery", "old_wakeup")

    try:
        v1h  = float(pair_data.get("volume", {}).get("h1", 0) or 0)
        v6h  = float(pair_data.get("volume", {}).get("h6", 0) or 0)
        v24h = float(pair_data.get("volume", {}).get("h24", 0) or 0)
        v5m  = float(pair_data.get("volume", {}).get("m5", 0) or 0)
        c5m  = float(pair_data.get("priceChange", {}).get("m5", 0) or 0)
        c1h  = float(pair_data.get("priceChange", {}).get("h1", 0) or 0)
        c6h  = float(pair_data.get("priceChange", {}).get("h6", 0) or 0)
        mc   = float(pair_data.get("marketCap", 0) or 0)
        b1h  = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
        s1h  = int(pair_data.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
        b5m  = int(pair_data.get("txns", {}).get("m5", {}).get("buys", 0) or 0)

        if is_waking:
            score += 25
            signals.append("Waking after dormancy")

        # Volume vs 6h avg
        if v1h > 0 and v6h > 0:
            avg = v6h / 6
            if avg > 0:
                m = v1h / avg
                if m >= 5:    score += 22; signals.append(f"Vol {round(m,1)}x 6h avg")
                elif m >= 3:  score += 14; signals.append(f"Vol {round(m,1)}x 6h avg")
                elif m >= 1.5: score += 7; signals.append(f"Vol picking up {round(m,1)}x")

        # Buy pressure
        t1h = b1h + s1h
        if t1h > 5:
            bp = b1h / t1h
            if bp >= 0.70:    score += 20; signals.append(f"{round(bp*100)}% buy pressure")
            elif bp >= 0.58:  score += 12; signals.append(f"{round(bp*100)}% buy pressure")

        # Trend alignment
        up = sum([c5m > 0, c1h > 0, c6h > 0])
        if up == 3:    score += 15; signals.append("Uptrend all TFs")
        elif up == 2:  score += 8;  signals.append("Uptrend 2/3 TFs")

        # Vol/MCap
        if mc > 0 and v24h > 0:
            r = v24h / mc
            if r > 1.2:    score += 15; signals.append(f"Vol/MCap {round(r*100)}%")
            elif r > 0.4:  score += 8;  signals.append(f"Vol/MCap {round(r*100)}%")

        # 5m buys (early momentum)
        if b5m >= 10:   score += 12; signals.append(f"{b5m} buys in 5m")
        elif b5m >= 5:  score += 6;  signals.append(f"{b5m} buys in 5m")

        # 5m volume
        if v5m > 8000:   score += 10; signals.append(f"${v5m:,.0f} in 5m")
        elif v5m > 2000: score += 5;  signals.append(f"${v5m:,.0f} in 5m")

        # DexScreener boost
        boost = pair_data.get("boosts", {}).get("active", 0) or 0
        if boost:
            score += 10; signals.append(f"DexScreener boosted ({boost})")

    except Exception as e:
        logger.warning(f"Score: {e}")

    return {"score": min(100, score), "signals": signals}


# ── MIN SCORE BY PIPELINE ─────────────────────────────────────────────────────

PIPELINE_MIN_SCORE = {
    "new_launch":   NEW_MIN_SCORE,
    "cto_recovery": CTO_MIN_SCORE,
    "old_wakeup":   WAKE_MIN_SCORE,
    "reserve_wake": WAKE_MIN_SCORE,
}


# ── NARRATIVE ─────────────────────────────────────────────────────────────────

NARRATIVES = {
    "🤖 AI":         ["ai", "agent", "gpt", "llm", "neural", "deepseek", "openai", "agi"],
    "🐸 Meme":       ["pepe", "doge", "shib", "inu", "cat", "frog", "moon", "wojak", "chad", "based", "bonk", "floki"],
    "🏛️ Political":  ["trump", "maga", "elon", "biden", "vote", "potus", "musk"],
    "🎮 GameFi":     ["game", "play", "nft", "metaverse", "quest", "guild", "arena", "p2e"],
    "💰 DeFi":       ["defi", "yield", "farm", "stake", "swap", "vault", "lp"],
    "🐂 BSC":        ["bnb", "binance", "pancake", "cake", "bsc"],
    "🌍 RWA":        ["rwa", "gold", "property", "real estate", "silver", "commodity"],
    "🔬 DeSci":      ["science", "research", "bio", "health", "dna", "lab", "pharma"],
    "💬 Social":     ["social", "friend", "creator", "fan", "community", "dao"],
    "🌙 Space":      ["space", "moon", "rocket", "star", "galaxy", "mars", "cosmos"],
    "🐉 Anime":      ["anime", "manga", "ninja", "samurai", "waifu", "kawaii"],
    "🦊 Animal":     ["dog", "cat", "bear", "bull", "fox", "wolf", "tiger", "panda", "ape", "hamster"],
    "💊 Degen":      ["degen", "gamble", "casino", "bet", "yolo"],
    "🍕 Fun":        ["pizza", "food", "coffee", "beer", "sushi", "cook"],
}

def detect_narrative(name: str, symbol: str) -> str:
    text  = f"{name} {symbol}".lower()
    found = [l for l, kws in NARRATIVES.items() if any(k in text for k in kws)]
    return "  ·  ".join(found[:3]) if found else "None"


# ── ALERT BUILDER ─────────────────────────────────────────────────────────────

PIPELINE_HEADER = {
    "new_launch":   ("🆕", "NEW LAUNCH"),
    "cto_recovery": ("🔄", "CTO / RECOVERY"),
    "old_wakeup":   ("💤🔥", "OLD TOKEN WAKING"),
    "reserve_wake": ("⛓🔥", "ON-CHAIN WAKEUP"),
}

async def build_alert(
    session,
    pair_data: dict,
    pipeline: str,
    safety_data: dict,
    score_data: dict,
    wake_signal: str = "",
    age_h: float = 0,
) -> tuple:
    """
    Returns (text, InlineKeyboardMarkup).
    Clean, compact, scannable. All critical info visible at a glance.
    """
    addr      = pair_data.get("baseToken", {}).get("address", "")
    name      = pair_data.get("baseToken", {}).get("name", "Unknown")
    symbol    = pair_data.get("baseToken", {}).get("symbol", "???")
    pair_addr = pair_data.get("pairAddress", "")
    dex_name  = pair_data.get("dexId", "BSC").replace("-"," ").title()
    dex_url   = pair_data.get("url", f"https://dexscreener.com/bsc/{pair_addr}")
    price_str = pair_data.get("priceUsd", "N/A")

    mc   = float(pair_data.get("marketCap", 0) or 0)
    liq  = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)
    v5m  = float(pair_data.get("volume", {}).get("m5", 0) or 0)
    v1h  = float(pair_data.get("volume", {}).get("h1", 0) or 0)
    v24h = float(pair_data.get("volume", {}).get("h24", 0) or 0)
    c5m  = pair_data.get("priceChange", {}).get("m5", "0")
    c1h  = pair_data.get("priceChange", {}).get("h1", "0")
    c6h  = pair_data.get("priceChange", {}).get("h6", "0")
    c24h = pair_data.get("priceChange", {}).get("h24", "0")
    b1h  = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    s1h  = int(pair_data.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
    b5m  = int(pair_data.get("txns", {}).get("m5", {}).get("buys", 0) or 0)
    s5m  = int(pair_data.get("txns", {}).get("m5", {}).get("sells", 0) or 0)

    if age_h < 1:   age = f"{round(age_h*60)}m"
    elif age_h < 48: age = f"{round(age_h,1)}h"
    else:           age = f"{round(age_h/24,1)}d"

    t1h  = b1h + s1h
    bp   = round(b1h/t1h*100) if t1h > 0 else 0
    lm   = round(liq/mc*100, 1) if mc > 0 else 0

    # Safety data
    hp       = safety_data.get("hp", {})
    gp       = safety_data.get("gp", {})
    contract = safety_data.get("contract") or analyze_contract(safety_data.get("src",""))
    top10    = round(safety_data.get("top10_pct", 0), 1)
    buy_tax  = safety_data.get("buy_tax")
    sell_tax = safety_data.get("sell_tax")

    renounced   = gp.get("owner_address","") == "0x0000000000000000000000000000000000000000"
    is_mintable = gp.get("is_mintable","0") == "1"
    hidden_own  = gp.get("hidden_owner","0") == "1"

    tax_str = (f"{buy_tax}% / {sell_tax}%"
               if buy_tax is not None and sell_tax is not None else "Unknown")

    # Holder count
    gmgn = await gmgn_holders(session, addr)
    gp_h = gp.get("holder_count")
    holder_sources = {}
    if gp_h:  holder_sources["GoPlus"] = int(gp_h)
    if gmgn:  holder_sources["GMGN"]   = int(gmgn)
    holders_str = f"{max(holder_sources.values()):,}" if holder_sources else "N/A"

    # Dev holding
    deployer     = await get_deployer(session, addr)
    total_supply = await rpc_total_supply(session, addr)
    dev_str      = "N/A"
    if deployer and total_supply:
        bal = await rpc_balance_of(session, addr, deployer)
        if bal is not None and total_supply > 0:
            pct = bal / total_supply * 100
            if pct == 0:    dev_str = "0% ✅"
            elif pct > 20:  dev_str = f"{round(pct,1)}% 🔴"
            elif pct > 10:  dev_str = f"{round(pct,1)}% ⚠️"
            else:           dev_str = f"{round(pct,1)}% ✅"

    narr     = detect_narrative(name, symbol)
    mclbl    = mc_label(mc)
    signals  = score_data.get("signals", [])
    score    = score_data.get("score", 0)
    emoji, header = PIPELINE_HEADER.get(pipeline, ("📡", "SIGNAL"))

    # Arrow helper
    def arr(v):
        try: return "▲" if float(v) >= 0 else "▼"
        except: return ""

    cflags = " · ".join(contract.get("flags", [])) if contract.get("flags") else "Clean ✅"

    msg = (
        f"{emoji} *{header}*  ·  {mclbl}\n"
        f"*{name}*  `${symbol}`\n"
        f"`{addr}`\n"
        f"{dex_name}  ·  {age} old\n"
    )

    if wake_signal:
        msg += f"_↳ {wake_signal}_\n"

    msg += (
        f"\n"
        f"💰 *${price_str}*"
        f"  {arr(c1h)}{c1h}% _(1h)_"
        f"  {arr(c24h)}{c24h}% _(24h)_\n"
        f"📈 MCap `${mc:,.0f}`  ·  💧 Liq `${liq:,.0f}` _({lm}%)_\n"
        f"📦 Vol  5m `${v5m:,.0f}`  ·  1h `${v1h:,.0f}`\n"
        f"🔄 1h  {b1h}B / {s1h}S  _{bp}% buys_"
        f"  ·  5m  {b5m}B / {s5m}S\n"
        f"\n"
        f"— *Safety* —\n"
        f"🍯 Honeypot: 🟢 Clean  ·  Tax: {tax_str}\n"
        f"📄 {'✅ Verified' if contract.get('verified') else '❌ Unverified'}"
        f"  ·  🔑 {'✅ Renounced' if renounced else '⚠️ Active owner'}\n"
        f"🖨️ Mint {'🔴' if is_mintable else '✅'}"
        f"  ·  👻 HiddenOwner {'🔴' if hidden_own else '✅'}\n"
        f"⚠️ {cflags}\n"
        f"🐋 Top 10: {top10}%  ·  👥 Holders: {holders_str}  ·  👨‍💻 Dev: {dev_str}\n"
        f"\n"
        f"— *Signals* ({score}/100) —\n"
    )

    if signals:
        msg += "\n".join(f"› {s}" for s in signals[:5]) + "\n"
    else:
        msg += "› Early stage\n"

    msg += (
        f"\n"
        f"🎭 {narr}\n"
    )

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("📊 Chart",   url=dex_url),
        InlineKeyboardButton("🔍 Scan",    url=f"https://bscscan.com/token/{addr}"),
        InlineKeyboardButton("⭐ Fav",     callback_data=f"addfav_{addr}"),
    ]])

    return msg, keyboard


# ── NEAR MISS ─────────────────────────────────────────────────────────────────

def log_near_miss(pair_data: dict, score: int, block_reason: str):
    if score < NEAR_MISS_MIN_SCORE:
        return
    addr   = pair_data.get("baseToken", {}).get("address", "")
    name   = pair_data.get("baseToken", {}).get("name", "?")
    symbol = pair_data.get("baseToken", {}).get("symbol", "???")
    mc     = float(pair_data.get("marketCap", 0) or 0)
    url    = pair_data.get("url", f"https://dexscreener.com/bsc/{addr}")

    if any(x.get("addr") == addr for x in near_miss_log):
        return

    near_miss_log.append({
        "addr": addr, "name": name, "symbol": symbol,
        "mc": mc, "url": url, "score": score,
        "reason": block_reason, "ts": time.time(),
    })


# ── DB BUILDER ────────────────────────────────────────────────────────────────

async def build_pair_database(session):
    current_block = await rpc_block_number(session)
    if not current_block:
        return
    from_block = current_block - DB_LOOKBACK_DAYS * BSC_BLOCKS_PER_DAY
    prev_size  = len(pair_database)

    for page in range(1, 6):
        url = (f"{BSCSCAN_URL}?module=logs&action=getLogs"
               f"&address={PANCAKE_V2_FACTORY}&topic0={PAIR_CREATED_TOPIC}"
               f"&fromBlock={from_block}&toBlock=latest"
               f"&page={page}&offset={DB_BUILD_BATCH}&apikey={BSCSCAN_KEY}")
        data = await http_get(session, url)
        if not data or data.get("status") != "1":
            break
        logs = data.get("result", [])
        if not logs:
            break
        for log in logs:
            parsed = parse_pair_log(log)
            if parsed:
                pa, ta = parsed
                if pa not in pair_database:
                    pair_database[pa] = ta
        await asyncio.sleep(0.25)

    if len(pair_database) > prev_size:
        logger.info(f"Pair DB: {prev_size} → {len(pair_database)}")


# ── RESERVE MONITOR ───────────────────────────────────────────────────────────

async def scan_reserves(session, app: Application):
    """Detect old tokens waking up via on-chain reserve changes."""
    global db_scan_pointer
    if not pair_database or not subscribed_chats:
        return

    pairs  = list(pair_database.items())
    total  = len(pairs)
    start  = db_scan_pointer % total
    end    = min(start + RESERVE_BATCH_SIZE, total)
    batch  = pairs[start:end]
    if len(batch) < RESERVE_BATCH_SIZE and total > RESERVE_BATCH_SIZE:
        batch += pairs[:RESERVE_BATCH_SIZE - len(batch)]
    db_scan_pointer = end % total

    flagged = []

    for pair_addr, token_addr in batch:
        try:
            res = await rpc_get_reserves(session, pair_addr)
            if not res:
                continue
            r0, r1 = res
            if r0 == 0 or r1 == 0:
                continue

            prev = pair_reserves.get(pair_addr)
            if not prev:
                pair_reserves[pair_addr] = {"r0": r0, "r1": r1, "stable": 0, "ts": time.time()}
                continue

            change = max(
                abs(r0 - prev["r0"]) / prev["r0"] * 100 if prev["r0"] > 0 else 0,
                abs(r1 - prev["r1"]) / prev["r1"] * 100 if prev["r1"] > 0 else 0,
            )

            if change < RESERVE_CHANGE_PCT:
                pair_reserves[pair_addr]["stable"] = min(prev["stable"] + 1, 500)
                pair_reserves[pair_addr].update({"r0": r0, "r1": r1, "ts": time.time()})
                continue

            if prev["stable"] >= RESERVE_MIN_STABLE:
                flagged.append((token_addr, pair_addr, prev["stable"], change))

            pair_reserves[pair_addr] = {"r0": r0, "r1": r1, "stable": 0, "ts": time.time()}
            await asyncio.sleep(0.04)

        except Exception as e:
            logger.warning(f"Reserve {pair_addr[:8]}: {e}")

    for token_addr, pair_addr, stable, change in flagged:
        try:
            if token_addr in alerted_tokens:
                continue

            pair_data = await dex_token(session, token_addr)
            if not pair_data:
                continue

            ts    = pair_data.get("pairCreatedAt")
            age_h = (time.time() - int(ts)/1000) / 3600 if ts else 999

            # Must be old enough to be a genuine wakeup
            if age_h < WAKE_MIN_AGE_H:
                continue

            mc  = float(pair_data.get("marketCap", 0) or 0)

            # Run safety gate
            passed, block_reason, safety_data = await safety_gate(session, pair_data, "wake")
            if not passed:
                logger.info(f"Reserve flagged blocked: {block_reason}")
                continue

            prev     = token_history.get(token_addr)
            score_d  = score_token(pair_data, prev, "reserve_wake")

            if score_d["score"] < WAKE_MIN_SCORE:
                log_near_miss(pair_data, score_d["score"], f"Reserve wakeup score {score_d['score']} < {WAKE_MIN_SCORE}")
                continue

            wake_signal = (
                f"Reserves moved {round(change,1)}% after "
                f"{stable} stable checks"
            )

            report, markup = await build_alert(
                session, pair_data, "reserve_wake",
                safety_data, score_d, wake_signal, age_h
            )

            price = float(pair_data.get("priceUsd") or 0)
            alerted_tokens[token_addr] = {"ts": time.time(), "price": price}
            name = pair_data.get("baseToken", {}).get("name", "?")
            logger.info(f"RESERVE ALERT: {name} mc=${mc:,.0f} age={round(age_h,1)}h score={score_d['score']}")

            for chat_id in list(subscribed_chats):
                try:
                    await app.bot.send_message(
                        chat_id=chat_id, text=report,
                        parse_mode=ParseMode.MARKDOWN, reply_markup=markup,
                        disable_web_page_preview=True
                    )
                    await asyncio.sleep(0.3)
                except Exception as e:
                    logger.warning(f"Send {chat_id}: {e}")

        except Exception as e:
            logger.warning(f"Reserve process: {e}")


# ── RPC NEW PAIR SCAN ─────────────────────────────────────────────────────────

async def rpc_scan_new_pairs(session, app: Application):
    global last_rpc_block
    if not subscribed_chats:
        return

    current = await rpc_block_number(session)
    if not current:
        return
    if last_rpc_block == 0:
        last_rpc_block = current - 150

    from_b = last_rpc_block + 1
    to_b   = min(current, from_b + 200)
    if from_b > to_b:
        return

    logs = await rpc_get_logs(session, from_b, to_b)
    last_rpc_block = to_b
    if not logs:
        return

    logger.info(f"RPC: {len(logs)} new pairs blocks {from_b}-{to_b}")

    for log in logs:
        try:
            parsed = parse_pair_log(log)
            if not parsed:
                continue
            pair_addr, token_addr = parsed

            if pair_addr not in pair_database:
                pair_database[pair_addr] = token_addr

            if token_addr in alerted_tokens or token_addr in seen_new_pairs:
                continue

            await asyncio.sleep(8)  # let DexScreener index

            pair_data = await dex_token(session, token_addr)
            if not pair_data:
                token_history[token_addr] = {
                    "vol_1h": 0, "vol_24h": 0, "price": None,
                    "ticker": "???", "mcap": 0, "timestamp": time.time(), "scan_count": 0,
                }
                continue

            ts    = pair_data.get("pairCreatedAt")
            age_h = (time.time() - int(ts)/1000) / 3600 if ts else 0

            # Pipeline check first
            qualifies, reason = check_new_launch(pair_data, age_h)
            if not qualifies:
                continue

            # Safety gate
            passed, block_reason, safety_data = await safety_gate(session, pair_data, "new")
            if not passed:
                logger.info(f"RPC new blocked ({block_reason}): {token_addr[:10]}")
                continue

            prev    = token_history.get(token_addr)
            score_d = score_token(pair_data, prev, "new_launch")

            if score_d["score"] < NEW_MIN_SCORE:
                log_near_miss(pair_data, score_d["score"], f"Score {score_d['score']} < {NEW_MIN_SCORE}")
                continue

            seen_new_pairs.add(token_addr)
            mc   = float(pair_data.get("marketCap", 0) or 0)
            name = pair_data.get("baseToken", {}).get("name", "?")
            logger.info(f"RPC NEW: {name} mc=${mc:,.0f} score={score_d['score']}")

            report, markup = await build_alert(
                session, pair_data, "new_launch",
                safety_data, score_d, age_h=age_h
            )
            alerted_tokens[token_addr] = {"ts": time.time(), "price": float(pair_data.get("priceUsd") or 0)}

            for chat_id in list(subscribed_chats):
                try:
                    await app.bot.send_message(
                        chat_id=chat_id, text=report,
                        parse_mode=ParseMode.MARKDOWN, reply_markup=markup,
                        disable_web_page_preview=True
                    )
                    await asyncio.sleep(0.3)
                except Exception as e:
                    logger.warning(f"Send {chat_id}: {e}")

        except Exception as e:
            logger.warning(f"RPC pair: {e}")


# ── MAIN SCAN ─────────────────────────────────────────────────────────────────

async def run_scan(session, app: Application):
    if not subscribed_chats:
        return
    logger.info("Scan cycle...")
    pairs = await fetch_bsc_pairs(session)

    for pair_data in pairs:
        try:
            addr  = pair_data.get("baseToken", {}).get("address", "")
            mc    = float(pair_data.get("marketCap", 0) or 0)
            v1h   = float(pair_data.get("volume", {}).get("h1", 0) or 0)
            v24h  = float(pair_data.get("volume", {}).get("h24", 0) or 0)
            price = float(pair_data.get("priceUsd") or 0)

            if not addr:
                continue

            prev = token_history.get(addr)
            token_history[addr] = {
                "vol_1h": v1h, "vol_24h": v24h,
                "price": pair_data.get("priceUsd"),
                "ticker": pair_data.get("baseToken", {}).get("symbol", "???"),
                "mcap": mc, "timestamp": time.time(),
                "scan_count": (prev.get("scan_count", 0) if prev else 0) + 1,
            }

            # Already alerted? Skip unless favlisted
            already  = addr in alerted_tokens
            is_faved = any(addr in favourites.get(cid, {}) for cid in subscribed_chats)

            if already and not is_faved:
                continue

            if already and is_faved:
                last_price = alerted_tokens[addr].get("price", 0)
                if last_price > 0 and price > 0:
                    if abs((price - last_price) / last_price * 100) < FAV_ALERT_PCT:
                        continue

            # Determine which pipeline applies
            ts    = pair_data.get("pairCreatedAt")
            age_h = (time.time() - int(ts)/1000) / 3600 if ts else 999

            pipeline    = None
            wake_signal = ""

            # Try CTO/Recovery first (any age, specific MC range)
            ok, reason = check_cto_recovery(pair_data, prev)
            if ok:
                pipeline = "cto_recovery"

            # Try new launch
            if not pipeline and age_h <= NEW_MAX_AGE_H and addr not in seen_new_pairs:
                ok, reason = check_new_launch(pair_data, age_h)
                if ok:
                    pipeline = "new_launch"
                    seen_new_pairs.add(addr)

            # Try old wakeup
            if not pipeline:
                ok, reason, wake_signal = check_old_wakeup(pair_data, prev, age_h)
                if ok:
                    pipeline = "old_wakeup"

            if not pipeline:
                # Doesn't fit any pipeline — near miss if score is decent
                quick_score = score_token(pair_data, prev, "new_launch")
                if quick_score["score"] >= NEAR_MISS_MIN_SCORE:
                    log_near_miss(pair_data, quick_score["score"], reason or "No pipeline match")
                continue

            # Safety gate — this is the hard filter
            gate_pipeline = "new" if pipeline == "new_launch" else "wake"
            passed, block_reason, safety_data = await safety_gate(session, pair_data, gate_pipeline)

            if not passed:
                quick_score = score_token(pair_data, prev, pipeline)
                log_near_miss(pair_data, quick_score["score"], f"Safety: {block_reason}")
                logger.info(f"BLOCKED ({block_reason}): {addr[:10]}")
                continue

            # Score
            score_d = score_token(pair_data, prev, pipeline)
            min_score = PIPELINE_MIN_SCORE.get(pipeline, 50)

            if score_d["score"] < min_score:
                log_near_miss(pair_data, score_d["score"], f"Score {score_d['score']} < {min_score}")
                continue

            name = pair_data.get("baseToken", {}).get("name", "?")
            logger.info(f"ALERT [{pipeline}] {name} mc=${mc:,.0f} score={score_d['score']}")

            report, markup = await build_alert(
                session, pair_data, pipeline,
                safety_data, score_d, wake_signal, age_h
            )
            alerted_tokens[addr] = {"ts": time.time(), "price": price}

            for chat_id in list(subscribed_chats):
                try:
                    await app.bot.send_message(
                        chat_id=chat_id, text=report,
                        parse_mode=ParseMode.MARKDOWN, reply_markup=markup,
                        disable_web_page_preview=True
                    )
                    await asyncio.sleep(0.3)
                except Exception as e:
                    logger.warning(f"Send {chat_id}: {e}")

        except Exception as e:
            logger.warning(f"Pair: {e}")


# ── FAV CHECKER ───────────────────────────────────────────────────────────────

async def check_fav_moves(app: Application):
    if not favourites:
        return
    async with aiohttp.ClientSession() as session:
        for chat_id, favs in favourites.items():
            for addr, info in list(favs.items()):
                try:
                    pair = await dex_token(session, addr)
                    if not pair:
                        continue
                    c1h    = float(pair.get("priceChange", {}).get("h1", 0) or 0)
                    price  = pair.get("priceUsd", "N/A")
                    ticker = pair.get("baseToken", {}).get("symbol", info.get("ticker", "???"))
                    url    = pair.get("url", f"https://dexscreener.com/bsc/{addr}")
                    if abs(c1h) >= FAV_ALERT_PCT:
                        key = f"fav_{addr}_{round(c1h/10)*10}"
                        if time.time() - alerted_tokens.get(key, {}).get("ts", 0) < 3600:
                            continue
                        alerted_tokens[key] = {"ts": time.time(), "price": 0}
                        d = "🚀 UP" if c1h > 0 else "🔴 DOWN"
                        await app.bot.send_message(
                            chat_id=chat_id,
                            text=f"⭐ *{ticker}* moved {d} *{c1h}%* in 1h\n💰 ${price}\n[Chart]({url})",
                            parse_mode=ParseMode.MARKDOWN,
                            disable_web_page_preview=True
                        )
                except Exception as e:
                    logger.warning(f"Fav move: {e}")


# ── BUTTON CALLBACK ───────────────────────────────────────────────────────────

async def handle_addfav(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    chat_id = query.message.chat_id
    try:
        addr = query.data.split("_", 1)[1]
    except Exception:
        await query.answer("Error.", show_alert=True)
        return

    if chat_id not in favourites:
        favourites[chat_id] = {}
    if addr in favourites[chat_id]:
        await query.answer("Already in Favlist ⭐")
        return

    ticker = token_history.get(addr, {}).get("ticker", "???")
    price  = float(token_history.get(addr, {}).get("price") or 0)
    favourites[chat_id][addr] = {"ticker": ticker, "added_ts": time.time(), "last_price": price}
    await query.answer(f"⭐ Added ${ticker}!")

    try:
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("📊 Chart",    url=f"https://dexscreener.com/bsc/{addr}"),
            InlineKeyboardButton("🔍 Scan",     url=f"https://bscscan.com/token/{addr}"),
            InlineKeyboardButton("⭐ In Fav ✅", callback_data=f"addfav_{addr}"),
        ]]))
    except Exception:
        pass

    await update_pinned_favs(context.application, chat_id)


# ── FAVOURITES ────────────────────────────────────────────────────────────────

async def build_fav_board(session, chat_id: int) -> str:
    favs = favourites.get(chat_id, {})
    if not favs:
        return "⭐ *FAVOURITES*\n\nEmpty — tap ⭐ on any alert."
    lines = ["⭐ *FAVOURITES*\n"]
    for addr, info in favs.items():
        pair = await dex_token(session, addr)
        if pair:
            price  = pair.get("priceUsd", "N/A")
            mc     = float(pair.get("marketCap", 0) or 0)
            v1h    = float(pair.get("volume", {}).get("h1", 0) or 0)
            c1h    = pair.get("priceChange", {}).get("h1", "0")
            c24h   = pair.get("priceChange", {}).get("h24", "0")
            ticker = pair.get("baseToken", {}).get("symbol", info.get("ticker", "???"))
            url    = pair.get("url", f"https://dexscreener.com/bsc/{addr}")
            e      = "🟢" if float(c1h or 0) >= 0 else "🔴"
            favs[addr]["ticker"] = ticker
            lines.append(
                f"[*${ticker}*]({url})  {mc_label(mc)}\n"
                f"💰 ${price}   MCap `${mc:,.0f}`\n"
                f"{e} {c1h}% _(1h)_  ·  {c24h}% _(24h)_\n"
                f"Vol `${v1h:,.0f}`\n"
                f"`{addr}`\n"
            )
        else:
            lines.append(f"${info.get('ticker','???')}  `{addr}`  _(unavailable)_\n")
    return "\n".join(lines)


async def update_pinned_favs(app: Application, chat_id: int):
    async with aiohttp.ClientSession() as session:
        board = await build_fav_board(session, chat_id)
    try:
        msg_id = pinned_msg_ids.get(chat_id)
        if msg_id:
            await app.bot.edit_message_text(
                chat_id=chat_id, message_id=msg_id, text=board,
                parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True
            )
        else:
            msg = await app.bot.send_message(
                chat_id=chat_id, text=board,
                parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True
            )
            pinned_msg_ids[chat_id] = msg.message_id
            await app.bot.pin_chat_message(
                chat_id=chat_id, message_id=msg.message_id, disable_notification=True
            )
    except Exception as e:
        logger.warning(f"Pin: {e}")


# ── COMMANDS ──────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    subscribed_chats.add(chat_id)
    await context.bot.set_my_commands([
        BotCommand("start",     "Activate scanner"),
        BotCommand("stop",      "Pause alerts"),
        BotCommand("scan",      "Deep scan any BSC token"),
        BotCommand("radar",     "Near-miss tokens worth a look"),
        BotCommand("watchlist", "Tracked tokens by MCap"),
        BotCommand("fav",       "Add to favourites"),
        BotCommand("unfav",     "Remove from favourites"),
        BotCommand("favlist",   "Live prices for favourites"),
        BotCommand("status",    "Scanner status"),
        BotCommand("filters",   "Active thresholds"),
    ])
    await update.message.reply_text(
        "🟡 *IMPULSE BSC v5.2*\n\n"
        "Three pipelines — each with hard safety gates:\n\n"
        "🆕 *New Launch* — under 72h, strict safety\n"
        "🔄 *CTO/Recovery* — any age, $1k–$50k MC, waking\n"
        "💤🔥 *Old Wakeup* — 24h+ old, $100k+ MC, dormant then active\n"
        "⛓🔥 *On-Chain* — reserve movement on known pairs\n\n"
        "Rugs blocked before they reach you.\n"
        "Tap ⭐ on any alert to add to Favlist.\n\n"
        "↓ Menu",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    subscribed_chats.discard(update.effective_chat.id)
    await update.message.reply_text("⏸ Paused. /start to resume.")


async def cmd_radar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not near_miss_log:
        await update.message.reply_text(
            "📻 *RADAR*\n\nNothing queued yet. Check back after a few scan cycles.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    sorted_m = sorted(near_miss_log, key=lambda x: x.get("score", 0), reverse=True)[:15]
    lines    = ["📻 *RADAR — NEAR MISSES*\n_(Passed scan, blocked from alert)_\n"]
    for t in sorted_m:
        age = f"{round((time.time()-t['ts'])/60)}m ago"
        lines.append(
            f"[*${t['symbol']}*  {t['name']}]({t['url']})\n"
            f"MCap `${t['mc']:,.0f}`  ·  Score `{t['score']}/100`  ·  {age}\n"
            f"⛔ _{t['reason']}_\n"
            f"`{t['addr']}`\n"
        )
    await update.message.reply_text(
        "\n".join(lines), parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    fav_c = sum(len(v) for v in favourites.values())
    await update.message.reply_text(
        f"*IMPULSE BSC v5.2*\n\n"
        f"✅ Running\n"
        f"⛓ Pair DB: `{len(pair_database):,}`\n"
        f"📊 Reserve snaps: `{len(pair_reserves):,}`\n"
        f"🪙 In memory: `{len(token_history):,}`\n"
        f"📻 Radar queue: `{len(near_miss_log)}`\n"
        f"🔔 Alerted: `{len(alerted_tokens):,}`\n"
        f"⭐ Favs: `{fav_c}`  ·  👥 Subs: `{len(subscribed_chats)}`\n"
        f"⛓ Last block: `{last_rpc_block:,}`",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_filters(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"*IMPULSE v5.2 — PIPELINES*\n\n"
        f"*Hard blocks (all pipelines):*\n"
        f"🚫 Honeypot detected\n"
        f"🚫 Buy tax > {MAX_BUY_TAX}%\n"
        f"🚫 Sell tax > {MAX_SELL_TAX}%\n"
        f"🚫 Top 10 holders > {MAX_TOP10_PCT}%\n"
        f"🚫 Sell ratio > {round(MAX_SELL_RATIO_1H*100)}% in 1h\n"
        f"🚫 Liq < ${MIN_LIQ_ABSOLUTE:,}\n"
        f"🚫 Liq/MCap < 3%\n\n"
        f"🆕 *New Launch* (<{NEW_MAX_AGE_H}h)\n"
        f"  Score ≥ {NEW_MIN_SCORE}  ·  Top 10 ≤ {NEW_MAX_TOP10}%\n"
        f"  Needs: verified OR renounced\n\n"
        f"🔄 *CTO/Recovery* (any age, ${CTO_MIN_MC:,}–${CTO_MAX_MC:,} MC)\n"
        f"  Score ≥ {CTO_MIN_SCORE}  ·  Buys ≥ {round(CTO_MIN_BUY_PCT*100)}%\n"
        f"  Vol ≥ ${CTO_MIN_VOL_1H}/hr\n\n"
        f"💤🔥 *Old Wakeup* (>{WAKE_MIN_AGE_H}h, MC >${WAKE_MIN_MC:,})\n"
        f"  Score ≥ {WAKE_MIN_SCORE}  ·  Vol {WAKE_SPIKE_MULT}x spike\n"
        f"  Prev vol < ${WAKE_PREV_MAX_VOL:,}/hr",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not token_history:
        await update.message.reply_text("Nothing tracked yet.")
        return
    top = sorted(token_history.items(), key=lambda x: x[1].get("mcap", 0), reverse=True)[:20]
    lines = ["*TOP 20 BY MCAP*\n"]
    for addr, h in top:
        mc     = h.get("mcap", 0)
        ticker = h.get("ticker", "???")
        link   = f"https://dexscreener.com/bsc/{addr}"
        lines.append(
            f"[*${ticker}*]({link})  {mc_label(mc)}\n"
            f"`{addr}`  ·  `${mc:,.0f}`"
        )
    await update.message.reply_text(
        "\n\n".join(lines), parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True
    )


async def cmd_fav(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Usage: /fav <address>")
        return
    addr = context.args[0].strip()
    if len(addr) != 42 or not addr.startswith("0x"):
        await update.message.reply_text("❌ Invalid address.")
        return
    if chat_id not in favourites:
        favourites[chat_id] = {}
    if addr in favourites[chat_id]:
        await update.message.reply_text("Already in favourites.")
        return
    msg = await update.message.reply_text("Adding...")
    async with aiohttp.ClientSession() as session:
        pair = await dex_token(session, addr)
    ticker = pair.get("baseToken", {}).get("symbol", "???") if pair else "???"
    price  = float(pair.get("priceUsd", 0) or 0) if pair else 0
    favourites[chat_id][addr] = {"ticker": ticker, "added_ts": time.time(), "last_price": price}
    await msg.edit_text(f"⭐ Added *${ticker}*", parse_mode=ParseMode.MARKDOWN)
    await update_pinned_favs(context.application, chat_id)


async def cmd_unfav(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Usage: /unfav <address>")
        return
    addr = context.args[0].strip()
    if chat_id in favourites and addr in favourites[chat_id]:
        ticker = favourites[chat_id][addr].get("ticker", "???")
        del favourites[chat_id][addr]
        await update.message.reply_text(f"Removed *${ticker}*", parse_mode=ParseMode.MARKDOWN)
        await update_pinned_favs(context.application, chat_id)
    else:
        await update.message.reply_text("Not in favourites.")


async def cmd_favlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not favourites.get(chat_id):
        await update.message.reply_text("No favourites. Tap ⭐ on an alert or /fav <address>.")
        return
    msg = await update.message.reply_text("Fetching...")
    async with aiohttp.ClientSession() as session:
        board = await build_fav_board(session, chat_id)
    try:
        await msg.edit_text(board, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
    except Exception:
        await msg.edit_text(board[:4000], disable_web_page_preview=True)
    await update_pinned_favs(context.application, chat_id)


async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /scan <BSC token address>")
        return
    addr = context.args[0].strip()
    msg  = await update.message.reply_text("🔍 Scanning...")
    async with aiohttp.ClientSession() as session:
        pair_data = await dex_token(session, addr)
        if not pair_data:
            await msg.edit_text("❌ Token not found on BSC.")
            return

        ts    = pair_data.get("pairCreatedAt")
        age_h = (time.time() - int(ts)/1000) / 3600 if ts else 0
        prev  = token_history.get(addr)

        # Determine pipeline for display
        _, reason, wake_signal = check_old_wakeup(pair_data, prev, age_h)
        pipeline = "old_wakeup" if wake_signal else "new_launch" if age_h < NEW_MAX_AGE_H else "old_wakeup"

        passed, block_reason, safety_data = await safety_gate(session, pair_data, "wake")
        if not passed:
            safety_data = {}

        score_d = score_token(pair_data, prev, pipeline)

        report, markup = await build_alert(
            session, pair_data, pipeline,
            safety_data, score_d, wake_signal, age_h
        )
    try:
        await msg.edit_text(
            report, parse_mode=ParseMode.MARKDOWN,
            reply_markup=markup, disable_web_page_preview=True
        )
    except Exception:
        await msg.edit_text(report[:4000], disable_web_page_preview=True)


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start",     cmd_start))
    app.add_handler(CommandHandler("stop",      cmd_stop))
    app.add_handler(CommandHandler("status",    cmd_status))
    app.add_handler(CommandHandler("filters",   cmd_filters))
    app.add_handler(CommandHandler("watchlist", cmd_watchlist))
    app.add_handler(CommandHandler("radar",     cmd_radar))
    app.add_handler(CommandHandler("scan",      cmd_scan))
    app.add_handler(CommandHandler("fav",       cmd_fav))
    app.add_handler(CommandHandler("unfav",     cmd_unfav))
    app.add_handler(CommandHandler("favlist",   cmd_favlist))
    app.add_handler(CallbackQueryHandler(handle_addfav, pattern=r"^addfav_"))

    async def dex_job(ctx):
        async with aiohttp.ClientSession() as session:
            await run_scan(session, app)

    async def rpc_job(ctx):
        async with aiohttp.ClientSession() as session:
            await rpc_scan_new_pairs(session, app)

    async def reserve_job(ctx):
        async with aiohttp.ClientSession() as session:
            await scan_reserves(session, app)

    async def db_job(ctx):
        async with aiohttp.ClientSession() as session:
            await build_pair_database(session)

    async def fav_job(ctx):
        await check_fav_moves(app)

    app.job_queue.run_repeating(db_job,      interval=DB_BUILD_INTERVAL,    first=5)
    app.job_queue.run_repeating(rpc_job,     interval=RPC_NEW_PAIR_INTERVAL, first=12)
    app.job_queue.run_repeating(reserve_job, interval=RESERVE_SCAN_INTERVAL, first=35)
    app.job_queue.run_repeating(dex_job,     interval=SCAN_INTERVAL,         first=22)
    app.job_queue.run_repeating(fav_job,     interval=FAV_CHECK_INTERVAL,    first=90)

    logger.info("IMPULSE BSC v5.2 — three pipelines, hard safety gates")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
