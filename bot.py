"""
BSC MEME SCANNER BOT v4
- Direct BSC RPC monitoring (GetBlock + Ankr) — finds tokens DexScreener never surfaces
- Monitors ALL tokens regardless of age — old tokens waking up is the priority
- One alert per token permanently, unless:
    • User taps 👁 eye button on the alert
    • Token is in /fav list
  Re-alert threshold: 50% move from last alert price
- Accurate top 10 holders from BSCScan API
- Accurate dev holding from deployer wallet balance
- DEX paid status from DexScreener boosts
- Watchlist shows MCap
- Full menu bar, favourites, pinned board
"""

import asyncio
import json
import logging
import os
import time
from typing import Optional

import aiohttp
from telegram import (
    Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes
)
from telegram.constants import ParseMode

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ── CREDENTIALS ───────────────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8623651466:AAFeEHh3t15n1ciB4L2MZbj57h_j3mzAGhw")
BSCSCAN_KEY    = os.getenv("ETHERSCAN_KEY",  "S3ZEIA898K31SY9EJXE3HYY8ZEZ6PBGMHB")
BSCSCAN_URL    = "https://api.bscscan.com/api"

# RPC endpoints — primary GetBlock, fallback Ankr
RPC_ENDPOINTS = [
    "https://go.getblock.us/75254fd9f61c4b43af30f2e55be8f55d",
    "https://rpc.ankr.com/bsc/0f0dde5ab6eee670d869cfe84af8d17eab6b832a050e17b3241f9b0dc513f1e5",
]

# PancakeSwap V2 Factory — most BSC memes launch here
PANCAKE_V2_FACTORY  = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"
PANCAKE_V3_FACTORY  = "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"
# PairCreated(address,address,address,uint256) topic
PAIR_CREATED_TOPIC  = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
# WBNB address (filter out BNB pairs that aren't memes)
WBNB = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c".lower()

# ── THRESHOLDS ────────────────────────────────────────────────────────────────
MIN_LIQ_MICRO        = 1_500
MIN_LIQ_STD          = 4_000
MICRO_CAP_MAX        = 500_000

MIN_SCORE_NEW        = 35
MIN_SCORE_WAKE       = 40
MIN_SCORE_STD        = 50

NEW_PAIR_MAX_AGE_H   = 72
WAKE_PREV_MAX_VOL    = 20_000
WAKE_MIN_VOL         = 2_000
WAKE_SPIKE_MULT      = 1.8

MAX_SELL_RATIO       = 0.75
MIN_LIQ_MCAP         = 0.04
MAX_DROP_1H          = -40

FAV_ALERT_PCT        = 50
WATCH_ALERT_PCT      = 50
FAV_CHECK_INTERVAL   = 120
SCAN_INTERVAL        = 60
RPC_SCAN_INTERVAL    = 30   # RPC new-pair scan runs faster

# ── STATE ─────────────────────────────────────────────────────────────────────
subscribed_chats = set()
alerted_tokens   = {}   # addr -> {ts, price}  — permanent, no re-alert unless watched/fav
token_history    = {}   # addr -> snapshot
seen_new_pairs   = set()
watched_tokens   = {}   # chat_id -> {addr -> alert_price}  (eye button)
favourites       = {}   # chat_id -> {addr -> info}
pinned_msg_ids   = {}   # chat_id -> message_id
last_rpc_block   = 0    # track last scanned block


# ── RPC HELPERS ───────────────────────────────────────────────────────────────

async def rpc_call(session: aiohttp.ClientSession, method: str, params: list) -> Optional[dict]:
    """Call BSC RPC, try primary then fallback"""
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    for endpoint in RPC_ENDPOINTS:
        try:
            async with session.post(
                endpoint,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    if "result" in data:
                        return data["result"]
        except Exception as e:
            logger.warning(f"RPC error {endpoint[:40]}: {e}")
    return None


async def rpc_block_number(session: aiohttp.ClientSession) -> Optional[int]:
    result = await rpc_call(session, "eth_blockNumber", [])
    if result:
        return int(result, 16)
    return None


async def rpc_get_logs(session: aiohttp.ClientSession, from_block: int, to_block: int) -> list:
    """Get PairCreated events from PancakeSwap V2 factory"""
    params = [{
        "fromBlock": hex(from_block),
        "toBlock": hex(to_block),
        "address": PANCAKE_V2_FACTORY,
        "topics": [PAIR_CREATED_TOPIC]
    }]
    result = await rpc_call(session, "eth_getLogs", params)
    return result if isinstance(result, list) else []


async def rpc_balance_of(session: aiohttp.ClientSession, token: str, wallet: str) -> Optional[int]:
    """Get token balance of a wallet via eth_call"""
    # balanceOf(address) selector = 0x70a08231
    padded = wallet.replace("0x", "").zfill(64)
    data = f"0x70a08231{padded}"
    result = await rpc_call(session, "eth_call", [
        {"to": token, "data": data},
        "latest"
    ])
    if result and result != "0x":
        try:
            return int(result, 16)
        except Exception:
            pass
    return None


async def rpc_total_supply(session: aiohttp.ClientSession, token: str) -> Optional[int]:
    """Get total supply via eth_call"""
    result = await rpc_call(session, "eth_call", [
        {"to": token, "data": "0x18160ddd"},
        "latest"
    ])
    if result and result != "0x":
        try:
            return int(result, 16)
        except Exception:
            pass
    return None


def parse_pair_created_log(log: dict) -> Optional[str]:
    """
    Extract token address from PairCreated log.
    Topics: [event_sig, token0, token1]
    Data: [pair_address, uint256]
    Token is whichever of token0/token1 is NOT WBNB
    """
    try:
        topics = log.get("topics", [])
        if len(topics) < 3:
            return None
        token0 = "0x" + topics[1][-40:]
        token1 = "0x" + topics[2][-40:]
        # Return the non-WBNB token
        if token0.lower() == WBNB:
            return token1
        if token1.lower() == WBNB:
            return token0
        # If neither is WBNB, return token0 (still valid, just non-BNB pair)
        return token0
    except Exception:
        return None


# ── HTTP HELPER ───────────────────────────────────────────────────────────────

async def fetch(session: aiohttp.ClientSession, url: str, headers: dict = None) -> Optional[dict]:
    try:
        async with session.get(
            url,
            headers=headers or {},
            timeout=aiohttp.ClientTimeout(total=12)
        ) as r:
            if r.status == 200:
                return await r.json()
    except Exception as e:
        logger.warning(f"Fetch error {url[:60]}: {e}")
    return None


# ── DEXSCREENER ───────────────────────────────────────────────────────────────

async def dex_token(session: aiohttp.ClientSession, address: str) -> Optional[dict]:
    """Get best BSC pair for a token from DexScreener"""
    data = await fetch(session, f"https://api.dexscreener.com/latest/dex/tokens/{address}")
    if not data:
        return None
    pairs = [p for p in data.get("pairs", []) if p.get("chainId") == "bsc"]
    if pairs:
        return max(pairs, key=lambda x: float(x.get("liquidity", {}).get("usd", 0) or 0))
    return None


async def fetch_bsc_pairs(session: aiohttp.ClientSession) -> list:
    """
    Fetch BSC pairs from DexScreener in parallel — supplementary to RPC.
    RPC handles discovery, DexScreener provides market data.
    """
    results = []
    seen = set()

    def add(pairs):
        for p in pairs:
            addr = p.get("baseToken", {}).get("address", "")
            if addr and addr.lower() not in seen:
                seen.add(addr.lower())
                results.append(p)

    tasks = [
        fetch(session, "https://api.dexscreener.com/latest/dex/pairs/bsc"),
        fetch(session, "https://api.dexscreener.com/latest/dex/search?q=pancakeswap"),
        fetch(session, "https://api.dexscreener.com/latest/dex/search?q=apeswap"),
        fetch(session, "https://api.dexscreener.com/latest/dex/search?q=biswap"),
        fetch(session, "https://api.dexscreener.com/token-profiles/latest/v1"),
        fetch(session, "https://api.dexscreener.com/token-boosts/top/v1"),
        fetch(session, "https://api.dexscreener.com/latest/dex/search?q=bsc+meme"),
    ]
    responses = await asyncio.gather(*tasks, return_exceptions=True)

    for i, r in enumerate(responses):
        if isinstance(r, Exception) or r is None:
            continue
        try:
            if i < 4:
                bsc = [p for p in r.get("pairs", []) if p.get("chainId") == "bsc"]
                add(bsc[:60])
            elif i == 4:  # profiles
                if isinstance(r, list):
                    bsc = [p for p in r if p.get("chainId") == "bsc"]
                    for p in bsc[:20]:
                        addr = p.get("tokenAddress")
                        if addr and addr.lower() not in seen:
                            pair = await dex_token(session, addr)
                            if pair:
                                seen.add(addr.lower())
                                results.append(pair)
            elif i == 5:  # boosts
                if isinstance(r, list):
                    bsc = [p for p in r if p.get("chainId") == "bsc"]
                    for p in bsc[:10]:
                        addr = p.get("tokenAddress")
                        if addr and addr.lower() not in seen:
                            pair = await dex_token(session, addr)
                            if pair:
                                seen.add(addr.lower())
                                results.append(pair)
            else:
                bsc = [p for p in r.get("pairs", []) if p.get("chainId") == "bsc"]
                add(bsc[:40])
        except Exception as e:
            logger.warning(f"DexScreener response error {i}: {e}")

    # Sleeping tokens from history — limited batch
    sleeping = [
        addr for addr, h in token_history.items()
        if h.get("vol_1h", 0) < WAKE_PREV_MAX_VOL and addr.lower() not in seen
    ][:40]

    if sleeping:
        tasks2 = [dex_token(session, addr) for addr in sleeping]
        results2 = await asyncio.gather(*tasks2, return_exceptions=True)
        for pair in results2:
            if pair and not isinstance(pair, Exception):
                addr = pair.get("baseToken", {}).get("address", "")
                if addr and addr.lower() not in seen:
                    seen.add(addr.lower())
                    results.append(pair)

    logger.info(f"DexScreener pairs: {len(results)}")
    return results


# ── BSCSCAN DATA ──────────────────────────────────────────────────────────────

async def get_top_holders(session: aiohttp.ClientSession, address: str) -> list:
    """
    Get top 10 holders directly from BSCScan.
    Returns list of {address, quantity, percentage}
    Much more accurate than GoPlus.
    """
    url = (
        f"{BSCSCAN_URL}?module=token&action=tokenholderlist"
        f"&contractaddress={address}&page=1&offset=10&apikey={BSCSCAN_KEY}"
    )
    data = await fetch(session, url)
    if data and data.get("status") == "1":
        return data.get("result", [])
    return []


async def get_deployer(session: aiohttp.ClientSession, address: str) -> Optional[str]:
    """Get the wallet that deployed this contract"""
    url = (
        f"{BSCSCAN_URL}?module=contract&action=getcontractcreation"
        f"&contractaddresses={address}&apikey={BSCSCAN_KEY}"
    )
    data = await fetch(session, url)
    if data and data.get("status") == "1" and data.get("result"):
        return data["result"][0].get("contractCreator")
    return None


async def get_dev_holding(
    session: aiohttp.ClientSession,
    token_addr: str,
    deployer: str,
    total_supply: Optional[int]
) -> Optional[float]:
    """
    Get deployer's current token balance as % of total supply.
    Uses direct RPC call for accuracy.
    """
    if not deployer or not total_supply or total_supply == 0:
        return None
    balance = await rpc_balance_of(session, token_addr, deployer)
    if balance is None:
        return None
    return (balance / total_supply) * 100


async def get_contract_source(session: aiohttp.ClientSession, address: str) -> str:
    url = (
        f"{BSCSCAN_URL}?module=contract&action=getsourcecode"
        f"&address={address}&apikey={BSCSCAN_KEY}"
    )
    data = await fetch(session, url)
    if data and data.get("status") == "1" and data.get("result"):
        return data["result"][0].get("SourceCode", "")
    return ""


def analyze_contract(source: str) -> dict:
    if not source.strip():
        return {"verified": False, "flags": ["❌ Unverified contract"], "score": 20}
    flags = []
    score = 100
    checks = {
        "mint(":        ("⚠️ Mintable supply", 25),
        "blacklist":    ("⚠️ Blacklist function", 20),
        "setfee":       ("⚠️ Changeable fees", 15),
        "pause()":      ("⚠️ Can pause trading", 20),
        "selfdestruct": ("🔴 Selfdestruct", 35),
        "delegatecall": ("🔴 Dangerous proxy", 25),
    }
    src = source.lower()
    for k, (msg, p) in checks.items():
        if k in src:
            flags.append(msg)
            score -= p
    return {"verified": True, "flags": flags, "score": max(0, score)}


# ── SECURITY ──────────────────────────────────────────────────────────────────

async def honeypot_check(session: aiohttp.ClientSession, address: str) -> dict:
    data = await fetch(session, f"https://api.honeypot.is/v2/IsHoneypot?address={address}&chainID=56")
    return data or {}


async def goplus_check(session: aiohttp.ClientSession, address: str) -> dict:
    data = await fetch(session, f"https://api.gopluslabs.io/api/v1/token_security/56?contract_addresses={address}")
    if data:
        result = data.get("result", {})
        if result:
            return list(result.values())[0]
    return {}


async def gmgn_holders(session: aiohttp.ClientSession, address: str) -> Optional[int]:
    try:
        data = await fetch(
            session,
            f"https://gmgn.ai/defi/quotation/v1/tokens/bsc/{address}",
            headers={"User-Agent": "Mozilla/5.0"}
        )
        if data:
            t = data.get("data", {}).get("token", {})
            h = t.get("holder_count") or t.get("holders")
            return int(h) if h else None
    except Exception:
        pass
    return None


def dex_paid_status(pair_data: dict) -> str:
    """
    Check if token has paid DexScreener promotion.
    Boosts = paid promotion. Info badge = free community listing.
    """
    boosts = pair_data.get("boosts", {})
    active = boosts.get("active", 0) or 0
    if active > 0:
        return f"✅ Paid — {active} active boost{'s' if active > 1 else ''}"
    # Check if token has profile (free)
    info = pair_data.get("info", {})
    if info:
        return "ℹ️ Free listing (community submitted)"
    return "❌ Not listed / no promotion"


# ── DUMP FILTER ───────────────────────────────────────────────────────────────

def is_dump(pair_data: dict) -> tuple:
    v1h     = float(pair_data.get("volume", {}).get("h1", 0) or 0)
    c1h     = float(pair_data.get("priceChange", {}).get("h1", 0) or 0)
    c6h     = float(pair_data.get("priceChange", {}).get("h6", 0) or 0)
    liq     = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)
    mc      = float(pair_data.get("marketCap", 0) or 0)
    buys    = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    sells   = int(pair_data.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
    total   = buys + sells

    if total > 8 and sells / total > MAX_SELL_RATIO:
        return True, f"Sell dominant — {round(sells/total*100)}% sells"
    if v1h > 3000 and c1h < MAX_DROP_1H:
        return True, f"Dumping {c1h}% with volume"
    if mc > 0 and liq > 0 and liq / mc < MIN_LIQ_MCAP:
        return True, f"Liq only {round(liq/mc*100,1)}% of mcap"
    if liq > 0 and v1h > liq * 15:
        return True, "Wash trading suspected"
    if c6h > 500 and c1h < -20:
        return True, f"Already up {c6h}% and reversing"
    return False, ""


# ── SLEEPING GIANT ────────────────────────────────────────────────────────────

def is_sleeping_giant(pair_data: dict, prev: Optional[dict]) -> tuple:
    v1h = float(pair_data.get("volume", {}).get("h1", 0) or 0)
    if not prev or prev.get("scan_count", 0) < 1:
        return False, ""
    prev_vol = prev.get("vol_1h", 0)
    if prev_vol >= WAKE_PREV_MAX_VOL or v1h < WAKE_MIN_VOL:
        return False, ""
    if prev_vol == 0:
        return True, f"💤➡🔥 First volume after silence — ${v1h:,.0f}/hr"
    if v1h >= prev_vol * WAKE_SPIKE_MULT:
        m = round(v1h / prev_vol, 1)
        return True, f"💤➡🔥 Volume {m}x spike — ${prev_vol:,.0f} → ${v1h:,.0f}/hr"
    return False, ""


# ── SCORE ─────────────────────────────────────────────────────────────────────

def score_token(pair_data: dict, prev: Optional[dict], is_waking: bool) -> dict:
    score = 0
    signals = []
    try:
        v5m  = float(pair_data.get("volume", {}).get("m5", 0) or 0)
        v1h  = float(pair_data.get("volume", {}).get("h1", 0) or 0)
        v6h  = float(pair_data.get("volume", {}).get("h6", 0) or 0)
        v24h = float(pair_data.get("volume", {}).get("h24", 0) or 0)
        c5m  = float(pair_data.get("priceChange", {}).get("m5", 0) or 0)
        c1h  = float(pair_data.get("priceChange", {}).get("h1", 0) or 0)
        c6h  = float(pair_data.get("priceChange", {}).get("h6", 0) or 0)
        mc   = float(pair_data.get("marketCap", 0) or 0)
        b1h  = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
        s1h  = int(pair_data.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
        b5m  = int(pair_data.get("txns", {}).get("m5", {}).get("buys", 0) or 0)

        if is_waking:
            score += 30
            signals.append("🚨 Sleeping giant waking up")

        if v1h > 0 and v6h > 0:
            avg = v6h / 6
            if avg > 0:
                m = v1h / avg
                if m >= 4:   score += 20; signals.append(f"📈 Volume {round(m,1)}x the 6h avg")
                elif m >= 2: score += 12; signals.append(f"📈 Volume {round(m,1)}x the 6h avg")
                elif m >= 1.3: score += 6; signals.append(f"📈 Volume picking up {round(m,1)}x avg")

        t1h = b1h + s1h
        if t1h > 3:
            bp = b1h / t1h
            if bp >= 0.68:   score += 18; signals.append(f"🟢 {round(bp*100)}% buy pressure in 1h")
            elif bp >= 0.55: score += 10; signals.append(f"🟢 {round(bp*100)}% buy pressure in 1h")

        up = sum([c5m > 0, c1h > 0, c6h > 0])
        if up == 3:   score += 14; signals.append("🟢 Uptrend all timeframes")
        elif up == 2: score += 7;  signals.append("🟡 Uptrend 2/3 timeframes")

        if mc > 0 and v24h > 0:
            r = v24h / mc
            if r > 1.0:   score += 14; signals.append(f"🔥 Vol/MCap {round(r*100)}%")
            elif r > 0.3: score += 7;  signals.append(f"📊 Vol/MCap {round(r*100)}%")

        if b5m >= 8:   score += 10; signals.append(f"⚡ {b5m} buys in 5m")
        elif b5m >= 3: score += 5;  signals.append(f"⚡ {b5m} buys in 5m")

        if 0 < mc <= MICRO_CAP_MAX:
            score += 7; signals.append(f"💎 Micro cap ${mc:,.0f}")

        if 2000 <= v1h <= 60000:
            score += 8; signals.append(f"💎 Early volume zone ${v1h:,.0f}/hr")

    except Exception as e:
        logger.warning(f"Score error: {e}")

    return {"score": min(100, score), "signals": signals}


def virality(pair_data: dict) -> dict:
    score = 0; signals = []
    b5m   = int(pair_data.get("txns", {}).get("m5", {}).get("buys", 0) or 0)
    b1h   = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    v5m   = float(pair_data.get("volume", {}).get("m5", 0) or 0)
    boost = pair_data.get("boosts", {}).get("active", 0) or 0

    if b5m >= 15: score += 28; signals.append(f"🔥 {b5m} buys in 5m")
    elif b5m >= 7: score += 15; signals.append(f"⚡ {b5m} buys in 5m")
    if b1h >= 100: score += 28; signals.append(f"📣 {b1h} buys in 1h")
    elif b1h >= 40: score += 15; signals.append(f"📣 {b1h} buys in 1h")
    if v5m > 10000: score += 22; signals.append(f"💸 ${v5m:,.0f} in 5m")
    elif v5m > 3000: score += 10; signals.append(f"💸 ${v5m:,.0f} in 5m")
    if boost: score += 22; signals.append(f"🚀 DexScreener boosted ({boost})")

    return {"score": min(100, score), "signals": signals}


NARRATIVES = {
    "🤖 AI":         ["ai", "agent", "gpt", "llm", "neural", "deepseek"],
    "🐸 Meme":       ["pepe", "doge", "shib", "inu", "cat", "frog", "moon", "wojak", "chad", "based", "bonk"],
    "🏛️ Political":  ["trump", "maga", "elon", "biden", "vote"],
    "🎮 Gaming":     ["game", "play", "nft", "metaverse", "guild"],
    "💰 DeFi":       ["defi", "yield", "farm", "stake", "swap"],
    "🐂 BSC Native": ["bnb", "binance", "pancake"],
}

def narrative(name: str, symbol: str) -> str:
    text = f"{name} {symbol}".lower()
    found = [l for l, kws in NARRATIVES.items() if any(k in text for k in kws)]
    return " | ".join(found) if found else "None detected"


def risk_label(is_hp, cscore, top10, gp) -> str:
    if is_hp: return "🔴 CRITICAL — HONEYPOT"
    if any([
        gp.get("can_take_back_ownership","0")=="1",
        gp.get("is_proxy","0")=="1",
        gp.get("hidden_owner","0")=="1"
    ]): return "🔴 HIGH RISK"
    if cscore < 40 or top10 > 85: return "🔴 HIGH RISK"
    if cscore < 65 or top10 > 65: return "🟡 MEDIUM RISK"
    if gp.get("owner_address","") == "0x0000000000000000000000000000000000000000" and cscore >= 75:
        return "🟢 LOW RISK ✓"
    return "🟡 MEDIUM RISK"


def predict(pair_data, acc, waking, dump) -> str:
    if dump: return "🔴 DUMP PATTERN — avoid"
    c1h = float(pair_data.get("priceChange", {}).get("h1", 0) or 0)
    c6h = float(pair_data.get("priceChange", {}).get("h6", 0) or 0)
    if waking and acc >= 60: return "🚀 HIGH POTENTIAL — sleeping giant waking"
    if waking: return "📈 WATCH — first volume appearing"
    if acc >= 75 and c1h > 0 and c6h > 0: return "🚀 STRONG BULLISH"
    if acc >= 55 and c1h > 0: return "📈 BULLISH — accumulation forming"
    if acc >= 40: return "🟡 EARLY SIGNAL — watch closely"
    return "⚪ NEUTRAL"


# ── FULL REPORT ───────────────────────────────────────────────────────────────

async def build_report(
    session: aiohttp.ClientSession,
    pair_data: dict,
    is_waking: bool = False,
    wake_signal: str = "",
    is_new: bool = False,
    new_age_str: str = "",
) -> tuple:
    """Returns (report_text, reply_markup)"""
    addr      = pair_data.get("baseToken", {}).get("address", "")
    name      = pair_data.get("baseToken", {}).get("name", "Unknown")
    symbol    = pair_data.get("baseToken", {}).get("symbol", "???")
    pair_addr = pair_data.get("pairAddress", "")
    dex_name  = pair_data.get("dexId", "BSC DEX").replace("-", " ").title()
    dex_url   = pair_data.get("url", f"https://dexscreener.com/bsc/{pair_addr}")
    price_str = pair_data.get("priceUsd", "N/A")
    mc        = float(pair_data.get("marketCap", 0) or 0)
    liq       = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)
    v5m       = float(pair_data.get("volume", {}).get("m5", 0) or 0)
    v1h       = float(pair_data.get("volume", {}).get("h1", 0) or 0)
    v6h       = float(pair_data.get("volume", {}).get("h6", 0) or 0)
    v24h      = float(pair_data.get("volume", {}).get("h24", 0) or 0)
    c5m       = pair_data.get("priceChange", {}).get("m5", "0")
    c1h       = pair_data.get("priceChange", {}).get("h1", "0")
    c6h       = pair_data.get("priceChange", {}).get("h6", "0")
    c24h      = pair_data.get("priceChange", {}).get("h24", "0")
    b1h       = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    s1h       = int(pair_data.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
    b5m       = int(pair_data.get("txns", {}).get("m5", {}).get("buys", 0) or 0)
    s5m       = int(pair_data.get("txns", {}).get("m5", {}).get("sells", 0) or 0)

    ts = pair_data.get("pairCreatedAt")
    if ts:
        ah = (time.time() - int(ts)/1000) / 3600
        age = f"{round(ah*60)}m" if ah < 1 else (f"{round(ah,1)}h" if ah < 48 else f"{round(ah/24,1)}d")
    else:
        age = "Unknown"

    t1h = b1h + s1h
    bp  = round(b1h/t1h*100) if t1h > 0 else 0
    lm  = round(liq/mc*100, 1) if mc > 0 else 0

    prev = token_history.get(addr)
    acc  = score_token(pair_data, prev, is_waking)
    vir  = virality(pair_data)

    # Parallel security + holder + dev data
    (hp_data, src, gp_data, gmgn,
     top_holders, deployer, total_supply) = await asyncio.gather(
        honeypot_check(session, addr),
        get_contract_source(session, addr),
        goplus_check(session, addr),
        gmgn_holders(session, addr),
        get_top_holders(session, addr),
        get_deployer(session, addr),
        rpc_total_supply(session, addr),
        return_exceptions=True
    )

    hp  = hp_data if isinstance(hp_data, dict) else {}
    gp  = gp_data if isinstance(gp_data, dict) else {}
    src = src if isinstance(src, str) else ""
    top_holders  = top_holders if isinstance(top_holders, list) else []
    deployer     = deployer if isinstance(deployer, str) else None
    total_supply = total_supply if isinstance(total_supply, int) else None
    gmgn         = gmgn if isinstance(gmgn, int) else None

    is_hp         = hp.get("isHoneypot", False)
    hp_reason     = hp.get("honeypotResult", {}).get("reason", "")
    buy_tax       = hp.get("simulationResult", {}).get("buyTax")
    sell_tax      = hp.get("simulationResult", {}).get("sellTax")
    contract      = analyze_contract(src)
    renounced     = gp.get("owner_address","") == "0x0000000000000000000000000000000000000000"
    can_take_back = gp.get("can_take_back_ownership","0") == "1"
    is_mintable   = gp.get("is_mintable","0") == "1"
    is_proxy      = gp.get("is_proxy","0") == "1"
    hidden_owner  = gp.get("hidden_owner","0") == "1"

    # Top 10 from BSCScan (accurate)
    top10_pct = 0.0
    top10_str = "Unknown"
    if top_holders:
        try:
            total_pct = sum(
                float(h.get("percentage", 0) or 0)
                for h in top_holders[:10]
                if h.get("percentage")
            )
            if total_pct > 0:
                top10_pct = total_pct
                top10_str = f"{round(total_pct, 1)}% (BSCScan)"
            else:
                # BSCScan sometimes gives TokenSupply-relative quantities
                quantities = [int(h.get("TokenHolderQuantity", 0) or 0) for h in top_holders[:10]]
                if total_supply and total_supply > 0 and sum(quantities) > 0:
                    top10_pct = sum(quantities) / total_supply * 100
                    top10_str = f"{round(top10_pct, 1)}% (BSCScan)"
        except Exception as e:
            logger.warning(f"Top holders calc error: {e}")
            top10_str = "Calc error"

    # Holder count cross-reference
    gp_holders = gp.get("holder_count")
    holder_sources = {}
    if gp_holders:
        holder_sources["GoPlus"] = int(gp_holders)
    if gmgn:
        holder_sources["GMGN"] = int(gmgn)
    if holder_sources:
        best = max(holder_sources.values())
        if len(holder_sources) == 2:
            gv, gm = holder_sources.get("GoPlus",0), holder_sources.get("GMGN",0)
            hdisplay = f"{best:,} (GoPlus: {gv:,} | GMGN: {gm:,})"
            if abs(gv-gm) > max(gv, gm)*0.5 and abs(gv-gm) > 50:
                hdisplay += " ⚠️"
        else:
            hdisplay = f"{best:,} (via {list(holder_sources.keys())[0]})"
    else:
        hdisplay = "Unknown"

    # Dev holding — from deployer wallet via RPC
    dev_pct = None
    dev_str = "Unknown"
    if deployer and total_supply:
        dev_pct = await get_dev_holding(session, addr, deployer, total_supply)
        if dev_pct is not None:
            dev_str = f"{round(dev_pct, 2)}% of supply"
            if dev_pct > 20:
                dev_str += " 🔴 HIGH"
            elif dev_pct > 10:
                dev_str += " ⚠️"
            elif dev_pct == 0:
                dev_str = "0% (sold/burned)"
            else:
                dev_str += " ✅"
        else:
            dev_str = f"Deployer: `{deployer[:8]}...` (balance unavailable)"
    elif deployer:
        dev_str = f"Deployer: `{deployer[:8]}...`"

    dex_paid = dex_paid_status(pair_data)
    dump_flag, dump_reason = is_dump(pair_data)
    risk = risk_label(is_hp, contract["score"], top10_pct, gp)
    narr = narrative(name, symbol)
    pred = predict(pair_data, acc["score"], is_waking, dump_flag)

    is_micro = 0 < mc <= MICRO_CAP_MAX
    mc_tag   = "💎 MICRO" if is_micro else "📊 STD"
    if is_new:    atype = f"🆕 NEW — {new_age_str}"
    elif is_waking: atype = "💤🔥 SLEEPING GIANT"
    else:         atype = "📡 ACCUMULATION"

    cflags = "\n".join(contract["flags"]) if contract["flags"] else "✅ No dangerous functions"

    msg = (
        f"🟡 *BSC — {atype}* {mc_tag}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 *{name}* `${symbol}`\n"
        f"🏦 {dex_name} | ⏱ Age: {age}\n"
        f"📍 `{addr}`\n\n"
    )
    if is_waking:
        msg += f"🚨 *{wake_signal}*\n\n"

    msg += (
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 *MARKET DATA*\n"
        f"💰 Price: ${price_str}\n"
        f"📈 MCap: ${mc:,.0f} | 💧 Liq: ${liq:,.0f} ({lm}%)\n"
        f"📦 Vol: 5m ${v5m:,.0f} | 1h ${v1h:,.0f} | 24h ${v24h:,.0f}\n"
        f"📉 5m: {c5m}% | 1h: {c1h}% | 6h: {c6h}% | 24h: {c24h}%\n"
        f"🔄 5m: {b5m}B/{s5m}S | 1h: {b1h}B/{s1h}S ({bp}% buys)\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🛡 *SECURITY — {risk}*\n"
        f"🍯 Honeypot: {'🔴 YES — AVOID' if is_hp else '🟢 Clean'}\n"
    )
    if hp_reason:
        msg += f"   └ {hp_reason}\n"
    msg += (
        f"📄 Verified: {'✅' if contract['verified'] else '❌ NO'} | "
        f"🔑 Renounced: {'✅' if renounced else '⚠️ No'}\n"
        f"🖨️ Mint: {'🔴' if is_mintable else '✅'} | "
        f"🌀 Proxy: {'🔴' if is_proxy else '✅'} | "
        f"👻 Hidden owner: {'🔴' if hidden_owner else '✅'}\n"
        f"💸 Tax: Buy {f'{buy_tax}%' if buy_tax is not None else '?'} / "
        f"Sell {f'{sell_tax}%' if sell_tax is not None else '?'}\n"
        f"🐋 Top 10 holders: {top10_str}\n"
        f"👥 Total holders: {hdisplay}\n"
        f"👨‍💻 Dev holding: {dev_str}\n"
        f"💳 DEX paid: {dex_paid}\n"
        f"🔬 Contract: {contract['score']}/100\n"
        f"{cflags}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    if dump_flag:
        msg += f"⛔ *DUMP: {dump_reason}*\n\n"

    msg += (
        f"🔥 *SCORE: {acc['score']}/100*\n"
        f"{chr(10).join(acc['signals']) or 'No strong signals'}\n\n"
        f"📣 *VIRAL: {vir['score']}/100*\n"
        f"{chr(10).join(vir['signals']) or 'Low activity'}\n\n"
        f"🎭 *NARRATIVE:* {narr}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 *PREDICTION:* {pred}\n\n"
        f"🔗 [DexScreener]({dex_url}) | [BSCScan](https://bscscan.com/token/{addr})"
    )

    # Eye button — tap to watch this token for 50%+ move re-alerts
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "👁 Watch this token",
            callback_data=f"watch_{addr}_{price_str}"
        )
    ]])

    return msg, keyboard


# ── RPC SCAN — NEW PAIR DISCOVERY ─────────────────────────────────────────────

async def rpc_scan_new_pairs(session: aiohttp.ClientSession, app: Application):
    """
    Directly monitor PancakeSwap V2 factory for new pair creation events.
    This catches tokens the moment they're created — no DexScreener dependency.
    """
    global last_rpc_block

    if not subscribed_chats:
        return

    current_block = await rpc_block_number(session)
    if not current_block:
        logger.warning("Could not get block number")
        return

    if last_rpc_block == 0:
        last_rpc_block = current_block - 100  # start from last 100 blocks on first run

    # Don't scan too many blocks at once
    from_block = last_rpc_block + 1
    to_block   = min(current_block, from_block + 200)

    if from_block > to_block:
        return

    logs = await rpc_get_logs(session, from_block, to_block)
    last_rpc_block = to_block

    if not logs:
        return

    logger.info(f"RPC: {len(logs)} new pairs found in blocks {from_block}-{to_block}")

    for log in logs:
        try:
            token_addr = parse_pair_created_log(log)
            if not token_addr:
                continue

            # Wait a moment for DexScreener to index it
            await asyncio.sleep(3)

            pair_data = await dex_token(session, token_addr)
            if not pair_data:
                # DexScreener hasn't indexed yet — add to history for later pickup
                token_history[token_addr] = {
                    "vol_1h": 0, "vol_24h": 0, "price": None,
                    "ticker": "???", "timestamp": time.time(), "scan_count": 0,
                }
                logger.info(f"New pair from RPC (not yet on DexScreener): {token_addr[:10]}")
                continue

            liq = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)
            if liq < MIN_LIQ_MICRO:
                continue

            dump_flag, _ = is_dump(pair_data)
            if dump_flag:
                continue

            if token_addr in seen_new_pairs:
                continue

            ts = pair_data.get("pairCreatedAt")
            age_h = (time.time() - int(ts)/1000) / 3600 if ts else 0
            new_age_str = f"{round(age_h*60)}m old" if age_h < 1 else f"{round(age_h,1)}h old"

            prev = token_history.get(token_addr)
            acc  = score_token(pair_data, prev, False)

            token_history[token_addr] = {
                "vol_1h": float(pair_data.get("volume", {}).get("h1", 0) or 0),
                "vol_24h": float(pair_data.get("volume", {}).get("h24", 0) or 0),
                "price": pair_data.get("priceUsd"),
                "ticker": pair_data.get("baseToken", {}).get("symbol", "???"),
                "timestamp": time.time(),
                "scan_count": 1,
            }

            if acc["score"] < MIN_SCORE_NEW:
                continue
            if token_addr in alerted_tokens:
                continue

            seen_new_pairs.add(token_addr)
            name = pair_data.get("baseToken", {}).get("name", "?")
            mc   = float(pair_data.get("marketCap", 0) or 0)
            logger.info(f"RPC NEW PAIR ALERT: {name} mc=${mc:,.0f} score={acc['score']}")

            report, markup = await build_report(
                session, pair_data, is_new=True, new_age_str=new_age_str
            )
            alerted_tokens[token_addr] = {
                "ts": time.time(),
                "price": float(pair_data.get("priceUsd") or 0)
            }

            for chat_id in list(subscribed_chats):
                try:
                    await app.bot.send_message(
                        chat_id=chat_id, text=report,
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=markup,
                        disable_web_page_preview=True
                    )
                    await asyncio.sleep(0.3)
                except Exception as e:
                    logger.warning(f"Send error {chat_id}: {e}")

        except Exception as e:
            logger.warning(f"RPC pair processing error: {e}")


# ── MAIN SCAN LOOP ────────────────────────────────────────────────────────────

async def run_scan(session: aiohttp.ClientSession, app: Application):
    if not subscribed_chats:
        return

    logger.info("Scan cycle...")
    pairs = await fetch_bsc_pairs(session)

    for pair_data in pairs:
        try:
            addr   = pair_data.get("baseToken", {}).get("address", "")
            liq    = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)
            mc     = float(pair_data.get("marketCap", 0) or 0)
            v1h    = float(pair_data.get("volume", {}).get("h1", 0) or 0)
            v24h   = float(pair_data.get("volume", {}).get("h24", 0) or 0)
            price  = float(pair_data.get("priceUsd") or 0)

            if not addr:
                continue

            is_micro = 0 < mc <= MICRO_CAP_MAX
            min_liq  = MIN_LIQ_MICRO if is_micro else MIN_LIQ_STD

            prev = token_history.get(addr)
            token_history[addr] = {
                "vol_1h": v1h, "vol_24h": v24h,
                "price": pair_data.get("priceUsd"),
                "ticker": pair_data.get("baseToken", {}).get("symbol", "???"),
                "mcap": mc,
                "timestamp": time.time(),
                "scan_count": (prev.get("scan_count",0) if prev else 0) + 1,
            }

            if liq < min_liq:
                continue

            dump_flag, dump_reason = is_dump(pair_data)
            if dump_flag:
                continue

            is_new_flag = False
            new_age_str = ""
            ts = pair_data.get("pairCreatedAt")
            if ts:
                age_h = (time.time() - int(ts)/1000) / 3600
                if age_h <= NEW_PAIR_MAX_AGE_H and addr not in seen_new_pairs:
                    is_new_flag = True
                    new_age_str = f"{round(age_h,1)}h old" if age_h >= 1 else f"{round(age_h*60)}m old"

            is_waking, wake_signal = is_sleeping_giant(pair_data, prev)
            acc = score_token(pair_data, prev, is_waking)

            # Alert decision
            should_alert = False
            if is_new_flag and acc["score"] >= MIN_SCORE_NEW:
                should_alert = True
                seen_new_pairs.add(addr)
            elif is_waking and acc["score"] >= MIN_SCORE_WAKE:
                should_alert = True
            elif acc["score"] >= MIN_SCORE_STD:
                should_alert = True

            if not should_alert:
                continue

            # One alert per token — permanent block unless watched or in favs
            already_alerted = addr in alerted_tokens

            # Check if in any chat's fav or watch list
            is_watched = any(
                addr in watched_tokens.get(cid, {})
                for cid in subscribed_chats
            )
            is_faved = any(
                addr in favourites.get(cid, {})
                for cid in subscribed_chats
            )

            if already_alerted and not is_watched and not is_faved:
                continue

            # If watched/faved, check 50% move threshold
            if already_alerted and (is_watched or is_faved):
                last_price = alerted_tokens[addr].get("price", 0)
                if last_price > 0 and price > 0:
                    move = abs((price - last_price) / last_price * 100)
                    if move < WATCH_ALERT_PCT:
                        continue

            name = pair_data.get("baseToken", {}).get("name", "?")
            logger.info(f"ALERT {name} score={acc['score']} new={is_new_flag} waking={is_waking} mc=${mc:,.0f}")

            report, markup = await build_report(
                session, pair_data, is_waking, wake_signal, is_new_flag, new_age_str
            )
            alerted_tokens[addr] = {"ts": time.time(), "price": price}

            for chat_id in list(subscribed_chats):
                try:
                    await app.bot.send_message(
                        chat_id=chat_id, text=report,
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=markup,
                        disable_web_page_preview=True
                    )
                    await asyncio.sleep(0.3)
                except Exception as e:
                    logger.warning(f"Send error {chat_id}: {e}")

        except Exception as e:
            logger.warning(f"Pair error: {e}")


# ── FAV MOVE CHECKER ──────────────────────────────────────────────────────────

async def check_fav_moves(app: Application):
    if not favourites:
        return
    async with aiohttp.ClientSession() as session:
        for chat_id, favs in favourites.items():
            for addr, info in favs.items():
                try:
                    pair = await dex_token(session, addr)
                    if not pair:
                        continue
                    c1h    = float(pair.get("priceChange", {}).get("h1", 0) or 0)
                    price  = pair.get("priceUsd", "N/A")
                    ticker = pair.get("baseToken", {}).get("symbol", info.get("ticker","???"))
                    url    = pair.get("url", f"https://dexscreener.com/bsc/{addr}")
                    if abs(c1h) >= FAV_ALERT_PCT:
                        key = f"fav_{addr}_{round(c1h/10)*10}"
                        if time.time() - alerted_tokens.get(key, {}).get("ts", 0) < 3600:
                            continue
                        alerted_tokens[key] = {"ts": time.time(), "price": 0}
                        direction = "🚀 UP" if c1h > 0 else "🔴 DOWN"
                        await app.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"⭐ *FAV ALERT — {ticker}*\n\n"
                                f"Moved {direction} *{c1h}%* in 1h\n"
                                f"💰 Price: ${price}\n\n"
                                f"[View on DexScreener]({url})"
                            ),
                            parse_mode=ParseMode.MARKDOWN,
                            disable_web_page_preview=True
                        )
                except Exception as e:
                    logger.warning(f"Fav move check error {addr}: {e}")


# ── EYE BUTTON CALLBACK ───────────────────────────────────────────────────────

async def handle_watch_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    chat_id = query.message.chat_id
    data    = query.data  # format: watch_{addr}_{price}

    try:
        parts  = data.split("_", 2)
        addr   = parts[1]
        price  = float(parts[2]) if len(parts) > 2 else 0
    except Exception:
        await query.answer("Error parsing token data.", show_alert=True)
        return

    if chat_id not in watched_tokens:
        watched_tokens[chat_id] = {}

    if addr in watched_tokens[chat_id]:
        # Toggle off
        ticker = token_history.get(addr, {}).get("ticker", addr[:8])
        del watched_tokens[chat_id][addr]
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("👁 Watch this token", callback_data=f"watch_{addr}_{price}")
        ]]))
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"👁 Stopped watching *${ticker}*",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        # Toggle on
        watched_tokens[chat_id][addr] = {"alert_price": price, "added_ts": time.time()}
        ticker = token_history.get(addr, {}).get("ticker", addr[:8])
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("👁 Watching ✅ (tap to stop)", callback_data=f"watch_{addr}_{price}")
        ]]))
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"👁 Now watching *${ticker}*\n\n"
                f"I'll alert you again if it moves ±{WATCH_ALERT_PCT}% from current price."
            ),
            parse_mode=ParseMode.MARKDOWN
        )


# ── FAVOURITES ────────────────────────────────────────────────────────────────

async def build_fav_board(session: aiohttp.ClientSession, chat_id: int) -> str:
    favs = favourites.get(chat_id, {})
    if not favs:
        return "⭐ *FAVOURITES*\n\nEmpty. Use /fav <address> to add tokens."
    lines = ["⭐ *FAVOURITES — LIVE*\n"]
    for addr, info in favs.items():
        pair = await dex_token(session, addr)
        if pair:
            price  = pair.get("priceUsd", "N/A")
            mc     = float(pair.get("marketCap", 0) or 0)
            v1h    = float(pair.get("volume", {}).get("h1", 0) or 0)
            c1h    = pair.get("priceChange", {}).get("h1", "0")
            c24h   = pair.get("priceChange", {}).get("h24", "0")
            liq    = float(pair.get("liquidity", {}).get("usd", 0) or 0)
            ticker = pair.get("baseToken", {}).get("symbol", info.get("ticker","???"))
            url    = pair.get("url", f"https://dexscreener.com/bsc/{addr}")
            favs[addr]["ticker"] = ticker
            e = "🟢" if float(c1h or 0) >= 0 else "🔴"
            lines.append(
                f"[${ticker}]({url})\n"
                f"💰 ${price} | MCap: ${mc:,.0f}\n"
                f"📦 Vol 1h: ${v1h:,.0f} | 💧 Liq: ${liq:,.0f}\n"
                f"{e} 1h: {c1h}% | 24h: {c24h}%\n"
                f"`{addr}`\n"
            )
        else:
            lines.append(f"${info.get('ticker','???')} — `{addr}` _(unavailable)_\n")
    return "\n".join(lines)


async def update_pinned_favs(app: Application, chat_id: int):
    async with aiohttp.ClientSession() as session:
        board = await build_fav_board(session, chat_id)
    try:
        msg_id = pinned_msg_ids.get(chat_id)
        if msg_id:
            await app.bot.edit_message_text(
                chat_id=chat_id, message_id=msg_id,
                text=board, parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True
            )
        else:
            msg = await app.bot.send_message(
                chat_id=chat_id, text=board,
                parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True
            )
            pinned_msg_ids[chat_id] = msg.message_id
            await app.bot.pin_chat_message(
                chat_id=chat_id, message_id=msg.message_id,
                disable_notification=True
            )
    except Exception as e:
        logger.warning(f"Pin update error: {e}")


# ── COMMANDS ──────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    subscribed_chats.add(chat_id)
    await context.bot.set_my_commands([
        BotCommand("start",     "Activate scanner"),
        BotCommand("stop",      "Pause alerts"),
        BotCommand("scan",      "Scan any BSC token"),
        BotCommand("watchlist", "View tracked tokens"),
        BotCommand("fav",       "Add to favourites"),
        BotCommand("unfav",     "Remove from favourites"),
        BotCommand("favlist",   "Live prices for favourites"),
        BotCommand("status",    "Scanner stats"),
        BotCommand("filters",   "Current settings"),
    ])
    await update.message.reply_text(
        "🟡 *BSC MEME SCANNER v4 ACTIVATED*\n\n"
        "Now using direct RPC blockchain monitoring.\n"
        "Catches tokens the moment they're created on PancakeSwap,\n"
        "and tracks ALL existing tokens for wakeup signals.\n\n"
        "Hunting:\n"
        "• 🆕 New pairs — direct from blockchain\n"
        "• 💤🔥 Sleeping tokens waking up — any age\n"
        "• 💎 Micro caps showing early volume\n"
        "• 📈 Accumulation patterns\n\n"
        "One alert per token. Tap 👁 to keep watching.\n"
        "Use menu below ↓",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    subscribed_chats.discard(update.effective_chat.id)
    await update.message.reply_text("⏸ Paused. /start to resume.")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    watched_count = sum(len(v) for v in watched_tokens.values())
    fav_count     = sum(len(v) for v in favourites.values())
    await update.message.reply_text(
        f"*BSC SCANNER STATUS*\n\n"
        f"✅ Running | 📡 RPC + DexScreener\n"
        f"⏱ DexScreener scan: {SCAN_INTERVAL}s\n"
        f"⛓ RPC new-pair scan: {RPC_SCAN_INTERVAL}s\n"
        f"🪙 Tokens in memory: {len(token_history)}\n"
        f"🔔 Alerted: {len(alerted_tokens)}\n"
        f"👁 Watched: {watched_count}\n"
        f"⭐ Favourites: {fav_count}\n"
        f"👥 Subscribers: {len(subscribed_chats)}\n"
        f"⛓ Last block: {last_rpc_block:,}",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_filters(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"*BSC FILTERS*\n\n"
        f"🆕 New pairs (≤{NEW_PAIR_MAX_AGE_H}h): score ≥{MIN_SCORE_NEW}\n"
        f"💤 Sleeping giant: score ≥{MIN_SCORE_WAKE}\n"
        f"📊 Standard: score ≥{MIN_SCORE_STD}\n\n"
        f"💧 Min liq micro: ${MIN_LIQ_MICRO:,}\n"
        f"💧 Min liq standard: ${MIN_LIQ_STD:,}\n"
        f"💎 Micro cap ceiling: ${MICRO_CAP_MAX:,}\n\n"
        f"⛔ Max sell ratio: {round(MAX_SELL_RATIO*100)}%\n"
        f"⛔ Max drop 1h: {MAX_DROP_1H}%\n\n"
        f"👁 Re-alert threshold: ±{WATCH_ALERT_PCT}%\n"
        f"⭐ Fav alert threshold: ±{FAV_ALERT_PCT}%",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not token_history:
        await update.message.reply_text("No tokens in memory yet. Give it a few minutes.")
        return

    sorted_tokens = sorted(
        token_history.items(),
        key=lambda x: x[1].get("mcap", 0),
        reverse=True
    )[:20]

    lines = ["*TOP 20 TRACKED TOKENS BY MCAP*\n"]
    for addr, h in sorted_tokens:
        mc     = h.get("mcap", 0)
        scans  = h.get("scan_count", 0)
        ticker = h.get("ticker", "???")
        link   = f"https://dexscreener.com/bsc/{addr}"
        lines.append(
            f"[${ticker}]({link})\n"
            f"`{addr}` | MCap: ${mc:,.0f} | Scans: {scans}"
        )

    await update.message.reply_text(
        "\n\n".join(lines),
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True
    )


async def cmd_fav(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Usage: /fav <BSC token address>")
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
    msg = await update.message.reply_text("⭐ Adding...")
    async with aiohttp.ClientSession() as session:
        pair = await dex_token(session, addr)
    ticker = pair.get("baseToken",{}).get("symbol","???") if pair else "???"
    price  = float(pair.get("priceUsd",0) or 0) if pair else 0
    favourites[chat_id][addr] = {"ticker": ticker, "added_ts": time.time(), "last_price": price}
    await msg.edit_text(f"⭐ Added *${ticker}* to favourites.", parse_mode=ParseMode.MARKDOWN)
    await update_pinned_favs(context.application, chat_id)


async def cmd_unfav(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Usage: /unfav <BSC token address>")
        return
    addr = context.args[0].strip()
    if chat_id in favourites and addr in favourites[chat_id]:
        ticker = favourites[chat_id][addr].get("ticker","???")
        del favourites[chat_id][addr]
        await update.message.reply_text(f"Removed *${ticker}*.", parse_mode=ParseMode.MARKDOWN)
        await update_pinned_favs(context.application, chat_id)
    else:
        await update.message.reply_text("Not in favourites.")


async def cmd_favlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not favourites.get(chat_id):
        await update.message.reply_text("No favourites. Use /fav <address> to add.")
        return
    msg = await update.message.reply_text("⭐ Fetching live data...")
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
    address = context.args[0].strip()
    msg = await update.message.reply_text("🔍 Scanning...")
    async with aiohttp.ClientSession() as session:
        pair_data = await dex_token(session, address)
        if not pair_data:
            await msg.edit_text("❌ Token not found on any BSC DEX.")
            return
        prev = token_history.get(address)
        is_waking, wake_signal = is_sleeping_giant(pair_data, prev)
        report, markup = await build_report(session, pair_data, is_waking, wake_signal)
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
    app.add_handler(CommandHandler("scan",      cmd_scan))
    app.add_handler(CommandHandler("fav",       cmd_fav))
    app.add_handler(CommandHandler("unfav",     cmd_unfav))
    app.add_handler(CommandHandler("favlist",   cmd_favlist))
    app.add_handler(CallbackQueryHandler(handle_watch_callback, pattern=r"^watch_"))

    async def dex_scan_job(ctx):
        async with aiohttp.ClientSession() as session:
            await run_scan(session, app)

    async def rpc_scan_job(ctx):
        async with aiohttp.ClientSession() as session:
            await rpc_scan_new_pairs(session, app)

    async def fav_job(ctx):
        await check_fav_moves(app)

    app.job_queue.run_repeating(dex_scan_job, interval=SCAN_INTERVAL,     first=15)
    app.job_queue.run_repeating(rpc_scan_job, interval=RPC_SCAN_INTERVAL, first=10)
    app.job_queue.run_repeating(fav_job,      interval=FAV_CHECK_INTERVAL, first=90)

    logger.info("BSC Scanner v4 starting — RPC + DexScreener")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
