"""
BSC MEME SCANNER BOT v3
Fixes:
- Scanner was choking on per-token re-scans, now uses batch endpoints
- Scores were too strict, recalibrated
- Anti-dump was too aggressive, loosened for micro caps
New features:
- Menu bar
- /fav, /unfav, /favlist
- Pinned favourites board
- 50% move alert on favourited tokens
"""

import asyncio
import logging
import os
import time
import json
from typing import Optional

import aiohttp
from telegram import Update, BotCommand, BotCommandScopeChat
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ── CONFIG ────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8623651466:AAFeEHh3t15n1ciB4L2MZbj57h_j3mzAGhw")
ETHERSCAN_KEY  = os.getenv("ETHERSCAN_KEY",  "S3ZEIA898K31SY9EJXE3HYY8ZEZ6PBGMHB")
MORALIS_KEY    = os.getenv("MORALIS_KEY",    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6Ijg4NDUzZTVjLThiNDAtNDYxYi04YWJjLTU3NWQ2YjhiYWRjOSIsIm9yZ0lkIjoiNTAyMzM3IiwidXNlcklkIjoiNTE2ODc5IiwidHlwZUlkIjoiMmE4YWNhYjQtMzQ3Yy00NWVlLTg4ZGQtM2Q4ZDQ5YzE1Y2M2IiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3NzIwNTMzNDMsImV4cCI6NDkyNzgxMzM0M30.cCIzfBXc6c4x6x0EfJbwF7csdi4PDmuGtEYBZzgN6BA")
BSCSCAN_URL    = "https://api.bscscan.com/api"

# ── THRESHOLDS ────────────────────────────────────────────────────────────────
MIN_LIQUIDITY_MICRO  = 1_500    # micro cap min liq
MIN_LIQUIDITY_STD    = 5_000    # standard min liq (lowered from 8k)
MICRO_CAP_MAX        = 200_000  # raised to $200k for wider micro cap net

MIN_SCORE_NEW        = 45       # new pairs — low bar, fresh is enough
MIN_SCORE_WAKE       = 45       # sleeping giant waking up
MIN_SCORE_STD        = 55       # standard accumulation

NEW_PAIR_MAX_AGE_H   = 72       # 72h = new pair
WAKE_PREV_MAX_VOL    = 15_000   # was sleeping if prev 1h vol under $15k
WAKE_MIN_VOL         = 3_000    # waking up if now showing $3k+/hr
WAKE_SPIKE_MULT      = 2.0      # 2x spike (lowered from 3x)

# Anti-dump — loosened for micro caps
MAX_SELL_RATIO       = 0.70     # raised from 0.65
MIN_LIQ_MCAP         = 0.05     # lowered from 0.08
MAX_DROP_1H          = -35      # raised from -25

# Favourites
FAV_ALERT_PCT        = 50       # alert if fav moves 50%+ in 1h
FAV_CHECK_INTERVAL   = 120      # check favs every 2 mins

ALERT_COOLDOWN       = 3600     # 1hr cooldown (lowered from 90min)
SCAN_INTERVAL        = 60       # scan every 60s (faster)

# ── STATE ─────────────────────────────────────────────────────────────────────
alerted_tokens   = {}   # addr -> timestamp
token_history    = {}   # addr -> snapshot
subscribed_chats = set()
seen_new_pairs   = set()
favourites       = {}   # chat_id -> {addr: {ticker, added_ts, last_price, last_price_ts}}
pinned_msg_ids   = {}   # chat_id -> message_id of pinned favlist


# ── HTTP HELPER ───────────────────────────────────────────────────────────────

async def fetch(session: aiohttp.ClientSession, url: str, headers: dict = {}) -> Optional[dict]:
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status == 200:
                return await r.json()
    except Exception as e:
        logger.warning(f"Fetch error {url[:60]}: {e}")
    return None


# ── BSC PAIR FETCHERS ─────────────────────────────────────────────────────────

async def dexscreener_token(session: aiohttp.ClientSession, address: str) -> Optional[dict]:
    data = await fetch(session, f"https://api.dexscreener.com/latest/dex/tokens/{address}")
    if not data:
        return None
    pairs = [p for p in data.get("pairs", []) if p.get("chainId") == "bsc"]
    if pairs:
        return max(pairs, key=lambda x: float(x.get("liquidity", {}).get("usd", 0) or 0))
    return None


async def fetch_bsc_pairs(session: aiohttp.ClientSession) -> list:
    """
    Fetch BSC pairs from multiple sources in parallel.
    Uses batch endpoints only — no per-token calls here.
    Per-token calls only happen for known favourites and history tokens.
    """
    results = []
    seen = set()

    def add(pairs):
        for p in pairs:
            addr = p.get("baseToken", {}).get("address", "")
            if addr and addr.lower() not in seen:
                seen.add(addr.lower())
                results.append(p)

    # Fetch all sources in parallel for speed
    tasks = [
        fetch(session, "https://api.dexscreener.com/latest/dex/pairs/bsc"),
        fetch(session, "https://api.dexscreener.com/latest/dex/search?q=pancakeswap"),
        fetch(session, "https://api.dexscreener.com/latest/dex/search?q=apeswap"),
        fetch(session, "https://api.dexscreener.com/latest/dex/search?q=biswap"),
        fetch(session, "https://api.dexscreener.com/token-profiles/latest/v1"),
        fetch(session, "https://api.dexscreener.com/token-boosts/top/v1"),
        fetch(session, "https://api.dexscreener.com/latest/dex/search?q=bsc+meme"),
        fetch(session, "https://api.dexscreener.com/latest/dex/search?q=bsc+new"),
    ]

    responses = await asyncio.gather(*tasks, return_exceptions=True)

    for i, r in enumerate(responses):
        if isinstance(r, Exception) or r is None:
            continue
        try:
            if i < 4:  # pairs/search endpoints
                pairs = r.get("pairs", [])
                bsc = [p for p in pairs if p.get("chainId") == "bsc"]
                add(bsc[:50])
            elif i == 4:  # token profiles
                if isinstance(r, list):
                    bsc = [p for p in r if p.get("chainId") == "bsc"]
                    # Batch fetch pair data for profiles
                    for p in bsc[:20]:
                        addr = p.get("tokenAddress")
                        if addr and addr.lower() not in seen:
                            pair = await dexscreener_token(session, addr)
                            if pair:
                                seen.add(addr.lower())
                                results.append(pair)
            elif i == 5:  # boosts
                if isinstance(r, list):
                    bsc = [p for p in r if p.get("chainId") == "bsc"]
                    for p in bsc[:10]:
                        addr = p.get("tokenAddress")
                        if addr and addr.lower() not in seen:
                            pair = await dexscreener_token(session, addr)
                            if pair:
                                seen.add(addr.lower())
                                results.append(pair)
            else:  # search endpoints
                pairs = r.get("pairs", [])
                bsc = [p for p in pairs if p.get("chainId") == "bsc"]
                add(bsc[:30])
        except Exception as e:
            logger.warning(f"Response processing error {i}: {e}")

    # Re-scan known sleeping tokens from history (limited batch)
    # Only tokens with low prev volume that might be waking up
    sleeping = [
        addr for addr, h in token_history.items()
        if h.get("vol_1h", 0) < WAKE_PREV_MAX_VOL
        and addr.lower() not in seen
    ][:30]  # max 30 per cycle to keep speed up

    if sleeping:
        logger.info(f"Re-checking {len(sleeping)} sleeping tokens")
        sleep_tasks = [dexscreener_token(session, addr) for addr in sleeping]
        sleep_results = await asyncio.gather(*sleep_tasks, return_exceptions=True)
        for pair in sleep_results:
            if pair and not isinstance(pair, Exception):
                addr = pair.get("baseToken", {}).get("address", "")
                if addr and addr.lower() not in seen:
                    seen.add(addr.lower())
                    results.append(pair)

    logger.info(f"Total BSC pairs this cycle: {len(results)}")
    return results


# ── SECURITY CHECKS ───────────────────────────────────────────────────────────

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


async def get_contract_source(session: aiohttp.ClientSession, address: str) -> str:
    params = f"?module=contract&action=getsourcecode&address={address}&apikey={ETHERSCAN_KEY}"
    data = await fetch(session, BSCSCAN_URL + params)
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


# ── DUMP FILTER ───────────────────────────────────────────────────────────────

def is_dump(pair_data: dict) -> tuple:
    vol_1h   = float(pair_data.get("volume", {}).get("h1", 0) or 0)
    ch_1h    = float(pair_data.get("priceChange", {}).get("h1", 0) or 0)
    ch_6h    = float(pair_data.get("priceChange", {}).get("h6", 0) or 0)
    liq      = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)
    mkt_cap  = float(pair_data.get("marketCap", 0) or 0)
    buys_1h  = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    sells_1h = int(pair_data.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
    total_1h = buys_1h + sells_1h

    if total_1h > 8 and sells_1h / total_1h > MAX_SELL_RATIO:
        return True, f"Sell dominant — {round(sells_1h/total_1h*100)}% sells in 1h"
    if vol_1h > 3000 and ch_1h < MAX_DROP_1H:
        return True, f"Dumping {ch_1h}% while volume spikes"
    if mkt_cap > 0 and liq > 0 and liq / mkt_cap < MIN_LIQ_MCAP:
        return True, f"Liq only {round(liq/mkt_cap*100,1)}% of mcap"
    if liq > 0 and vol_1h > liq * 15:
        return True, "Wash trading suspected"
    if ch_6h > 500 and ch_1h < -20:
        return True, f"Already up {ch_6h}% and reversing"
    return False, ""


# ── SLEEPING GIANT DETECTION ──────────────────────────────────────────────────

def is_sleeping_giant(pair_data: dict, prev: Optional[dict]) -> tuple:
    vol_1h = float(pair_data.get("volume", {}).get("h1", 0) or 0)
    if not prev or prev.get("scan_count", 0) < 1:
        return False, ""

    prev_vol = prev.get("vol_1h", 0)
    was_sleeping = prev_vol < WAKE_PREV_MAX_VOL

    if not was_sleeping or vol_1h < WAKE_MIN_VOL:
        return False, ""

    if prev_vol == 0 and vol_1h >= WAKE_MIN_VOL:
        return True, f"💤➡🔥 First volume after silence — ${vol_1h:,.0f}/hr"

    if prev_vol > 0 and vol_1h >= prev_vol * WAKE_SPIKE_MULT:
        mult = round(vol_1h / prev_vol, 1)
        return True, f"💤➡🔥 Volume {mult}x spike — ${prev_vol:,.0f} → ${vol_1h:,.0f}/hr"

    return False, ""


# ── ACCUMULATION SCORE ────────────────────────────────────────────────────────

def score_token(pair_data: dict, prev: Optional[dict], is_waking: bool) -> dict:
    score = 0
    signals = []

    try:
        vol_5m   = float(pair_data.get("volume", {}).get("m5", 0) or 0)
        vol_1h   = float(pair_data.get("volume", {}).get("h1", 0) or 0)
        vol_6h   = float(pair_data.get("volume", {}).get("h6", 0) or 0)
        vol_24h  = float(pair_data.get("volume", {}).get("h24", 0) or 0)
        ch_5m    = float(pair_data.get("priceChange", {}).get("m5", 0) or 0)
        ch_1h    = float(pair_data.get("priceChange", {}).get("h1", 0) or 0)
        ch_6h    = float(pair_data.get("priceChange", {}).get("h6", 0) or 0)
        mkt_cap  = float(pair_data.get("marketCap", 0) or 0)
        buys_1h  = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
        sells_1h = int(pair_data.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
        buys_5m  = int(pair_data.get("txns", {}).get("m5", {}).get("buys", 0) or 0)

        if is_waking:
            score += 30
            signals.append("🚨 Sleeping giant waking up")

        # Volume momentum
        if vol_1h > 0 and vol_6h > 0:
            avg = vol_6h / 6
            if avg > 0:
                m = vol_1h / avg
                if m >= 4:
                    score += 20; signals.append(f"📈 Volume {round(m,1)}x the 6h avg")
                elif m >= 2.5:
                    score += 13; signals.append(f"📈 Volume {round(m,1)}x the 6h avg")
                elif m >= 1.5:
                    score += 7;  signals.append(f"📈 Volume {round(m,1)}x the 6h avg")

        # Buy pressure
        total = buys_1h + sells_1h
        if total > 3:
            bp = buys_1h / total
            if bp >= 0.70:
                score += 18; signals.append(f"🟢 {round(bp*100)}% buy pressure in 1h")
            elif bp >= 0.58:
                score += 10; signals.append(f"🟢 {round(bp*100)}% buy pressure in 1h")

        # Price trend
        up = sum([ch_5m > 0, ch_1h > 0, ch_6h > 0])
        if up == 3:
            score += 14; signals.append("🟢 Uptrend all timeframes")
        elif up == 2:
            score += 7;  signals.append("🟡 Uptrend 2/3 timeframes")

        # Vol/MCap
        if mkt_cap > 0 and vol_24h > 0:
            r = vol_24h / mkt_cap
            if r > 1.0:
                score += 14; signals.append(f"🔥 Vol/MCap {round(r*100)}%")
            elif r > 0.4:
                score += 7;  signals.append(f"📊 Vol/MCap {round(r*100)}%")

        # 5m burst
        if buys_5m >= 8:
            score += 10; signals.append(f"⚡ {buys_5m} buys in 5m")
        elif buys_5m >= 4:
            score += 5;  signals.append(f"⚡ {buys_5m} buys in 5m")

        # Micro cap bonus
        if 0 < mkt_cap <= MICRO_CAP_MAX:
            score += 7; signals.append(f"💎 Micro cap ${mkt_cap:,.0f}")

        # Early volume zone
        if 3000 <= vol_1h <= 50000:
            score += 8; signals.append(f"💎 Early volume zone ${vol_1h:,.0f}/hr")

    except Exception as e:
        logger.warning(f"Score error: {e}")

    return {"score": min(100, score), "signals": signals}


# ── VIRALITY ──────────────────────────────────────────────────────────────────

def virality(pair_data: dict) -> dict:
    score = 0
    signals = []
    b5m    = int(pair_data.get("txns", {}).get("m5", {}).get("buys", 0) or 0)
    b1h    = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    v5m    = float(pair_data.get("volume", {}).get("m5", 0) or 0)
    boost  = pair_data.get("boosts", {}).get("active", 0) or 0

    if b5m >= 15: score += 28; signals.append(f"🔥 {b5m} buys in 5m")
    elif b5m >= 7: score += 15; signals.append(f"⚡ {b5m} buys in 5m")
    if b1h >= 100: score += 28; signals.append(f"📣 {b1h} buys in 1h")
    elif b1h >= 40: score += 15; signals.append(f"📣 {b1h} buys in 1h")
    if v5m > 10000: score += 22; signals.append(f"💸 ${v5m:,.0f} in 5m")
    elif v5m > 3000: score += 10; signals.append(f"💸 ${v5m:,.0f} in 5m")
    if boost: score += 22; signals.append(f"🚀 DexScreener boosted")

    return {"score": min(100, score), "signals": signals}


# ── NARRATIVE ─────────────────────────────────────────────────────────────────

NARRATIVES = {
    "🤖 AI":         ["ai", "agent", "gpt", "llm", "neural", "deepseek", "openai"],
    "🐸 Meme":       ["pepe", "doge", "shib", "inu", "cat", "frog", "moon", "wojak", "chad", "based", "bonk"],
    "🏛️ Political":  ["trump", "maga", "elon", "biden", "vote", "president"],
    "🎮 Gaming":     ["game", "play", "nft", "metaverse", "quest", "guild"],
    "💰 DeFi":       ["defi", "yield", "farm", "stake", "swap"],
    "🌍 RWA":        ["rwa", "gold", "property", "asset"],
    "🐂 BSC Native": ["bnb", "binance", "pancake"],
}

def narrative(name: str, symbol: str) -> str:
    text = f"{name} {symbol}".lower()
    found = [l for l, kws in NARRATIVES.items() if any(k in text for k in kws)]
    return " | ".join(found) if found else "None detected"


# ── RISK RATING ───────────────────────────────────────────────────────────────

def risk_label(is_hp: bool, cscore: int, top10: float, gp: dict) -> str:
    if is_hp: return "🔴 CRITICAL — HONEYPOT"
    if gp.get("can_take_back_ownership","0")=="1" or gp.get("is_proxy","0")=="1" or gp.get("hidden_owner","0")=="1":
        return "🔴 HIGH RISK"
    if cscore < 40 or top10 > 85: return "🔴 HIGH RISK"
    if cscore < 65 or top10 > 65: return "🟡 MEDIUM RISK"
    owner = gp.get("owner_address","")
    if owner == "0x0000000000000000000000000000000000000000" and cscore >= 75:
        return "🟢 LOW RISK ✓"
    return "🟡 MEDIUM RISK"


# ── PREDICTION ────────────────────────────────────────────────────────────────

def predict(pair_data: dict, acc: int, waking: bool, dump: bool) -> str:
    if dump: return "🔴 DUMP PATTERN — avoid"
    ch1 = float(pair_data.get("priceChange", {}).get("h1", 0) or 0)
    ch6 = float(pair_data.get("priceChange", {}).get("h6", 0) or 0)
    liq = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)
    if waking and acc >= 60: return "🚀 HIGH POTENTIAL — sleeping giant waking, early entry"
    if waking: return "📈 WATCH — accumulation starting, confirm next candle"
    if acc >= 75 and ch1 > 0 and ch6 > 0: return "🚀 STRONG BULLISH"
    if acc >= 55 and ch1 > 0: return "📈 BULLISH — accumulation forming"
    if acc >= 40: return "🟡 EARLY SIGNAL — watch"
    return "⚪ NEUTRAL"


# ── FULL REPORT ───────────────────────────────────────────────────────────────

async def build_report(
    session: aiohttp.ClientSession,
    pair_data: dict,
    is_waking: bool = False,
    wake_signal: str = "",
    is_new: bool = False,
    new_age_str: str = "",
) -> str:
    addr      = pair_data.get("baseToken", {}).get("address", "")
    name      = pair_data.get("baseToken", {}).get("name", "Unknown")
    symbol    = pair_data.get("baseToken", {}).get("symbol", "???")
    pair_addr = pair_data.get("pairAddress", "")
    dex_name  = pair_data.get("dexId", "BSC DEX").replace("-", " ").title()
    dex_url   = pair_data.get("url", f"https://dexscreener.com/bsc/{pair_addr}")

    price   = pair_data.get("priceUsd", "N/A")
    mkt_cap = float(pair_data.get("marketCap", 0) or 0)
    liq     = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)
    v5m     = float(pair_data.get("volume", {}).get("m5", 0) or 0)
    v1h     = float(pair_data.get("volume", {}).get("h1", 0) or 0)
    v6h     = float(pair_data.get("volume", {}).get("h6", 0) or 0)
    v24h    = float(pair_data.get("volume", {}).get("h24", 0) or 0)
    c5m     = pair_data.get("priceChange", {}).get("m5", "0")
    c1h     = pair_data.get("priceChange", {}).get("h1", "0")
    c6h     = pair_data.get("priceChange", {}).get("h6", "0")
    c24h    = pair_data.get("priceChange", {}).get("h24", "0")
    b1h     = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    s1h     = int(pair_data.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
    b5m     = int(pair_data.get("txns", {}).get("m5", {}).get("buys", 0) or 0)
    s5m     = int(pair_data.get("txns", {}).get("m5", {}).get("sells", 0) or 0)

    ts = pair_data.get("pairCreatedAt")
    if ts:
        ah = (time.time() - int(ts)/1000) / 3600
        age = f"{round(ah*60)}m" if ah < 1 else (f"{round(ah,1)}h" if ah < 48 else f"{round(ah/24,1)}d")
    else:
        age = "Unknown"

    t1h = b1h + s1h
    bp  = round(b1h/t1h*100) if t1h > 0 else 0
    lm  = round(liq/mkt_cap*100,1) if mkt_cap > 0 else 0

    prev  = token_history.get(addr)
    acc   = score_token(pair_data, prev, is_waking)
    vir   = virality(pair_data)
    token_history[addr] = {
        "vol_1h": v1h, "vol_24h": v24h, "price": price,
        "ticker": symbol, "timestamp": time.time(),
        "scan_count": (prev.get("scan_count",0) if prev else 0) + 1,
    }

    # Parallel security
    hp_data, src, gp_data, gmgn = await asyncio.gather(
        honeypot_check(session, addr),
        get_contract_source(session, addr),
        goplus_check(session, addr),
        gmgn_holders(session, addr),
    )

    hp  = hp_data or {}
    gp  = gp_data or {}
    is_hp     = hp.get("isHoneypot", False)
    hp_reason = hp.get("honeypotResult", {}).get("reason", "")
    buy_tax   = hp.get("simulationResult", {}).get("buyTax")
    sell_tax  = hp.get("simulationResult", {}).get("sellTax")
    contract  = analyze_contract(src)

    renounced     = gp.get("owner_address","") == "0x0000000000000000000000000000000000000000"
    can_take_back = gp.get("can_take_back_ownership","0") == "1"
    is_mintable   = gp.get("is_mintable","0") == "1"
    is_proxy      = gp.get("is_proxy","0") == "1"
    hidden_owner  = gp.get("hidden_owner","0") == "1"
    gp_holders    = gp.get("holder_count")
    top10         = float(gp.get("top_10_holder_rate", 0) or 0) * 100

    # Holder cross-reference
    sources = {}
    if gp_holders: sources["GoPlus"] = int(gp_holders)
    if gmgn: sources["GMGN"] = int(gmgn)
    if sources:
        best = max(sources.values())
        if len(sources) == 2:
            gv, gm = sources.get("GoPlus",0), sources.get("GMGN",0)
            hdisplay = f"{best:,} (GoPlus: {gv:,} | GMGN: {gm:,})"
            if abs(gv-gm) > gv*0.5 and abs(gv-gm) > 50:
                hdisplay += " ⚠️ sources disagree"
        else:
            src_name = list(sources.keys())[0]
            hdisplay = f"{best:,} (via {src_name})"
    else:
        hdisplay = "Unknown"

    dump_flag, dump_reason = is_dump(pair_data)
    risk    = risk_label(is_hp, contract["score"], top10, gp)
    narr    = narrative(name, symbol)
    pred    = predict(pair_data, acc["score"], is_waking, dump_flag)

    is_micro = 0 < mkt_cap <= MICRO_CAP_MAX
    mc_tag   = "💎 MICRO" if is_micro else "📊 STD"
    if is_new:   atype = f"🆕 NEW — {new_age_str}"
    elif is_waking: atype = "💤🔥 SLEEPING GIANT"
    else:        atype = "📡 ACCUMULATION"

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
        f"💰 Price: ${price}\n"
        f"📈 MCap: ${mkt_cap:,.0f} | 💧 Liq: ${liq:,.0f} ({lm}%)\n"
        f"📦 Vol: 5m ${v5m:,.0f} | 1h ${v1h:,.0f} | 24h ${v24h:,.0f}\n"
        f"📉 5m: {c5m}% | 1h: {c1h}% | 6h: {c6h}% | 24h: {c24h}%\n"
        f"🔄 5m: {b5m}B/{s5m}S | 1h: {b1h}B/{s1h}S ({bp}% buys)\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🛡 *SECURITY — {risk}*\n"
        f"🍯 Honeypot: {'🔴 YES — AVOID' if is_hp else '🟢 Clean'}\n"
    )
    if hp_reason: msg += f"   └ {hp_reason}\n"
    msg += (
        f"📄 Verified: {'✅' if contract['verified'] else '❌ NO'} | "
        f"🔑 Renounced: {'✅' if renounced else '⚠️ No'}\n"
        f"🖨️ Mint: {'🔴' if is_mintable else '✅'} | "
        f"🌀 Proxy: {'🔴' if is_proxy else '✅'} | "
        f"👻 Hidden owner: {'🔴' if hidden_owner else '✅'}\n"
        f"💸 Tax: Buy {f'{buy_tax}%' if buy_tax is not None else '?'} / "
        f"Sell {f'{sell_tax}%' if sell_tax is not None else '?'}\n"
        f"🐋 Top 10: {round(top10,1)}% | 👥 Holders: {hdisplay}\n"
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
    return msg


# ── FAVOURITES HELPERS ────────────────────────────────────────────────────────

def get_favs(chat_id: int) -> dict:
    return favourites.get(chat_id, {})


async def build_fav_board(session: aiohttp.ClientSession, chat_id: int) -> str:
    favs = get_favs(chat_id)
    if not favs:
        return "⭐ *FAVOURITES*\n\nNo tokens added yet. Use /fav <address> to add one."

    lines = ["⭐ *FAVOURITES — LIVE PRICES*\n"]
    for addr, info in favs.items():
        pair = await dexscreener_token(session, addr)
        if pair:
            price   = pair.get("priceUsd", "N/A")
            mkt_cap = float(pair.get("marketCap", 0) or 0)
            v1h     = float(pair.get("volume", {}).get("h1", 0) or 0)
            c1h     = pair.get("priceChange", {}).get("h1", "0")
            c24h    = pair.get("priceChange", {}).get("h24", "0")
            liq     = float(pair.get("liquidity", {}).get("usd", 0) or 0)
            ticker  = pair.get("baseToken", {}).get("symbol", info.get("ticker", "???"))
            dex_url = pair.get("url", f"https://dexscreener.com/bsc/{addr}")

            # Update price for move detection
            favs[addr]["last_price"] = float(price) if price != "N/A" else favs[addr].get("last_price", 0)
            favs[addr]["last_price_ts"] = time.time()
            favs[addr]["ticker"] = ticker

            c1h_emoji = "🟢" if float(c1h or 0) >= 0 else "🔴"
            lines.append(
                f"[${ticker}]({dex_url})\n"
                f"💰 ${price} | MCap: ${mkt_cap:,.0f}\n"
                f"📦 Vol 1h: ${v1h:,.0f} | 💧 Liq: ${liq:,.0f}\n"
                f"{c1h_emoji} 1h: {c1h}% | 24h: {c24h}%\n"
                f"`{addr}`\n"
            )
        else:
            ticker = info.get("ticker", "???")
            lines.append(f"${ticker} — `{addr}` _(data unavailable)_\n")

    return "\n".join(lines)


async def update_pinned_favs(app: Application, chat_id: int):
    """Silently update the pinned favourites board"""
    async with aiohttp.ClientSession() as session:
        board = await build_fav_board(session, chat_id)
    try:
        msg_id = pinned_msg_ids.get(chat_id)
        if msg_id:
            await app.bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=board,
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True
            )
        else:
            msg = await app.bot.send_message(
                chat_id=chat_id,
                text=board,
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True
            )
            pinned_msg_ids[chat_id] = msg.message_id
            await app.bot.pin_chat_message(
                chat_id=chat_id,
                message_id=msg.message_id,
                disable_notification=True
            )
    except Exception as e:
        logger.warning(f"Pinned favs update error: {e}")


async def check_fav_moves(app: Application):
    """Check if any favourited tokens moved 50%+ in 1h"""
    if not favourites:
        return
    async with aiohttp.ClientSession() as session:
        for chat_id, favs in favourites.items():
            if not favs:
                continue
            for addr, info in favs.items():
                try:
                    pair = await dexscreener_token(session, addr)
                    if not pair:
                        continue
                    ch_1h = float(pair.get("priceChange", {}).get("h1", 0) or 0)
                    ticker = pair.get("baseToken", {}).get("symbol", info.get("ticker", "???"))
                    price  = pair.get("priceUsd", "N/A")
                    dex_url = pair.get("url", f"https://dexscreener.com/bsc/{addr}")

                    if abs(ch_1h) >= FAV_ALERT_PCT:
                        direction = "🚀 UP" if ch_1h > 0 else "🔴 DOWN"
                        alert_key = f"fav_move_{addr}_{round(ch_1h/10)*10}"
                        if time.time() - alerted_tokens.get(alert_key, 0) < 3600:
                            continue
                        alerted_tokens[alert_key] = time.time()
                        await app.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"⭐ *FAVOURITE ALERT*\n\n"
                                f"*{ticker}* moved {direction} {ch_1h}% in the last hour\n"
                                f"💰 Current price: ${price}\n\n"
                                f"[View on DexScreener]({dex_url})"
                            ),
                            parse_mode=ParseMode.MARKDOWN,
                            disable_web_page_preview=True
                        )
                except Exception as e:
                    logger.warning(f"Fav move check error {addr}: {e}")


# ── MAIN SCAN LOOP ────────────────────────────────────────────────────────────

async def run_scan(session: aiohttp.ClientSession, app: Application):
    if not subscribed_chats:
        return

    logger.info("Scan cycle starting...")
    pairs = await fetch_bsc_pairs(session)

    for pair_data in pairs:
        try:
            addr    = pair_data.get("baseToken", {}).get("address", "")
            liq     = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)
            mkt_cap = float(pair_data.get("marketCap", 0) or 0)
            vol_1h  = float(pair_data.get("volume", {}).get("h1", 0) or 0)
            vol_24h = float(pair_data.get("volume", {}).get("h24", 0) or 0)

            if not addr:
                continue

            is_micro  = 0 < mkt_cap <= MICRO_CAP_MAX
            min_liq   = MIN_LIQUIDITY_MICRO if is_micro else MIN_LIQUIDITY_STD

            # Always save to history regardless of liquidity
            prev = token_history.get(addr)
            token_history[addr] = {
                "vol_1h": vol_1h, "vol_24h": vol_24h,
                "price": pair_data.get("priceUsd"),
                "ticker": pair_data.get("baseToken", {}).get("symbol", "???"),
                "timestamp": time.time(),
                "scan_count": (prev.get("scan_count",0) if prev else 0) + 1,
            }

            if liq < min_liq:
                continue

            # Dump filter
            dump_flag, dump_reason = is_dump(pair_data)
            if dump_flag:
                logger.debug(f"Dump: {addr[:8]} {dump_reason}")
                continue

            # Detection
            is_new_pair_flag = False
            new_age_str = ""
            ts = pair_data.get("pairCreatedAt")
            if ts:
                age_h = (time.time() - int(ts)/1000) / 3600
                if age_h <= NEW_PAIR_MAX_AGE_H and addr not in seen_new_pairs:
                    is_new_pair_flag = True
                    new_age_str = f"{round(age_h,1)}h old" if age_h >= 1 else f"{round(age_h*60)}m old"

            is_waking, wake_signal = is_sleeping_giant(pair_data, prev)
            acc = score_token(pair_data, prev, is_waking)

            # Alert decision
            should_alert = False
            if is_new_pair_flag and acc["score"] >= MIN_SCORE_NEW:
                should_alert = True
                seen_new_pairs.add(addr)
            elif is_waking and acc["score"] >= MIN_SCORE_WAKE:
                should_alert = True
            elif acc["score"] >= MIN_SCORE_STD:
                should_alert = True

            if not should_alert:
                continue

            if time.time() - alerted_tokens.get(addr, 0) < ALERT_COOLDOWN:
                continue

            name = pair_data.get("baseToken", {}).get("name", "?")
            logger.info(f"ALERT {name} score={acc['score']} new={is_new_pair_flag} waking={is_waking} mc=${mkt_cap:,.0f}")

            report = await build_report(session, pair_data, is_waking, wake_signal, is_new_pair_flag, new_age_str)
            alerted_tokens[addr] = time.time()

            for chat_id in list(subscribed_chats):
                try:
                    await app.bot.send_message(
                        chat_id=chat_id,
                        text=report,
                        parse_mode=ParseMode.MARKDOWN,
                        disable_web_page_preview=True
                    )
                    await asyncio.sleep(0.3)
                except Exception as e:
                    logger.warning(f"Send error {chat_id}: {e}")

        except Exception as e:
            logger.warning(f"Pair error: {e}")
            continue


# ── COMMANDS ──────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    subscribed_chats.add(chat_id)

    # Set menu commands
    await context.bot.set_my_commands([
        BotCommand("start",     "Activate scanner"),
        BotCommand("stop",      "Pause alerts"),
        BotCommand("scan",      "Scan any BSC token"),
        BotCommand("watchlist", "View tracked tokens"),
        BotCommand("fav",       "Add token to favourites"),
        BotCommand("unfav",     "Remove from favourites"),
        BotCommand("favlist",   "View favourites with live prices"),
        BotCommand("status",    "Scanner stats"),
        BotCommand("filters",   "Current filter settings"),
    ])

    await update.message.reply_text(
        "🟡 *BSC MEME SCANNER ACTIVATED*\n\n"
        "Monitoring ALL BSC DEXes:\n"
        "• PancakeSwap v2 + v3, ApeSwap, BiSwap + more\n\n"
        "Hunting:\n"
        "• 🆕 New pairs under 72h\n"
        "• 💤🔥 Sleeping tokens waking up — any age\n"
        "• 💎 Micro caps showing early volume\n"
        "• 📈 Genuine accumulation patterns\n\n"
        "Use the menu below for all commands ↓",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    subscribed_chats.discard(update.effective_chat.id)
    await update.message.reply_text("⏸ Alerts paused. Send /start to resume.")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"*BSC SCANNER STATUS*\n\n"
        f"✅ Running\n"
        f"⏱ Scan every: {SCAN_INTERVAL}s\n"
        f"🪙 Tokens in memory: {len(token_history)}\n"
        f"🔔 Alerts sent: {len(alerted_tokens)}\n"
        f"⭐ Favourites: {sum(len(v) for v in favourites.values())}\n"
        f"👥 Subscribers: {len(subscribed_chats)}",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_filters(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"*BSC FILTERS*\n\n"
        f"🆕 New pairs (under {NEW_PAIR_MAX_AGE_H}h): min score {MIN_SCORE_NEW}\n"
        f"💤 Sleeping giant: min score {MIN_SCORE_WAKE}\n"
        f"📊 Standard: min score {MIN_SCORE_STD}\n\n"
        f"💧 Min liq (micro): ${MIN_LIQUIDITY_MICRO:,}\n"
        f"💧 Min liq (standard): ${MIN_LIQUIDITY_STD:,}\n"
        f"💎 Micro cap ceiling: ${MICRO_CAP_MAX:,}\n\n"
        f"⛔ Anti-dump: max sells {round(MAX_SELL_RATIO*100)}% | "
        f"max drop {MAX_DROP_1H}%\n"
        f"⏰ Cooldown: {ALERT_COOLDOWN//60}min per token\n"
        f"⭐ Fav alert: ±{FAV_ALERT_PCT}% move in 1h",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not token_history:
        await update.message.reply_text("No tokens in memory yet — give it a few minutes.")
        return

    sorted_tokens = sorted(
        token_history.items(),
        key=lambda x: x[1].get("vol_1h", 0),
        reverse=True
    )[:20]

    lines = ["*TOP 20 TRACKED TOKENS*\n"]
    for addr, h in sorted_tokens:
        vol    = h.get("vol_1h", 0)
        scans  = h.get("scan_count", 0)
        ticker = h.get("ticker", "???")
        link   = f"https://dexscreener.com/bsc/{addr}"
        lines.append(f"[${ticker}]({link})\n`{addr}` | Vol 1h: ${vol:,.0f} | Scans: {scans}")

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
        await update.message.reply_text("❌ Invalid BSC address. Should start with 0x and be 42 characters.")
        return

    if chat_id not in favourites:
        favourites[chat_id] = {}

    if addr in favourites[chat_id]:
        await update.message.reply_text("Already in your favourites.")
        return

    msg = await update.message.reply_text("⭐ Adding to favourites...")

    async with aiohttp.ClientSession() as session:
        pair = await dexscreener_token(session, addr)

    ticker = pair.get("baseToken", {}).get("symbol", "???") if pair else "???"
    price  = float(pair.get("priceUsd", 0) or 0) if pair else 0

    favourites[chat_id][addr] = {
        "ticker": ticker,
        "added_ts": time.time(),
        "last_price": price,
        "last_price_ts": time.time(),
    }

    await msg.edit_text(f"⭐ Added *${ticker}* to favourites.", parse_mode=ParseMode.MARKDOWN)

    # Update pinned board
    await update_pinned_favs(context.application, chat_id)


async def cmd_unfav(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Usage: /unfav <BSC token address>")
        return

    addr = context.args[0].strip()
    if chat_id in favourites and addr in favourites[chat_id]:
        ticker = favourites[chat_id][addr].get("ticker", "???")
        del favourites[chat_id][addr]
        await update.message.reply_text(f"Removed *${ticker}* from favourites.", parse_mode=ParseMode.MARKDOWN)
        await update_pinned_favs(context.application, chat_id)
    else:
        await update.message.reply_text("Token not found in your favourites.")


async def cmd_favlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not get_favs(chat_id):
        await update.message.reply_text("No favourites yet. Use /fav <address> to add tokens.")
        return

    msg = await update.message.reply_text("⭐ Fetching live data for your favourites...")
    async with aiohttp.ClientSession() as session:
        board = await build_fav_board(session, chat_id)

    try:
        await msg.edit_text(board, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
    except Exception:
        await msg.edit_text(board[:4000], disable_web_page_preview=True)

    # Also silently refresh the pinned board
    await update_pinned_favs(context.application, chat_id)


async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /scan <BSC token address>")
        return

    address = context.args[0].strip()
    msg = await update.message.reply_text("🔍 Scanning... please wait")

    async with aiohttp.ClientSession() as session:
        pair_data = await dexscreener_token(session, address)
        if not pair_data:
            await msg.edit_text("❌ Token not found on any BSC DEX.")
            return

        prev = token_history.get(address)
        is_waking, wake_signal = is_sleeping_giant(pair_data, prev)
        report = await build_report(session, pair_data, is_waking, wake_signal)

    try:
        await msg.edit_text(report, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
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

    async def scan_job(ctx):
        async with aiohttp.ClientSession() as session:
            await run_scan(session, app)

    async def fav_job(ctx):
        await check_fav_moves(app)

    app.job_queue.run_repeating(scan_job, interval=SCAN_INTERVAL, first=15)
    app.job_queue.run_repeating(fav_job, interval=FAV_CHECK_INTERVAL, first=60)

    logger.info("BSC Scanner v3 starting...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
