"""
BSC MEME SCANNER BOT - PancakeSwap + All Major BSC DEXes
- Tracks ALL tokens regardless of age
- Detects sleeping tokens waking up (low volume suddenly spiking)
- Multi-DEX coverage: PancakeSwap v2/v3, ApeSwap, BiSwap, BabySwap, MDEX
- Full security suite: Honeypot, GoPlus, Contract analysis
- Anti-dump filtering
- Low MC + early volume detection ($5k-$50k volume trigger)
"""

import asyncio
import logging
import os
import time
from typing import Optional

import aiohttp
from telegram import Update
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

BSCSCAN_URL = "https://api.bscscan.com/api"

# ── BSC DEX LIST ──────────────────────────────────────────────────────────────
# All major BSC DEXes monitored
BSC_DEXES = [
    "pancakeswap",
    "pancakeswap-v3",
    "apeswap",
    "biswap",
    "babyswap",
    "mdex",
    "knightswap",
    "thena",
]

# ── THRESHOLDS ────────────────────────────────────────────────────────────────

# Sleeping token wake-up detection
WAKE_MIN_VOLUME_USD      = 5_000    # min $5k volume to consider a wake-up signal
WAKE_MAX_VOLUME_USD      = 50_000   # alert zone: $5k-$50k volume = early stage
WAKE_VOLUME_SPIKE_MULT   = 3.0      # volume must be 3x the previous scan's volume
WAKE_PREV_MAX_VOL        = 10_000   # previous volume must have been under $10k (was sleeping)

# Micro cap detection
MICRO_CAP_MAX            = 100_000  # under $100k mcap = micro cap
MICRO_MIN_LIQUIDITY      = 2_000    # $2k min liquidity for micro caps

# Standard tokens
STD_MIN_LIQUIDITY        = 8_000    # $8k min liquidity

# Score thresholds
MIN_SCORE_WAKE           = 60       # sleeping token waking up — lower bar, signal itself is strong
MIN_SCORE_STANDARD       = 65       # standard accumulation alert

# Anti-dump
MAX_SELL_RATIO_1H        = 0.65
MIN_LIQ_TO_MCAP          = 0.08
MAX_PRICE_DROP_1H        = -25

# New pair detection
NEW_PAIR_MAX_AGE_HOURS   = 48       # pairs under 48h old = new pair
NEW_PAIR_MIN_LIQUIDITY   = 3_000    # $3k min liquidity for new pairs
NEW_PAIR_MIN_SCORE       = 60       # score threshold for new pair alerts

# Cooldowns
ALERT_COOLDOWN           = 5400     # 90 min cooldown per token
SCAN_INTERVAL            = 75       # scan every 75 seconds

# Tracking
alerted_tokens   = {}   # addr -> last alert timestamp
token_history    = {}   # addr -> {vol_1h, vol_24h, price, holders, timestamp, scan_count}
subscribed_chats = set()
seen_new_pairs   = set()  # track pairs we've already seen as "new"


# ── DEX SCREENER HELPERS ──────────────────────────────────────────────────────

async def dex_get(session: aiohttp.ClientSession, url: str) -> Optional[dict]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status == 200:
                return await r.json()
    except Exception as e:
        logger.warning(f"Fetch error: {e}")
    return None


async def dexscreener_token(session: aiohttp.ClientSession, address: str) -> Optional[dict]:
    """Get best BSC pair for a token address"""
    data = await dex_get(session, f"https://api.dexscreener.com/latest/dex/tokens/{address}")
    if not data:
        return None
    pairs = [p for p in data.get("pairs", []) if p.get("chainId") == "bsc"]
    if pairs:
        return max(pairs, key=lambda x: float(x.get("liquidity", {}).get("usd", 0) or 0))
    return None


async def fetch_all_bsc_pairs(session: aiohttp.ClientSession) -> list:
    """
    Multi-strategy BSC pair collection.
    Pulls from ALL major DEXes and multiple endpoints to ensure
    no token is missed regardless of age.
    """
    results = []
    seen_addrs = set()

    def add_pairs(pairs: list):
        for p in pairs:
            addr = p.get("baseToken", {}).get("address", "")
            if addr and addr not in seen_addrs:
                seen_addrs.add(addr)
                results.append(p)

    # Strategy 1: Direct BSC pairs (all DEXes, sorted by activity)
    try:
        data = await dex_get(session, "https://api.dexscreener.com/latest/dex/pairs/bsc")
        if data and "pairs" in data:
            add_pairs(data["pairs"][:60])
    except Exception as e:
        logger.warning(f"Direct BSC pairs error: {e}")

    # Strategy 2: Per-DEX searches to catch tokens not in top pairs
    for dex in BSC_DEXES:
        try:
            data = await dex_get(session, f"https://api.dexscreener.com/latest/dex/search?q={dex}")
            if data and "pairs" in data:
                bsc = [p for p in data["pairs"] if p.get("chainId") == "bsc"]
                add_pairs(bsc[:20])
        except Exception as e:
            logger.warning(f"DEX search error {dex}: {e}")

    # Strategy 3: Token profiles on BSC (community-listed tokens)
    try:
        data = await dex_get(session, "https://api.dexscreener.com/token-profiles/latest/v1")
        if data and isinstance(data, list):
            bsc_profiles = [p for p in data if p.get("chainId") == "bsc"]
            for p in bsc_profiles[:25]:
                addr = p.get("tokenAddress")
                if addr and addr not in seen_addrs:
                    pair = await dexscreener_token(session, addr)
                    if pair:
                        seen_addrs.add(addr)
                        results.append(pair)
    except Exception as e:
        logger.warning(f"Profiles error: {e}")

    # Strategy 4: Active boosts on BSC (community paid to promote = interest signal)
    try:
        data = await dex_get(session, "https://api.dexscreener.com/token-boosts/top/v1")
        if data and isinstance(data, list):
            bsc_boosted = [p for p in data if p.get("chainId") == "bsc"]
            for p in bsc_boosted[:15]:
                addr = p.get("tokenAddress")
                if addr and addr not in seen_addrs:
                    pair = await dexscreener_token(session, addr)
                    if pair:
                        seen_addrs.add(addr)
                        results.append(pair)
    except Exception as e:
        logger.warning(f"Boosts error: {e}")

    # Strategy 5: Re-scan all known tokens in history
    # This is the key fix — ensures old tokens are never missed
    known_addrs = list(token_history.keys())
    logger.info(f"Re-scanning {len(known_addrs)} known tokens from history")
    for addr in known_addrs:
        if addr not in seen_addrs:
            pair = await dexscreener_token(session, addr)
            if pair:
                seen_addrs.add(addr)
                results.append(pair)
            await asyncio.sleep(0.1)  # small delay to avoid rate limiting

    # Strategy 6: Volume spike discovery — scans ALL BSC pairs sorted by
    # trending score on 1h timeframe. Key fix for sleeping giants that never
    # appear in profile/boost feeds.
    try:
        for page in range(1, 4):
            data = await dex_get(
                session,
                f"https://api.dexscreener.com/latest/dex/search?q=pancakeswap&rankBy=trendingScoreH1&order=desc&page={page}"
            )
            if data and "pairs" in data:
                bsc = [p for p in data["pairs"] if p.get("chainId") == "bsc"]
                add_pairs(bsc)
            await asyncio.sleep(0.2)
    except Exception as e:
        logger.warning(f"Volume spike discovery error: {e}")

    # Strategy 7: PancakeSwap pairs sorted by 1h volume
    try:
        data = await dex_get(session, "https://api.dexscreener.com/latest/dex/pairs/bsc/pancakeswap-v2")
        if data and "pairs" in data:
            pairs_sorted = sorted(
                data["pairs"],
                key=lambda x: float(x.get("volume", {}).get("h1", 0) or 0),
                reverse=True
            )
            add_pairs(pairs_sorted[:80])
    except Exception as e:
        logger.warning(f"PancakeSwap v2 direct fetch error: {e}")

    logger.info(f"Total BSC pairs collected: {len(results)}")
    return results


# ── GMGN HOLDER CHECK ─────────────────────────────────────────────────────────

async def gmgn_holders(session: aiohttp.ClientSession, address: str):
    try:
        url = f"https://gmgn.ai/defi/quotation/v1/tokens/bsc/{address}"
        headers = {"User-Agent": "Mozilla/5.0"}
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                data = await r.json()
                token_data = data.get("data", {}).get("token", {})
                holders = token_data.get("holder_count") or token_data.get("holders")
                if holders:
                    return int(holders)
    except Exception as e:
        logger.warning(f"GMGN holder fetch error: {e}")
    return None


# ── NEW PAIR DETECTION ────────────────────────────────────────────────────────

def is_new_pair(pair_data: dict) -> tuple:
    """
    Returns (is_new, age_str)
    Flags pairs launched in the last 48 hours with strong early signals
    """
    created_ts = pair_data.get("pairCreatedAt")
    if not created_ts:
        return False, ""

    age_hrs = (time.time() - int(created_ts) / 1000) / 3600

    if age_hrs > NEW_PAIR_MAX_AGE_HOURS:
        return False, ""

    if age_hrs < 1:
        age_str = f"{round(age_hrs * 60)}m old"
    elif age_hrs < 24:
        age_str = f"{round(age_hrs, 1)}h old"
    else:
        age_str = f"{round(age_hrs/24, 1)} days old"

    return True, age_str


# ── SECURITY ──────────────────────────────────────────────────────────────────

async def honeypot_check(session: aiohttp.ClientSession, address: str) -> dict:
    try:
        url = f"https://api.honeypot.is/v2/IsHoneypot?address={address}&chainID=56"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                return await r.json()
    except Exception as e:
        logger.warning(f"Honeypot error: {e}")
    return {}


async def goplus_check(session: aiohttp.ClientSession, address: str) -> dict:
    """GoPlus — gold standard BSC token security, no API key needed"""
    try:
        url = f"https://api.gopluslabs.io/api/v1/token_security/56?contract_addresses={address}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status == 200:
                data = await r.json()
                result = data.get("result", {})
                if result:
                    return list(result.values())[0]
    except Exception as e:
        logger.warning(f"GoPlus error: {e}")
    return {}


async def get_contract_source(session: aiohttp.ClientSession, address: str) -> Optional[str]:
    params = {
        "module": "contract",
        "action": "getsourcecode",
        "address": address,
        "apikey": ETHERSCAN_KEY
    }
    try:
        async with session.get(BSCSCAN_URL, params=params, timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status == 200:
                data = await r.json()
                if data.get("status") == "1" and data.get("result"):
                    return data["result"][0].get("SourceCode", "")
    except Exception as e:
        logger.warning(f"Contract source error: {e}")
    return None


def analyze_contract(source: Optional[str]) -> dict:
    if not source or source.strip() == "":
        return {"verified": False, "flags": ["❌ Contract NOT verified on BSCScan"], "score": 20}

    flags = []
    score = 100
    checks = {
        "mint(":        ("⚠️ Mint function — supply can be inflated", 25),
        "blacklist":    ("⚠️ Blacklist — wallets can be blocked", 20),
        "setfee":       ("⚠️ Dynamic fees — tax can be changed anytime", 15),
        "pause()":      ("⚠️ Pause function — trading can be halted", 20),
        "selfdestruct": ("🔴 Selfdestruct — contract can be destroyed", 35),
        "delegatecall": ("🔴 Delegatecall — dangerous proxy pattern", 25),
        "maxwallet":    ("⚠️ Max wallet size enforced", 5),
        "maxtx":        ("⚠️ Max transaction size enforced", 5),
        "cooldown":     ("⚠️ Trading cooldown present", 5),
    }
    src = source.lower()
    for pattern, (msg, penalty) in checks.items():
        if pattern in src:
            flags.append(msg)
            score -= penalty

    return {"verified": True, "flags": flags, "score": max(0, score)}


# ── ANTI-DUMP FILTER ──────────────────────────────────────────────────────────

def is_dump_pattern(pair_data: dict) -> tuple:
    vol_1h   = float(pair_data.get("volume", {}).get("h1", 0) or 0)
    ch_1h    = float(pair_data.get("priceChange", {}).get("h1", 0) or 0)
    ch_6h    = float(pair_data.get("priceChange", {}).get("h6", 0) or 0)
    liq      = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)
    mkt_cap  = float(pair_data.get("marketCap", 0) or 0)
    buys_1h  = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    sells_1h = int(pair_data.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
    buys_5m  = int(pair_data.get("txns", {}).get("m5", {}).get("buys", 0) or 0)
    sells_5m = int(pair_data.get("txns", {}).get("m5", {}).get("sells", 0) or 0)
    vol_5m   = float(pair_data.get("volume", {}).get("m5", 0) or 0)
    total_1h = buys_1h + sells_1h
    total_5m = buys_5m + sells_5m

    # Dominant selling
    if total_1h > 8 and sells_1h / total_1h > MAX_SELL_RATIO_1H:
        return True, f"Sell pressure dominant — {round(sells_1h/total_1h*100)}% sells in 1h"

    # Price collapsing while volume spikes
    if vol_1h > 3000 and ch_1h < MAX_PRICE_DROP_1H:
        return True, f"Price falling {ch_1h}% while volume spikes — sell-off in progress"

    # Rug-thin liquidity
    if mkt_cap > 0 and liq > 0 and liq / mkt_cap < MIN_LIQ_TO_MCAP:
        return True, f"Liquidity only {round(liq/mkt_cap*100, 1)}% of mcap — easy rug"

    # Wash trading
    if liq > 0 and vol_1h > liq * 12:
        return True, f"Volume {round(vol_1h/liq)}x liquidity — likely wash trading"

    # Already pumped massively and reversing
    if ch_6h > 400 and ch_1h < -15:
        return True, f"Already up {ch_6h}% in 6h and now reversing — late entry trap"

    # 5m collapse
    if total_5m > 5 and sells_5m / total_5m > 0.75 and ch_1h < -10:
        return True, f"5m candle dumping — {round(sells_5m/total_5m*100)}% sells"

    return False, ""


# ── SLEEPING TOKEN DETECTION ──────────────────────────────────────────────────

def is_sleeping_giant(pair_data: dict, prev: Optional[dict]) -> tuple:
    """
    Core feature: detect tokens that have been quiet for a while
    and are now starting to show volume activity.
    Returns (is_waking_up, signal_description)
    """
    vol_1h  = float(pair_data.get("volume", {}).get("h1", 0) or 0)
    vol_24h = float(pair_data.get("volume", {}).get("h24", 0) or 0)
    vol_6h  = float(pair_data.get("volume", {}).get("h6", 0) or 0)

    if not prev:
        # First time seeing this token — just store, don't alert yet
        return False, ""

    prev_vol_1h  = prev.get("vol_1h", 0)
    prev_vol_24h = prev.get("vol_24h", 0)
    scan_count   = prev.get("scan_count", 0)

    # Need at least 2 scans of history to detect the change
    if scan_count < 2:
        return False, ""

    # Was it sleeping? Previous 1h volume was very low
    was_sleeping = prev_vol_1h < WAKE_PREV_MAX_VOL

    if not was_sleeping:
        return False, ""

    # Is it now waking up?
    if vol_1h < WAKE_MIN_VOLUME_USD:
        return False, ""

    # Check the spike
    if prev_vol_1h == 0 and vol_1h >= WAKE_MIN_VOLUME_USD:
        return True, f"💤➡🔥 SLEEPING GIANT WAKING UP — went from $0 to ${vol_1h:,.0f} volume in 1h"

    if prev_vol_1h > 0:
        spike = vol_1h / prev_vol_1h
        if spike >= WAKE_VOLUME_SPIKE_MULT:
            return True, f"💤➡🔥 SLEEPING GIANT — volume {round(spike,1)}x spike (${prev_vol_1h:,.0f} → ${vol_1h:,.0f})"

    return False, ""


# ── ACCUMULATION SCORE ────────────────────────────────────────────────────────

def accumulation_score(pair_data: dict, prev: Optional[dict], is_waking: bool) -> dict:
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
        liq      = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)
        mkt_cap  = float(pair_data.get("marketCap", 0) or 0)
        buys_1h  = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
        sells_1h = int(pair_data.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
        buys_5m  = int(pair_data.get("txns", {}).get("m5", {}).get("buys", 0) or 0)

        # Sleeping giant bonus — biggest possible signal
        if is_waking:
            score += 30
            signals.append("🚨 SLEEPING GIANT WAKING UP — volume appearing after silence")

        # Volume momentum vs 6h average
        if vol_1h > 0 and vol_6h > 0:
            avg_6h = vol_6h / 6
            if avg_6h > 0:
                mult = vol_1h / avg_6h
                if mult >= 4:
                    score += 22
                    signals.append(f"📈 Volume {round(mult,1)}x the 6h hourly average")
                elif mult >= 2.5:
                    score += 15
                    signals.append(f"📈 Volume {round(mult,1)}x the 6h average")
                elif mult >= 1.5:
                    score += 8
                    signals.append(f"📈 Volume picking up — {round(mult,1)}x avg")

        # Buy pressure
        total_1h = buys_1h + sells_1h
        if total_1h > 5:
            buy_pct = buys_1h / total_1h
            if buy_pct >= 0.75:
                score += 20
                signals.append(f"🟢 Strong buy pressure — {round(buy_pct*100)}% buys in 1h")
            elif buy_pct >= 0.62:
                score += 12
                signals.append(f"🟢 Good buy pressure — {round(buy_pct*100)}% buys in 1h")

        # Multi-timeframe alignment
        tfs_up = sum([ch_5m > 0, ch_1h > 0, ch_6h > 0])
        if tfs_up == 3:
            score += 15
            signals.append("🟢 Price trending up across all timeframes")
        elif tfs_up == 2:
            score += 7
            signals.append("🟡 Uptrend on 2/3 timeframes")

        # Vol/MCap ratio
        if mkt_cap > 0 and vol_24h > 0:
            ratio = vol_24h / mkt_cap
            if ratio > 1.0:
                score += 15
                signals.append(f"🔥 Vol/MCap: {round(ratio*100)}% — massive activity for this size")
            elif ratio > 0.5:
                score += 8
                signals.append(f"📊 Vol/MCap: {round(ratio*100)}%")

        # Burst buying in 5m
        if buys_5m >= 10:
            score += 12
            signals.append(f"⚡ {buys_5m} buys in last 5 minutes")
        elif buys_5m >= 5:
            score += 6
            signals.append(f"⚡ {buys_5m} buys in 5 mins")

        # Early volume in the wake-up zone ($5k-$50k)
        if WAKE_MIN_VOLUME_USD <= vol_1h <= WAKE_MAX_VOLUME_USD:
            score += 10
            signals.append(f"💎 Early volume zone — ${vol_1h:,.0f}/hr (catch it before it runs)")

        # Micro cap bonus
        if 0 < mkt_cap <= MICRO_CAP_MAX:
            score += 8
            signals.append(f"💎 Micro cap ${mkt_cap:,.0f} — room to multiply")

    except Exception as e:
        logger.warning(f"Score error: {e}")

    return {"score": min(100, score), "signals": signals}


# ── VIRALITY ──────────────────────────────────────────────────────────────────

def virality_score(pair_data: dict) -> dict:
    score = 0
    signals = []

    buys_5m = int(pair_data.get("txns", {}).get("m5", {}).get("buys", 0) or 0)
    buys_1h = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    vol_5m  = float(pair_data.get("volume", {}).get("m5", 0) or 0)
    boosted = pair_data.get("boosts", {}).get("active", 0) or 0

    if buys_5m >= 20:
        score += 30; signals.append(f"🔥 {buys_5m} buys in 5 mins — going viral")
    elif buys_5m >= 10:
        score += 18; signals.append(f"⚡ {buys_5m} buys in 5 mins")
    elif buys_5m >= 5:
        score += 8

    if buys_1h >= 150:
        score += 30; signals.append(f"📣 {buys_1h} buys in 1h — strong community")
    elif buys_1h >= 80:
        score += 18; signals.append(f"📣 {buys_1h} buys in 1h")
    elif buys_1h >= 30:
        score += 9; signals.append(f"📣 {buys_1h} buys in 1h")

    if vol_5m > 20000:
        score += 25; signals.append(f"💸 ${vol_5m:,.0f} volume in 5 mins")
    elif vol_5m > 8000:
        score += 12; signals.append(f"💸 ${vol_5m:,.0f} in 5 mins")

    if boosted:
        score += 25; signals.append(f"🚀 DexScreener boosted ({boosted} boosts)")

    return {"score": min(100, score), "signals": signals}


# ── NARRATIVE ─────────────────────────────────────────────────────────────────

NARRATIVES = {
    "🤖 AI":        ["ai", "agent", "gpt", "llm", "neural", "deepseek", "openai"],
    "🐸 Meme":      ["pepe", "doge", "shib", "inu", "cat", "frog", "moon", "wojak", "chad", "based", "bonk"],
    "🏛️ Political": ["trump", "maga", "elon", "biden", "vote", "president"],
    "🎮 Gaming":    ["game", "play", "nft", "metaverse", "quest", "guild"],
    "💰 DeFi":      ["defi", "yield", "farm", "stake", "swap", "liquidity"],
    "🌍 RWA":       ["rwa", "gold", "property", "asset"],
    "🐂 BSC Native":["bsc", "bnb", "binance", "pancake", "cake"],
}

def detect_narrative(name: str, symbol: str) -> list:
    text = f"{name} {symbol}".lower()
    return [label for label, kws in NARRATIVES.items() if any(k in text for k in kws)]


# ── RISK RATING ───────────────────────────────────────────────────────────────

def risk_rating(is_honeypot: bool, contract_score: int, top10_pct: float, gp: dict) -> str:
    if is_honeypot:
        return "🔴 CRITICAL — HONEYPOT DETECTED"
    can_take_back = gp.get("can_take_back_ownership", "0") == "1"
    is_proxy      = gp.get("is_proxy", "0") == "1"
    owner_addr    = gp.get("owner_address", "")
    renounced     = owner_addr == "0x0000000000000000000000000000000000000000"
    hidden_owner  = gp.get("hidden_owner", "0") == "1"

    if contract_score < 40 or top10_pct > 85 or is_proxy or can_take_back or hidden_owner:
        return "🔴 HIGH RISK"
    elif contract_score < 65 or top10_pct > 65:
        return "🟡 MEDIUM RISK"
    elif renounced and contract_score >= 75 and top10_pct < 50:
        return "🟢 LOW RISK ✓"
    return "🟡 MEDIUM RISK"


# ── PREDICTION ────────────────────────────────────────────────────────────────

def price_prediction(pair_data: dict, acc_score: int, is_waking: bool, dump: bool) -> str:
    if dump:
        return "🔴 DUMP PATTERN — do not enter"
    ch_1h = float(pair_data.get("priceChange", {}).get("h1", 0) or 0)
    ch_6h = float(pair_data.get("priceChange", {}).get("h6", 0) or 0)
    liq   = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)

    if is_waking and acc_score >= 70:
        return "🚀 HIGH POTENTIAL — sleeping token waking up, early entry opportunity"
    elif is_waking and acc_score >= 55:
        return "📈 WATCH CLOSELY — accumulation starting, confirm with next candle"
    elif acc_score >= 80 and ch_1h > 0 and ch_6h > 0:
        return "🚀 STRONG BULLISH — high probability continuation"
    elif acc_score >= 65 and ch_1h > 0:
        return "📈 BULLISH — accumulation pattern, monitor closely"
    elif acc_score >= 50:
        return "🟡 EARLY SIGNAL — needs confirmation"
    return "⚪ NEUTRAL — insufficient momentum"


# ── FULL REPORT ───────────────────────────────────────────────────────────────

async def build_report(
    session: aiohttp.ClientSession,
    pair_data: dict,
    is_waking: bool,
    wake_signal: str,
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
    vol_5m  = float(pair_data.get("volume", {}).get("m5", 0) or 0)
    vol_1h  = float(pair_data.get("volume", {}).get("h1", 0) or 0)
    vol_6h  = float(pair_data.get("volume", {}).get("h6", 0) or 0)
    vol_24h = float(pair_data.get("volume", {}).get("h24", 0) or 0)
    ch_5m   = pair_data.get("priceChange", {}).get("m5", "0")
    ch_1h   = pair_data.get("priceChange", {}).get("h1", "0")
    ch_6h   = pair_data.get("priceChange", {}).get("h6", "0")
    ch_24h  = pair_data.get("priceChange", {}).get("h24", "0")
    b1h     = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    s1h     = int(pair_data.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
    b5m     = int(pair_data.get("txns", {}).get("m5", {}).get("buys", 0) or 0)
    s5m     = int(pair_data.get("txns", {}).get("m5", {}).get("sells", 0) or 0)

    created_ts = pair_data.get("pairCreatedAt")
    if created_ts:
        age_hrs = (time.time() - int(created_ts) / 1000) / 3600
        if age_hrs < 1:
            age_str = f"{round(age_hrs*60)}m"
        elif age_hrs < 48:
            age_str = f"{round(age_hrs, 1)}h"
        else:
            age_str = f"{round(age_hrs/24, 1)} days"
    else:
        age_str = "Unknown"

    total_1h = b1h + s1h
    buy_pct  = round(b1h / total_1h * 100) if total_1h > 0 else 0
    liq_mc   = round(liq / mkt_cap * 100, 1) if mkt_cap > 0 else 0

    # Parallel security checks
    prev = token_history.get(addr)
    acc  = accumulation_score(pair_data, prev, is_waking)
    viral = virality_score(pair_data)

    hp_data, source, gp_data, gmgn_count = await asyncio.gather(
        honeypot_check(session, addr),
        get_contract_source(session, addr),
        goplus_check(session, addr),
        gmgn_holders(session, addr),
    )

    hp           = hp_data or {}
    gp           = gp_data or {}
    is_honeypot  = hp.get("isHoneypot", False)
    hp_reason    = hp.get("honeypotResult", {}).get("reason", "")
    buy_tax_hp   = hp.get("simulationResult", {}).get("buyTax")
    sell_tax_hp  = hp.get("simulationResult", {}).get("sellTax")
    contract     = analyze_contract(source)

    owner_addr    = gp.get("owner_address", "Unknown")
    renounced     = owner_addr == "0x0000000000000000000000000000000000000000"
    can_take_back = gp.get("can_take_back_ownership", "0") == "1"
    is_mintable   = gp.get("is_mintable", "0") == "1"
    is_proxy      = gp.get("is_proxy", "0") == "1"
    hidden_owner  = gp.get("hidden_owner", "0") == "1"
    gp_holder_count = gp.get("holder_count")
    top10_pct     = float(gp.get("top_10_holder_rate", 0) or 0) * 100

    # Cross-reference holder count from GMGN vs GoPlus
    # Use whichever is higher — DexScreener often undercounts
    holder_sources = {}
    if gp_holder_count:
        holder_sources["GoPlus"] = int(gp_holder_count)
    if gmgn_count:
        holder_sources["GMGN"] = int(gmgn_count)

    if holder_sources:
        best_holder_count = max(holder_sources.values())
        if len(holder_sources) == 2:
            gp_val = holder_sources.get("GoPlus", 0)
            gm_val = holder_sources.get("GMGN", 0)
            discrepancy = abs(gp_val - gm_val)
            holder_display = f"{best_holder_count:,} (GoPlus: {gp_val:,} | GMGN: {gm_val:,})"
            if discrepancy > gp_val * 0.5 and discrepancy > 50:
                holder_display += " ⚠️ Sources disagree"
        else:
            src = list(holder_sources.keys())[0]
            holder_display = f"{best_holder_count:,} (via {src})"
    else:
        holder_display = "Unknown"

    dump_flag, dump_reason = is_dump_pattern(pair_data)
    risk         = risk_rating(is_honeypot, contract["score"], top10_pct, gp)
    narratives   = detect_narrative(name, symbol)
    narrative_str = " | ".join(narratives) if narratives else "None detected"
    prediction   = price_prediction(pair_data, acc["score"], is_waking, dump_flag)

    is_micro = 0 < mkt_cap <= MICRO_CAP_MAX
    mc_tag   = "💎 MICRO CAP" if is_micro else "📊 STANDARD"
    if is_new:
        alert_type = f"🆕 NEW PAIR — {new_age_str}"
    elif is_waking:
        alert_type = "💤🔥 SLEEPING GIANT"
    else:
        alert_type = "📡 ACCUMULATION ALERT"

    contract_flags = "\n".join(contract["flags"]) if contract["flags"] else "✅ No dangerous functions"

    msg = (
        f"🟡 *BSC SCANNER — {alert_type}* {mc_tag}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 *{name}* `${symbol}`\n"
        f"🏦 DEX: {dex_name}\n"
        f"📍 `{addr[:8]}...{addr[-6:]}`\n"
        f"⏱ Pair age: {age_str}\n\n"
    )

    if is_waking:
        msg += f"🚨 *{wake_signal}*\n\n"

    msg += (
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 *MARKET DATA*\n"
        f"💰 Price: ${price}\n"
        f"📈 MCap: ${mkt_cap:,.0f}\n"
        f"💧 Liquidity: ${liq:,.0f} ({liq_mc}% of mcap)\n"
        f"📦 Vol 5m: ${vol_5m:,.0f}\n"
        f"📦 Vol 1h: ${vol_1h:,.0f}\n"
        f"📦 Vol 6h: ${vol_6h:,.0f}\n"
        f"📦 Vol 24h: ${vol_24h:,.0f}\n\n"
        f"📉 *PRICE CHANGE*\n"
        f"5m: {ch_5m}% | 1h: {ch_1h}% | 6h: {ch_6h}% | 24h: {ch_24h}%\n\n"
        f"🔄 *TRANSACTIONS*\n"
        f"5m: {b5m} buys / {s5m} sells\n"
        f"1h: {b1h} buys / {s1h} sells — {buy_pct}% buy pressure\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🛡 *SECURITY — {risk}*\n"
        f"🍯 Honeypot: {'🔴 YES — AVOID' if is_honeypot else '🟢 Clean'}\n"
    )
    if hp_reason:
        msg += f"   └ {hp_reason}\n"
    msg += (
        f"📄 Contract: {'✅ Verified' if contract['verified'] else '❌ UNVERIFIED'}\n"
        f"🔑 Ownership: {'✅ Renounced' if renounced else '⚠️ Not renounced'}\n"
        f"👻 Hidden owner: {'🔴 YES' if hidden_owner else '✅ No'}\n"
        f"🖨️ Mintable: {'🔴 YES' if is_mintable else '✅ No'}\n"
        f"🌀 Proxy contract: {'🔴 YES' if is_proxy else '✅ No'}\n"
        f"🔓 Can reclaim ownership: {'🔴 YES' if can_take_back else '✅ No'}\n"
        f"💸 Buy tax: {f'{buy_tax_hp}%' if buy_tax_hp is not None else 'N/A'} | "
        f"Sell tax: {f'{sell_tax_hp}%' if sell_tax_hp is not None else 'N/A'}\n"
        f"🐋 Top 10 holders: {round(top10_pct, 1)}%\n"
        f"👥 Holders: {holder_display}\n"
        f"🔬 Contract score: {contract['score']}/100\n"
        f"{contract_flags}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    if dump_flag:
        msg += f"⛔ *DUMP PATTERN: {dump_reason}*\n\n"

    msg += (
        f"🔥 *ACCUMULATION: {acc['score']}/100*\n"
        f"{chr(10).join(acc['signals']) if acc['signals'] else 'No strong signals'}\n\n"
        f"📣 *VIRALITY: {viral['score']}/100*\n"
        f"{chr(10).join(viral['signals']) if viral['signals'] else 'Low activity'}\n\n"
        f"🎭 *NARRATIVE:* {narrative_str}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 *PREDICTION:* {prediction}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 [DexScreener]({dex_url}) | [BSCScan](https://bscscan.com/token/{addr})"
    )

    return msg


# ── SCAN LOOP ─────────────────────────────────────────────────────────────────

async def run_scan(session: aiohttp.ClientSession, app: Application):
    if not subscribed_chats:
        return

    logger.info("BSC scan cycle starting...")
    pairs = await fetch_all_bsc_pairs(session)
    logger.info(f"Processing {len(pairs)} BSC pairs")

    for pair_data in pairs:
        try:
            addr    = pair_data.get("baseToken", {}).get("address", "")
            liq     = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)
            mkt_cap = float(pair_data.get("marketCap", 0) or 0)
            vol_1h  = float(pair_data.get("volume", {}).get("h1", 0) or 0)
            vol_24h = float(pair_data.get("volume", {}).get("h24", 0) or 0)

            if not addr:
                continue

            # Minimum liquidity
            is_micro = 0 < mkt_cap <= MICRO_CAP_MAX
            min_liq  = MICRO_MIN_LIQUIDITY if is_micro else STD_MIN_LIQUIDITY
            if liq < min_liq:
                # Still save to history so we track it
                prev = token_history.get(addr, {})
                token_history[addr] = {
                    "vol_1h": vol_1h,
                    "vol_24h": vol_24h,
                    "price": pair_data.get("priceUsd"),
                    "ticker": pair_data.get("baseToken", {}).get("symbol", "???"),
                    "timestamp": time.time(),
                    "scan_count": prev.get("scan_count", 0) + 1,
                }
                continue

            # Anti-dump check
            dump_flag, dump_reason = is_dump_pattern(pair_data)
            if dump_flag:
                prev = token_history.get(addr, {})
                token_history[addr] = {
                    "vol_1h": vol_1h, "vol_24h": vol_24h,
                    "price": pair_data.get("priceUsd"),
                    "ticker": pair_data.get("baseToken", {}).get("symbol", "???"),
                    "timestamp": time.time(),
                    "scan_count": prev.get("scan_count", 0) + 1,
                }
                logger.info(f"Dump filtered: {addr[:8]} — {dump_reason}")
                continue

            # New pair detection
            is_new, new_age_str = is_new_pair(pair_data)
            is_new_unseen = is_new and addr not in seen_new_pairs

            # Sleeping giant detection
            prev = token_history.get(addr)
            is_waking, wake_signal = is_sleeping_giant(pair_data, prev)

            # Accumulation score
            acc = accumulation_score(pair_data, prev, is_waking)

            # Update history
            token_history[addr] = {
                "vol_1h": vol_1h,
                "vol_24h": vol_24h,
                "price": pair_data.get("priceUsd"),
                    "ticker": pair_data.get("baseToken", {}).get("symbol", "???"),
                "timestamp": time.time(),
                "scan_count": (prev.get("scan_count", 0) if prev else 0) + 1,
            }

            # Determine if we should alert
            should_alert = False

            if is_new_unseen and liq >= NEW_PAIR_MIN_LIQUIDITY and acc["score"] >= NEW_PAIR_MIN_SCORE:
                should_alert = True
                seen_new_pairs.add(addr)
            elif is_waking and acc["score"] >= MIN_SCORE_WAKE:
                should_alert = True
            elif acc["score"] >= MIN_SCORE_STANDARD:
                should_alert = True

            if not should_alert:
                continue

            # Cooldown
            if time.time() - alerted_tokens.get(addr, 0) < ALERT_COOLDOWN:
                continue

            # Build and send
            logger.info(f"ALERT {pair_data.get('baseToken',{}).get('name','?')} | score={acc['score']} | new={is_new_unseen} | waking={is_waking} | mc=${mkt_cap:,.0f}")
            report = await build_report(session, pair_data, is_waking, wake_signal, is_new_unseen, new_age_str)
            alerted_tokens[addr] = time.time()

            for chat_id in list(subscribed_chats):
                try:
                    await app.bot.send_message(
                        chat_id=chat_id,
                        text=report,
                        parse_mode=ParseMode.MARKDOWN,
                        disable_web_page_preview=True
                    )
                    await asyncio.sleep(0.4)
                except Exception as e:
                    logger.warning(f"Send error {chat_id}: {e}")

        except Exception as e:
            logger.warning(f"Pair processing error: {e}")
            continue


# ── COMMANDS ──────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    subscribed_chats.add(update.effective_chat.id)
    await update.message.reply_text(
        "🟡 *BSC MEME SCANNER ACTIVATED*\n\n"
        "Monitoring ALL BSC DEXes:\n"
        "• PancakeSwap v2 + v3\n"
        "• ApeSwap, BiSwap, BabySwap, MDEX + more\n\n"
        "What this bot hunts:\n"
        "• 🆕 New pairs (under 48h) with strong early signals\n"
        "• 💤🔥 Sleeping tokens waking up — ANY age\n"
        "• 💎 Micro caps showing first volume ($5k-$50k)\n"
        "• 📈 Genuine accumulation patterns\n"
        "• ⛔ Dump patterns filtered out automatically\n\n"
        "*Commands:*\n"
        "/scan `<address>` — scan any BSC token\n"
        "/status — scanner stats\n"
        "/filters — current settings\n"
        "/watchlist — tokens being tracked\n"
        "/stop — pause alerts",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    subscribed_chats.discard(update.effective_chat.id)
    await update.message.reply_text("⏸ Alerts paused. Send /start to resume.")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    waking = sum(1 for addr, h in token_history.items() if h.get("scan_count", 0) > 1)
    await update.message.reply_text(
        f"*BSC SCANNER STATUS*\n\n"
        f"✅ Running\n"
        f"📡 Chain: BSC only\n"
        f"🏦 DEXes: PancakeSwap v2/v3, ApeSwap, BiSwap, BabySwap, MDEX + more\n"
        f"⏱ Scan every: {SCAN_INTERVAL}s\n"
        f"🪙 Tokens in memory: {len(token_history)}\n"
        f"👁 Tokens with history: {waking}\n"
        f"🔔 Alerts sent: {len(alerted_tokens)}\n"
        f"👥 Subscribers: {len(subscribed_chats)}",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_filters(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"*BSC SCANNER FILTERS*\n\n"
        f"🆕 *New Pair Detection (under 48h old)*\n"
        f"  Min liquidity: ${NEW_PAIR_MIN_LIQUIDITY:,}\n"
        f"  Min score: {NEW_PAIR_MIN_SCORE}/100\n\n"
        f"💤 *Sleeping Giant Detection*\n"
        f"  Previous vol must be under: ${WAKE_PREV_MAX_VOL:,}\n"
        f"  Wake-up min volume: ${WAKE_MIN_VOLUME_USD:,}/hr\n"
        f"  Early zone (best entry): ${WAKE_MIN_VOLUME_USD:,}–${WAKE_MAX_VOLUME_USD:,}/hr\n"
        f"  Spike multiplier: {WAKE_VOLUME_SPIKE_MULT}x\n\n"
        f"💎 *Micro Cap (under ${MICRO_CAP_MAX:,} mcap)*\n"
        f"  Min liquidity: ${MICRO_MIN_LIQUIDITY:,}\n"
        f"  Min score: {MIN_SCORE_WAKE}/100\n\n"
        f"📊 *Standard Tokens*\n"
        f"  Min liquidity: ${STD_MIN_LIQUIDITY:,}\n"
        f"  Min score: {MIN_SCORE_STANDARD}/100\n\n"
        f"⛔ *Anti-Dump*\n"
        f"  Max sell ratio 1h: {round(MAX_SELL_RATIO_1H*100)}%\n"
        f"  Min liq/mcap ratio: {round(MIN_LIQ_TO_MCAP*100)}%\n"
        f"  Max price drop 1h: {MAX_PRICE_DROP_1H}%\n\n"
        f"⏰ Alert cooldown: {ALERT_COOLDOWN//60} mins per token",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not token_history:
        await update.message.reply_text("No tokens in memory yet. Give it a few scan cycles.")
        return

    # Show top 20 most active sorted by 1h volume
    sorted_tokens = sorted(token_history.items(), key=lambda x: x[1].get("vol_1h", 0), reverse=True)[:20]
    lines = ["*TOP 20 TRACKED TOKENS*\n"]
    for addr, h in sorted_tokens:
        vol    = h.get("vol_1h", 0)
        scans  = h.get("scan_count", 0)
        ticker = h.get("ticker", "???")
        dex_link = f"https://dexscreener.com/bsc/{addr}"
        lines.append(
            f"[${ticker}]({dex_link})\n`{addr}` | Vol 1h: ${vol:,.0f} | Scans: {scans}"
        )

    await update.message.reply_text("\n\n".join(lines), parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)


async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /scan <BSC token address>")
        return

    address = context.args[0].strip()
    msg = await update.message.reply_text("🔍 Scanning BSC token... please wait")

    async with aiohttp.ClientSession() as session:
        pair_data = await dexscreener_token(session, address)
        if not pair_data:
            await msg.edit_text("❌ Token not found on any BSC DEX. Check the address.")
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

    async def scan_job(context):
        async with aiohttp.ClientSession() as session:
            await run_scan(session, app)

    app.job_queue.run_repeating(scan_job, interval=SCAN_INTERVAL, first=20)

    logger.info("BSC Meme Scanner starting...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
