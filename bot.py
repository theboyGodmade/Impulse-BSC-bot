"""
BSC MEME SCANNER BOT v5
Core upgrade: Reserve monitoring for old token detection
- Builds a database of ALL PancakeSwap pairs from last 30 days via BSCScan
- Monitors on-chain reserves via RPC — catches movement before DexScreener
- A 21-day old CTO waking up is detected the moment reserves change
- One alert per token permanently (unless in favlist → 50% move re-alerts)
- ⭐ button on alerts adds token directly to favlist
- Correct MC labels
"""

import asyncio
import logging
import os
import time
from typing import Optional

import aiohttp
from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
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

RPC_ENDPOINTS = [
    "https://go.getblock.us/75254fd9f61c4b43af30f2e55be8f55d",
    "https://rpc.ankr.com/bsc/0f0dde5ab6eee670d869cfe84af8d17eab6b832a050e17b3241f9b0dc513f1e5",
]

# PancakeSwap V2 factory + events
PANCAKE_V2_FACTORY = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"
PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
WBNB               = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"

# getReserves() selector
GET_RESERVES_SELECTOR = "0x0902f1ac"

# ── THRESHOLDS ────────────────────────────────────────────────────────────────
MIN_LIQ_MICRO        = 1_500
MIN_LIQ_STD          = 4_000

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

# Reserve monitoring
RESERVE_CHANGE_PCT   = 3.0    # % change in reserves to flag as activity
RESERVE_MIN_STABLE   = 4      # must be stable for this many checks before flagging
RESERVE_BATCH_SIZE   = 60     # pairs to check per reserve scan cycle
DB_BUILD_BATCH       = 100    # pairs to add to DB per BSCScan query
DB_LOOKBACK_DAYS     = 30     # how far back to look for old pairs
BSC_BLOCKS_PER_DAY   = 28_800 # ~3s per block

SCAN_INTERVAL        = 60
RPC_NEW_PAIR_INTERVAL = 30
RESERVE_SCAN_INTERVAL = 45
DB_BUILD_INTERVAL    = 300    # rebuild/extend DB every 5 mins
FAV_CHECK_INTERVAL   = 120

# ── MC LABELS ─────────────────────────────────────────────────────────────────
def mc_label(mc: float) -> str:
    if mc <= 0:       return ""
    if mc < 20_000:   return "🔬 MICRO"
    if mc < 100_000:  return "💎 LOW"
    if mc < 200_000:  return "📊 LOW-MID"
    if mc < 1_000_000: return "📈 MID CAP"
    if mc < 20_000_000: return "🔥 HIGH CAP"
    return "🏆 VERY HIGH"

# ── STATE ─────────────────────────────────────────────────────────────────────
subscribed_chats = set()
alerted_tokens   = {}   # addr -> {ts, price}
token_history    = {}   # addr -> snapshot
seen_new_pairs   = set()
favourites       = {}   # chat_id -> {addr -> info}
pinned_msg_ids   = {}   # chat_id -> message_id

# Reserve monitoring state
pair_database    = {}   # pair_addr -> token_addr  (our local DB of old pairs)
pair_reserves    = {}   # pair_addr -> {r0, r1, stable_count, last_ts, alerted}
db_scan_pointer  = 0    # rotating index into pair_database for batch scanning
last_rpc_block   = 0    # for new pair RPC detection


# ── RPC ───────────────────────────────────────────────────────────────────────

async def rpc_call(session: aiohttp.ClientSession, method: str, params: list) -> Optional[object]:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    for endpoint in RPC_ENDPOINTS:
        try:
            async with session.post(endpoint, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    data = await r.json()
                    if "result" in data:
                        return data["result"]
        except Exception as e:
            logger.warning(f"RPC {endpoint[:40]}: {e}")
    return None


async def rpc_block_number(session) -> Optional[int]:
    r = await rpc_call(session, "eth_blockNumber", [])
    return int(r, 16) if r else None


async def rpc_get_reserves(session, pair_addr: str) -> Optional[tuple]:
    """
    Call getReserves() on a PancakeSwap V2 pair.
    Returns (reserve0, reserve1) or None.
    """
    result = await rpc_call(session, "eth_call", [
        {"to": pair_addr, "data": GET_RESERVES_SELECTOR},
        "latest"
    ])
    if not result or result == "0x" or len(result) < 194:
        return None
    try:
        # Result is 3 x 32-byte values: reserve0, reserve1, blockTimestampLast
        data = result[2:]  # strip 0x
        r0 = int(data[0:64], 16)
        r1 = int(data[64:128], 16)
        return r0, r1
    except Exception:
        return None


async def rpc_get_logs(session, from_block: int, to_block: int) -> list:
    params = [{
        "fromBlock": hex(from_block),
        "toBlock": hex(to_block),
        "address": PANCAKE_V2_FACTORY,
        "topics": [PAIR_CREATED_TOPIC]
    }]
    result = await rpc_call(session, "eth_getLogs", params)
    return result if isinstance(result, list) else []


async def rpc_balance_of(session, token: str, wallet: str) -> Optional[int]:
    padded = wallet.replace("0x", "").zfill(64)
    result = await rpc_call(session, "eth_call", [
        {"to": token, "data": f"0x70a08231{padded}"},
        "latest"
    ])
    if result and result != "0x":
        try:
            return int(result, 16)
        except Exception:
            pass
    return None


async def rpc_total_supply(session, token: str) -> Optional[int]:
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


def parse_pair_log(log: dict) -> Optional[tuple]:
    """Extract (pair_addr, token_addr) from PairCreated log"""
    try:
        topics = log.get("topics", [])
        if len(topics) < 3:
            return None
        token0 = "0x" + topics[1][-40:]
        token1 = "0x" + topics[2][-40:]
        # Pair address is in data (first 32 bytes)
        data = log.get("data", "")
        if len(data) >= 66:
            pair_addr = "0x" + data[26:66]
        else:
            return None
        # Token is whichever is not WBNB
        if token0.lower() == WBNB:
            return pair_addr.lower(), token1.lower()
        if token1.lower() == WBNB:
            return pair_addr.lower(), token0.lower()
        return pair_addr.lower(), token0.lower()
    except Exception:
        return None


# ── DATABASE BUILDER ──────────────────────────────────────────────────────────

async def build_pair_database(session: aiohttp.ClientSession):
    """
    Query BSCScan for PairCreated events going back 30 days.
    Adds pairs to pair_database. Called periodically to keep DB fresh.
    Batched to respect BSCScan rate limits.
    """
    global pair_database

    current_block = await rpc_block_number(session)
    if not current_block:
        return

    lookback_blocks = DB_LOOKBACK_DAYS * BSC_BLOCKS_PER_DAY
    from_block = current_block - lookback_blocks

    existing = len(pair_database)

    try:
        # BSCScan getLogs for PairCreated on PancakeSwap V2 factory
        # Page through results to build the database
        for page in range(1, 6):  # up to 5 pages = 500 pairs per build cycle
            url = (
                f"{BSCSCAN_URL}?module=logs&action=getLogs"
                f"&address={PANCAKE_V2_FACTORY}"
                f"&topic0={PAIR_CREATED_TOPIC}"
                f"&fromBlock={from_block}&toBlock=latest"
                f"&page={page}&offset={DB_BUILD_BATCH}"
                f"&apikey={BSCSCAN_KEY}"
            )
            data = await http_get(session, url)
            if not data or data.get("status") != "1":
                break

            logs = data.get("result", [])
            if not logs:
                break

            added = 0
            for log in logs:
                parsed = parse_pair_log(log)
                if parsed:
                    pair_addr, token_addr = parsed
                    if pair_addr not in pair_database:
                        pair_database[pair_addr] = token_addr
                        added += 1

            logger.info(f"DB build page {page}: +{added} pairs (total: {len(pair_database)})")
            await asyncio.sleep(0.3)  # respect rate limit

    except Exception as e:
        logger.warning(f"DB build error: {e}")

    new_total = len(pair_database)
    if new_total > existing:
        logger.info(f"Pair DB: {existing} → {new_total} pairs (+{new_total - existing})")


# ── RESERVE MONITOR ───────────────────────────────────────────────────────────

async def scan_reserves(session: aiohttp.ClientSession, app: Application):
    """
    Core sleeping giant detector.
    Scans a rotating batch of pairs from pair_database.
    Calls getReserves() on each, detects movement after silence.
    """
    global db_scan_pointer

    if not pair_database or not subscribed_chats:
        return

    pairs_list = list(pair_database.items())
    total = len(pairs_list)

    # Take a rotating batch
    start = db_scan_pointer % total
    end   = min(start + RESERVE_BATCH_SIZE, total)
    batch = pairs_list[start:end]

    # Wrap around
    if len(batch) < RESERVE_BATCH_SIZE and total > RESERVE_BATCH_SIZE:
        batch += pairs_list[:RESERVE_BATCH_SIZE - len(batch)]

    db_scan_pointer = end % total

    logger.info(f"Reserve scan: checking {len(batch)} pairs (DB size: {total})")

    flagged_tokens = []

    for pair_addr, token_addr in batch:
        try:
            reserves = await rpc_get_reserves(session, pair_addr)
            if not reserves:
                continue

            r0, r1 = reserves

            # Skip if reserves are zero (dead pair)
            if r0 == 0 or r1 == 0:
                continue

            prev = pair_reserves.get(pair_addr)

            if not prev:
                # First time seeing this pair — store baseline, don't alert
                pair_reserves[pair_addr] = {
                    "r0": r0, "r1": r1,
                    "stable_count": 0,
                    "last_ts": time.time(),
                    "alerted": False
                }
                continue

            prev_r0 = prev["r0"]
            prev_r1 = prev["r1"]
            stable  = prev["stable_count"]

            # Calculate % change in reserves
            change_r0 = abs(r0 - prev_r0) / prev_r0 * 100 if prev_r0 > 0 else 0
            change_r1 = abs(r1 - prev_r1) / prev_r1 * 100 if prev_r1 > 0 else 0
            max_change = max(change_r0, change_r1)

            if max_change < RESERVE_CHANGE_PCT:
                # Reserves stable — increment stable count
                pair_reserves[pair_addr]["stable_count"] = min(stable + 1, 100)
                pair_reserves[pair_addr]["r0"] = r0
                pair_reserves[pair_addr]["r1"] = r1
                pair_reserves[pair_addr]["last_ts"] = time.time()
                continue

            # Reserves changed — check if it was sleeping long enough
            if stable >= RESERVE_MIN_STABLE:
                # This is a genuine wakeup signal
                logger.info(
                    f"RESERVE WAKEUP: {token_addr[:10]} "
                    f"pair {pair_addr[:10]} "
                    f"change={round(max_change, 1)}% after {stable} stable cycles"
                )
                flagged_tokens.append((token_addr, pair_addr, stable, max_change))

            # Update reserves
            pair_reserves[pair_addr] = {
                "r0": r0, "r1": r1,
                "stable_count": 0,
                "last_ts": time.time(),
                "alerted": False
            }

            await asyncio.sleep(0.05)

        except Exception as e:
            logger.warning(f"Reserve check error {pair_addr[:10]}: {e}")

    # Now process flagged tokens through the full pipeline
    for token_addr, pair_addr, stable_cycles, reserve_change in flagged_tokens:
        try:
            if token_addr in alerted_tokens:
                continue

            # Get full market data from DexScreener
            pair_data = await dex_token(session, token_addr)
            if not pair_data:
                # DexScreener doesn't have it yet — still worth alerting with what we know
                logger.info(f"Reserve wakeup {token_addr[:10]} not yet on DexScreener")
                continue

            liq = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)
            mc  = float(pair_data.get("marketCap", 0) or 0)

            if liq < MIN_LIQ_MICRO:
                continue

            dump_flag, _ = is_dump(pair_data)
            if dump_flag:
                continue

            prev = token_history.get(token_addr)
            acc  = score_token(pair_data, prev, True)  # treat as waking

            # Build wake signal string
            wake_signal = (
                f"⛓ On-chain reserves moved {round(reserve_change, 1)}% "
                f"after {stable_cycles} stable cycles — "
                f"first trading activity detected"
            )

            report, markup = await build_report(
                session, pair_data,
                is_waking=True,
                wake_signal=wake_signal,
                reserve_detected=True
            )

            price = float(pair_data.get("priceUsd") or 0)
            alerted_tokens[token_addr] = {"ts": time.time(), "price": price}

            name = pair_data.get("baseToken", {}).get("name", "?")
            logger.info(f"RESERVE ALERT: {name} mc=${mc:,.0f} liq=${liq:,.0f}")

            for chat_id in list(subscribed_chats):
                try:
                    await app.bot.send_message(
                        chat_id=chat_id,
                        text=report,
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=markup,
                        disable_web_page_preview=True
                    )
                    await asyncio.sleep(0.3)
                except Exception as e:
                    logger.warning(f"Send error {chat_id}: {e}")

        except Exception as e:
            logger.warning(f"Flagged token processing error: {e}")


# ── HTTP HELPER ───────────────────────────────────────────────────────────────

async def http_get(session: aiohttp.ClientSession, url: str, headers: dict = None) -> Optional[dict]:
    try:
        async with session.get(url, headers=headers or {}, timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status == 200:
                return await r.json()
    except Exception as e:
        logger.warning(f"HTTP GET error {url[:60]}: {e}")
    return None


# ── DEXSCREENER ───────────────────────────────────────────────────────────────

async def dex_token(session: aiohttp.ClientSession, address: str) -> Optional[dict]:
    data = await http_get(session, f"https://api.dexscreener.com/latest/dex/tokens/{address}")
    if not data:
        return None
    pairs = [p for p in data.get("pairs", []) if p.get("chainId") == "bsc"]
    if pairs:
        return max(pairs, key=lambda x: float(x.get("liquidity", {}).get("usd", 0) or 0))
    return None


async def fetch_bsc_pairs(session: aiohttp.ClientSession) -> list:
    results = []
    seen = set()

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
                bsc = [p for p in r.get("pairs", []) if p.get("chainId") == "bsc"]
                add(bsc[:60])
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
                bsc = [p for p in r.get("pairs", []) if p.get("chainId") == "bsc"]
                add(bsc[:40])
        except Exception as e:
            logger.warning(f"DexScreener batch error {i}: {e}")

    # Re-check sleeping tokens from history
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


# ── SECURITY ──────────────────────────────────────────────────────────────────

async def honeypot_check(session, addr):
    return await http_get(session, f"https://api.honeypot.is/v2/IsHoneypot?address={addr}&chainID=56") or {}


async def goplus_check(session, addr):
    data = await http_get(session, f"https://api.gopluslabs.io/api/v1/token_security/56?contract_addresses={addr}")
    if data:
        r = data.get("result", {})
        if r:
            return list(r.values())[0]
    return {}


async def get_top_holders(session, addr):
    url = (f"{BSCSCAN_URL}?module=token&action=tokenholderlist"
           f"&contractaddress={addr}&page=1&offset=10&apikey={BSCSCAN_KEY}")
    data = await http_get(session, url)
    return data.get("result", []) if data and data.get("status") == "1" else []


async def get_deployer(session, addr):
    url = (f"{BSCSCAN_URL}?module=contract&action=getcontractcreation"
           f"&contractaddresses={addr}&apikey={BSCSCAN_KEY}")
    data = await http_get(session, url)
    if data and data.get("status") == "1" and data.get("result"):
        return data["result"][0].get("contractCreator")
    return None


async def get_contract_source(session, addr):
    url = (f"{BSCSCAN_URL}?module=contract&action=getsourcecode"
           f"&address={addr}&apikey={BSCSCAN_KEY}")
    data = await http_get(session, url)
    if data and data.get("status") == "1" and data.get("result"):
        return data["result"][0].get("SourceCode", "")
    return ""


async def gmgn_holders(session, addr):
    try:
        data = await http_get(
            session,
            f"https://gmgn.ai/defi/quotation/v1/tokens/bsc/{addr}",
            headers={"User-Agent": "Mozilla/5.0"}
        )
        if data:
            t = data.get("data", {}).get("token", {})
            h = t.get("holder_count") or t.get("holders")
            return int(h) if h else None
    except Exception:
        pass
    return None


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
    for k, (msg, p) in checks.items():
        if k in source.lower():
            flags.append(msg)
            score -= p
    return {"verified": True, "flags": flags, "score": max(0, score)}


def dex_paid_status(pair_data: dict) -> str:
    boosts = pair_data.get("boosts", {})
    active = boosts.get("active", 0) or 0
    if active > 0:
        return f"✅ Paid — {active} active boost{'s' if active > 1 else ''}"
    if pair_data.get("info"):
        return "ℹ️ Free listing (community submitted)"
    return "❌ Not listed"


# ── DUMP FILTER ───────────────────────────────────────────────────────────────

def is_dump(pair_data: dict) -> tuple:
    v1h   = float(pair_data.get("volume", {}).get("h1", 0) or 0)
    c1h   = float(pair_data.get("priceChange", {}).get("h1", 0) or 0)
    c6h   = float(pair_data.get("priceChange", {}).get("h6", 0) or 0)
    liq   = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)
    mc    = float(pair_data.get("marketCap", 0) or 0)
    buys  = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    sells = int(pair_data.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
    total = buys + sells

    if total > 8 and sells / total > MAX_SELL_RATIO:
        return True, f"Sell dominant — {round(sells/total*100)}% sells"
    if v1h > 3000 and c1h < MAX_DROP_1H:
        return True, f"Dumping {c1h}% with volume"
    if mc > 0 and liq > 0 and liq / mc < MIN_LIQ_MCAP:
        return True, f"Liq only {round(liq/mc*100,1)}% of mcap"
    if liq > 0 and v1h > liq * 15:
        return True, "Wash trading suspected"
    if c6h > 500 and c1h < -20:
        return True, f"Already pumped {c6h}% and reversing"
    return False, ""


# ── SLEEPING GIANT (DexScreener-based) ───────────────────────────────────────

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


# ── SCORING ───────────────────────────────────────────────────────────────────

def score_token(pair_data: dict, prev: Optional[dict], is_waking: bool) -> dict:
    score = 0
    signals = []
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
            score += 30
            signals.append("🚨 Token waking up after silence")

        if v1h > 0 and v6h > 0:
            avg = v6h / 6
            if avg > 0:
                m = v1h / avg
                if m >= 4:    score += 20; signals.append(f"📈 Volume {round(m,1)}x the 6h avg")
                elif m >= 2:  score += 12; signals.append(f"📈 Volume {round(m,1)}x the 6h avg")
                elif m >= 1.3: score += 6; signals.append(f"📈 Volume picking up {round(m,1)}x")

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

    if b5m >= 15:  score += 28; signals.append(f"🔥 {b5m} buys in 5m")
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

def narrative(name, symbol):
    text = f"{name} {symbol}".lower()
    found = [l for l, kws in NARRATIVES.items() if any(k in text for k in kws)]
    return " | ".join(found) if found else "None detected"


def risk_label(is_hp, cscore, top10, gp):
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


def predict(pair_data, acc, waking, dump):
    if dump: return "🔴 DUMP PATTERN — avoid"
    c1h = float(pair_data.get("priceChange", {}).get("h1", 0) or 0)
    c6h = float(pair_data.get("priceChange", {}).get("h6", 0) or 0)
    if waking and acc >= 60: return "🚀 HIGH POTENTIAL — waking after silence"
    if waking: return "📈 WATCH — first activity appearing"
    if acc >= 75 and c1h > 0 and c6h > 0: return "🚀 STRONG BULLISH"
    if acc >= 55 and c1h > 0: return "📈 BULLISH — accumulation forming"
    if acc >= 40: return "🟡 EARLY SIGNAL — watch closely"
    return "⚪ NEUTRAL"


# ── FULL REPORT ───────────────────────────────────────────────────────────────

async def build_report(
    session,
    pair_data: dict,
    is_waking: bool = False,
    wake_signal: str = "",
    is_new: bool = False,
    new_age_str: str = "",
    reserve_detected: bool = False,
) -> tuple:
    """Returns (text, InlineKeyboardMarkup)"""

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
    c6h_s     = pair_data.get("priceChange", {}).get("h6", "0")
    c24h      = pair_data.get("priceChange", {}).get("h24", "0")
    b1h       = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    s1h       = int(pair_data.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
    b5m_t     = int(pair_data.get("txns", {}).get("m5", {}).get("buys", 0) or 0)
    s5m_t     = int(pair_data.get("txns", {}).get("m5", {}).get("sells", 0) or 0)

    ts = pair_data.get("pairCreatedAt")
    if ts:
        ah  = (time.time() - int(ts)/1000) / 3600
        age = f"{round(ah*60)}m" if ah < 1 else (f"{round(ah,1)}h" if ah < 48 else f"{round(ah/24,1)}d")
    else:
        age = "Unknown"

    t1h = b1h + s1h
    bp  = round(b1h/t1h*100) if t1h > 0 else 0
    lm  = round(liq/mc*100, 1) if mc > 0 else 0

    prev = token_history.get(addr)
    acc  = score_token(pair_data, prev, is_waking)
    vir  = virality(pair_data)

    token_history[addr] = {
        "vol_1h": v1h, "vol_24h": v24h, "price": price_str,
        "ticker": symbol, "mcap": mc, "timestamp": time.time(),
        "scan_count": (prev.get("scan_count",0) if prev else 0) + 1,
    }

    # Parallel security + data
    results = await asyncio.gather(
        honeypot_check(session, addr),
        get_contract_source(session, addr),
        goplus_check(session, addr),
        gmgn_holders(session, addr),
        get_top_holders(session, addr),
        get_deployer(session, addr),
        rpc_total_supply(session, addr),
        return_exceptions=True
    )

    hp_data, src, gp_data, gmgn, top_holders, deployer, total_supply = results

    hp  = hp_data  if isinstance(hp_data, dict)  else {}
    gp  = gp_data  if isinstance(gp_data, dict)  else {}
    src = src      if isinstance(src, str)        else ""
    top_holders   = top_holders   if isinstance(top_holders, list) else []
    deployer      = deployer      if isinstance(deployer, str)     else None
    total_supply  = total_supply  if isinstance(total_supply, int) else None
    gmgn          = gmgn          if isinstance(gmgn, int)         else None

    is_hp         = hp.get("isHoneypot", False)
    hp_reason     = hp.get("honeypotResult", {}).get("reason", "")
    buy_tax       = hp.get("simulationResult", {}).get("buyTax")
    sell_tax      = hp.get("simulationResult", {}).get("sellTax")
    contract      = analyze_contract(src)
    renounced     = gp.get("owner_address","") == "0x0000000000000000000000000000000000000000"
    is_mintable   = gp.get("is_mintable","0") == "1"
    is_proxy      = gp.get("is_proxy","0") == "1"
    hidden_owner  = gp.get("hidden_owner","0") == "1"
    can_take_back = gp.get("can_take_back_ownership","0") == "1"

    # Top 10 from BSCScan
    top10_pct = 0.0
    top10_str = "Unknown"
    if top_holders:
        try:
            pcts = [float(h.get("percentage", 0) or 0) for h in top_holders[:10]]
            total_pct = sum(pcts)
            if total_pct > 0:
                top10_pct = total_pct
                top10_str = f"{round(total_pct, 1)}% (BSCScan ✓)"
            elif total_supply and total_supply > 0:
                quantities = [int(h.get("TokenHolderQuantity", 0) or 0) for h in top_holders[:10]]
                top10_pct = sum(quantities) / total_supply * 100
                top10_str = f"{round(top10_pct, 1)}% (BSCScan ✓)"
        except Exception as e:
            logger.warning(f"Top10 calc: {e}")

    # Holder count
    gp_holders = gp.get("holder_count")
    holder_sources = {}
    if gp_holders: holder_sources["GoPlus"] = int(gp_holders)
    if gmgn:       holder_sources["GMGN"]   = int(gmgn)
    if holder_sources:
        best = max(holder_sources.values())
        if len(holder_sources) == 2:
            gv, gm = holder_sources.get("GoPlus",0), holder_sources.get("GMGN",0)
            hdisplay = f"{best:,} (GoPlus: {gv:,} | GMGN: {gm:,})"
            if abs(gv-gm) > max(gv,gm)*0.5 and abs(gv-gm) > 50:
                hdisplay += " ⚠️"
        else:
            hdisplay = f"{best:,} (via {list(holder_sources.keys())[0]})"
    else:
        hdisplay = "Unknown"

    # Dev holding via RPC
    dev_str = "Unknown"
    if deployer and total_supply:
        dev_pct = await get_dev_holding(session, addr, deployer, total_supply)
        if dev_pct is not None:
            dev_str = f"{round(dev_pct, 2)}%"
            if dev_pct > 20:   dev_str += " 🔴 HIGH"
            elif dev_pct > 10: dev_str += " ⚠️"
            elif dev_pct == 0: dev_str = "0% (sold/burned) ✅"
            else:              dev_str += " ✅"
        else:
            dev_str = f"`{deployer[:10]}...` (unavailable)"
    elif deployer:
        dev_str = f"`{deployer[:10]}...`"

    dex_paid = dex_paid_status(pair_data)
    dump_flag, dump_reason = is_dump(pair_data)
    risk = risk_label(is_hp, contract["score"], top10_pct, gp)
    narr = narrative(name, symbol)
    pred = predict(pair_data, acc["score"], is_waking, dump_flag)

    mclabel = mc_label(mc)

    # Alert type
    if reserve_detected:
        atype = "⛓ ON-CHAIN WAKEUP"
    elif is_new:
        atype = f"🆕 NEW — {new_age_str}"
    elif is_waking:
        atype = "💤🔥 SLEEPING GIANT"
    else:
        atype = "📡 ACCUMULATION"

    cflags = "\n".join(contract["flags"]) if contract["flags"] else "✅ No dangerous functions"

    msg = (
        f"🟡 *BSC — {atype}* {mclabel}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 *{name}* `${symbol}`\n"
        f"🏦 {dex_name} | ⏱ Age: {age}\n"
        f"📍 `{addr}`\n\n"
    )

    if wake_signal:
        msg += f"🚨 *{wake_signal}*\n\n"

    msg += (
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 *MARKET DATA*\n"
        f"💰 Price: ${price_str}\n"
        f"📈 MCap: ${mc:,.0f} | 💧 Liq: ${liq:,.0f} ({lm}%)\n"
        f"📦 Vol: 5m ${v5m:,.0f} | 1h ${v1h:,.0f} | 24h ${v24h:,.0f}\n"
        f"📉 5m: {c5m}% | 1h: {c1h}% | 6h: {c6h_s}% | 24h: {c24h}%\n"
        f"🔄 5m: {b5m_t}B/{s5m_t}S | 1h: {b1h}B/{s1h}S ({bp}% buys)\n\n"
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

    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "⭐ Add to Favlist",
            callback_data=f"addfav_{addr}"
        )
    ]])

    return msg, markup


# ── HELPER: dev holding ───────────────────────────────────────────────────────

async def get_dev_holding(session, token_addr, deployer, total_supply):
    if not deployer or not total_supply or total_supply == 0:
        return None
    balance = await rpc_balance_of(session, token_addr, deployer)
    if balance is None:
        return None
    return (balance / total_supply) * 100


# ── RPC NEW PAIR SCAN ─────────────────────────────────────────────────────────

async def rpc_scan_new_pairs(session: aiohttp.ClientSession, app: Application):
    """Detect brand new pairs directly from blockchain events"""
    global last_rpc_block

    if not subscribed_chats:
        return

    current_block = await rpc_block_number(session)
    if not current_block:
        return

    if last_rpc_block == 0:
        last_rpc_block = current_block - 150

    from_block = last_rpc_block + 1
    to_block   = min(current_block, from_block + 200)

    if from_block > to_block:
        return

    logs = await rpc_get_logs(session, from_block, to_block)
    last_rpc_block = to_block

    if not logs:
        return

    logger.info(f"RPC: {len(logs)} new pairs in blocks {from_block}-{to_block}")

    for log in logs:
        try:
            parsed = parse_pair_log(log)
            if not parsed:
                continue
            pair_addr, token_addr = parsed

            # Add to pair DB immediately for future reserve monitoring
            if pair_addr not in pair_database:
                pair_database[pair_addr] = token_addr

            if token_addr in alerted_tokens or token_addr in seen_new_pairs:
                continue

            await asyncio.sleep(5)  # let DexScreener index it

            pair_data = await dex_token(session, token_addr)
            if not pair_data:
                token_history[token_addr] = {
                    "vol_1h": 0, "vol_24h": 0, "price": None,
                    "ticker": "???", "mcap": 0,
                    "timestamp": time.time(), "scan_count": 0,
                }
                continue

            liq = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)
            if liq < MIN_LIQ_MICRO:
                continue

            dump_flag, _ = is_dump(pair_data)
            if dump_flag:
                continue

            prev = token_history.get(token_addr)
            acc  = score_token(pair_data, prev, False)
            if acc["score"] < MIN_SCORE_NEW:
                continue

            ts = pair_data.get("pairCreatedAt")
            age_h = (time.time() - int(ts)/1000) / 3600 if ts else 0
            new_age_str = f"{round(age_h*60)}m old" if age_h < 1 else f"{round(age_h,1)}h old"

            seen_new_pairs.add(token_addr)
            mc = float(pair_data.get("marketCap", 0) or 0)
            name = pair_data.get("baseToken", {}).get("name", "?")
            logger.info(f"RPC NEW PAIR: {name} mc=${mc:,.0f} score={acc['score']}")

            report, markup = await build_report(
                session, pair_data, is_new=True, new_age_str=new_age_str
            )
            price = float(pair_data.get("priceUsd") or 0)
            alerted_tokens[token_addr] = {"ts": time.time(), "price": price}

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
            logger.warning(f"RPC pair error: {e}")


# ── MAIN DEXSCREENER SCAN ─────────────────────────────────────────────────────

async def run_scan(session: aiohttp.ClientSession, app: Application):
    if not subscribed_chats:
        return

    logger.info("DexScreener scan...")
    pairs = await fetch_bsc_pairs(session)

    for pair_data in pairs:
        try:
            addr  = pair_data.get("baseToken", {}).get("address", "")
            liq   = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)
            mc    = float(pair_data.get("marketCap", 0) or 0)
            v1h   = float(pair_data.get("volume", {}).get("h1", 0) or 0)
            v24h  = float(pair_data.get("volume", {}).get("h24", 0) or 0)
            price = float(pair_data.get("priceUsd") or 0)

            if not addr:
                continue

            is_micro = mc <= 200_000
            min_liq  = MIN_LIQ_MICRO if is_micro else MIN_LIQ_STD

            prev = token_history.get(addr)
            token_history[addr] = {
                "vol_1h": v1h, "vol_24h": v24h,
                "price": pair_data.get("priceUsd"),
                "ticker": pair_data.get("baseToken", {}).get("symbol", "???"),
                "mcap": mc, "timestamp": time.time(),
                "scan_count": (prev.get("scan_count",0) if prev else 0) + 1,
            }

            if liq < min_liq:
                continue

            dump_flag, _ = is_dump(pair_data)
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

            # One alert per token — permanent unless in favlist
            already = addr in alerted_tokens
            is_faved = any(addr in favourites.get(cid, {}) for cid in subscribed_chats)

            if already and not is_faved:
                continue

            if already and is_faved:
                last_price = alerted_tokens[addr].get("price", 0)
                if last_price > 0 and price > 0:
                    move = abs((price - last_price) / last_price * 100)
                    if move < FAV_ALERT_PCT:
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
            for addr, info in list(favs.items()):
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
                        d = "🚀 UP" if c1h > 0 else "🔴 DOWN"
                        await app.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"⭐ *FAV ALERT — {ticker}*\n\n"
                                f"Moved {d} *{c1h}%* in 1h\n"
                                f"💰 Price: ${price}\n\n"
                                f"[View on DexScreener]({url})"
                            ),
                            parse_mode=ParseMode.MARKDOWN,
                            disable_web_page_preview=True
                        )
                except Exception as e:
                    logger.warning(f"Fav move error {addr}: {e}")


# ── BUTTON CALLBACKS ──────────────────────────────────────────────────────────

async def handle_addfav_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    chat_id = query.message.chat_id

    try:
        addr = query.data.split("_", 1)[1]
    except Exception:
        await query.answer("Error reading token address.", show_alert=True)
        return

    if chat_id not in favourites:
        favourites[chat_id] = {}

    if addr in favourites[chat_id]:
        await query.answer("Already in your Favlist ⭐", show_alert=False)
        return

    # Get ticker from history or DexScreener
    ticker = token_history.get(addr, {}).get("ticker", "???")
    price  = float(token_history.get(addr, {}).get("price") or 0)

    favourites[chat_id][addr] = {
        "ticker": ticker,
        "added_ts": time.time(),
        "last_price": price,
    }

    await query.answer(f"⭐ Added ${ticker} to Favlist!", show_alert=False)

    # Update button to show it's added
    try:
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("⭐ In Favlist ✅", callback_data=f"addfav_{addr}")
        ]]))
    except Exception:
        pass

    # Refresh pinned board
    await update_pinned_favs(context.application, chat_id)


# ── FAVOURITES BOARD ──────────────────────────────────────────────────────────

async def build_fav_board(session, chat_id: int) -> str:
    favs = favourites.get(chat_id, {})
    if not favs:
        return "⭐ *FAVOURITES*\n\nEmpty. Tap ⭐ on any alert to add a token."
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
                f"💰 ${price} | MCap: ${mc:,.0f} {mc_label(mc)}\n"
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
        BotCommand("watchlist", "View tracked tokens by MCap"),
        BotCommand("fav",       "Add token to favourites"),
        BotCommand("unfav",     "Remove from favourites"),
        BotCommand("favlist",   "Live prices for favourites"),
        BotCommand("status",    "Scanner stats + DB size"),
        BotCommand("filters",   "Current settings"),
    ])
    await update.message.reply_text(
        "🟡 *IMPULSE BSC SCANNER v5*\n\n"
        "⛓ Now monitors on-chain reserves directly.\n"
        "Old tokens moving at $30k MC — caught before DexScreener.\n\n"
        "Three detection layers:\n"
        "• ⛓ On-chain reserve changes (old tokens, any age)\n"
        "• 🆕 New pairs direct from blockchain\n"
        "• 📡 DexScreener accumulation signals\n\n"
        "One alert per token. Tap ⭐ to add to Favlist.\n"
        "Favlist tokens re-alert on ±50% moves.\n\n"
        "Use menu below ↓",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    subscribed_chats.discard(update.effective_chat.id)
    await update.message.reply_text("⏸ Paused. /start to resume.")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    fav_count = sum(len(v) for v in favourites.values())
    await update.message.reply_text(
        f"*IMPULSE BSC v5 STATUS*\n\n"
        f"✅ Running\n"
        f"⛓ Pair DB: {len(pair_database):,} pairs monitored\n"
        f"📊 Reserve snapshots: {len(pair_reserves):,}\n"
        f"🪙 Tokens in memory: {len(token_history):,}\n"
        f"🔔 Alerts sent: {len(alerted_tokens):,}\n"
        f"⭐ Favourites: {fav_count}\n"
        f"👥 Subscribers: {len(subscribed_chats)}\n"
        f"⛓ Last block: {last_rpc_block:,}",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_filters(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"*IMPULSE FILTERS*\n\n"
        f"⛓ *On-chain reserve monitoring*\n"
        f"  DB lookback: {DB_LOOKBACK_DAYS} days\n"
        f"  Reserve change threshold: {RESERVE_CHANGE_PCT}%\n"
        f"  Min stable cycles before alert: {RESERVE_MIN_STABLE}\n"
        f"  Batch size: {RESERVE_BATCH_SIZE} pairs/cycle\n\n"
        f"🆕 New pairs: score ≥{MIN_SCORE_NEW}\n"
        f"💤 Sleeping giant: score ≥{MIN_SCORE_WAKE}\n"
        f"📊 Standard: score ≥{MIN_SCORE_STD}\n\n"
        f"💧 Min liq (micro): ${MIN_LIQ_MICRO:,}\n"
        f"💧 Min liq (standard): ${MIN_LIQ_STD:,}\n\n"
        f"⭐ Fav re-alert: ±{FAV_ALERT_PCT}% move in 1h\n\n"
        f"*MC Labels*\n"
        f"🔬 Micro: under $20k\n"
        f"💎 Low: $20k–$100k\n"
        f"📊 Low-Mid: $100k–$200k\n"
        f"📈 Mid Cap: $200k–$1m\n"
        f"🔥 High Cap: $1m–$20m\n"
        f"🏆 Very High: $20m+",
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
        label  = mc_label(mc)
        lines.append(
            f"[${ticker}]({link})\n"
            f"`{addr}` | ${mc:,.0f} {label} | Scans: {scans}"
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
        await update.message.reply_text("No favourites. Tap ⭐ on any alert or use /fav <address>.")
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
    app.add_handler(CallbackQueryHandler(handle_addfav_callback, pattern=r"^addfav_"))

    async def dex_job(ctx):
        async with aiohttp.ClientSession() as session:
            await run_scan(session, app)

    async def rpc_new_pair_job(ctx):
        async with aiohttp.ClientSession() as session:
            await rpc_scan_new_pairs(session, app)

    async def reserve_job(ctx):
        async with aiohttp.ClientSession() as session:
            await scan_reserves(session, app)

    async def db_build_job(ctx):
        async with aiohttp.ClientSession() as session:
            await build_pair_database(session)

    async def fav_job(ctx):
        await check_fav_moves(app)

    # Stagger job starts so they don't all hit at once
    app.job_queue.run_repeating(db_build_job,      interval=DB_BUILD_INTERVAL,    first=5)
    app.job_queue.run_repeating(rpc_new_pair_job,  interval=RPC_NEW_PAIR_INTERVAL, first=10)
    app.job_queue.run_repeating(reserve_job,       interval=RESERVE_SCAN_INTERVAL, first=30)
    app.job_queue.run_repeating(dex_job,           interval=SCAN_INTERVAL,         first=20)
    app.job_queue.run_repeating(fav_job,           interval=FAV_CHECK_INTERVAL,    first=90)

    logger.info("IMPULSE BSC v5 starting — RPC reserve monitoring active")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
