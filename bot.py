"""
IMPULSE BSC SCANNER v5.1
All v5 capabilities retained plus:
- /radar command: near-miss tokens (scanned but not alerted, with reasons)
- RugDoc honeypot check as second opinion
- Liquidity lock detection (UNCX, PinkLock, burn/dead)
- Dev wallet age check (flag brand new wallets)
- Holder growth rate tracking (new holders/hr)
- Expanded narrative detection (20+ categories)
- Full alert redesign — clean, modern, compact, button-driven
- Fixed: on-chain wakeup label only for tokens >24h old
- Fixed: new launch thresholds restored
- Improved security section
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

# Known liquidity lock contract addresses on BSC
LIQ_LOCK_CONTRACTS = {
    "0xc765bddb93b0d1c1a88282ba0fa6b2d00e3e0c83": "UNCX",
    "0x7ee058420e5937496f5a2096f04caa7721cf70cc": "PinkLock",
    "0xdba68f07d1b7ca219f78ae8582c213d975c25caf": "Mudra",
    "0x000000000000000000000000000000000000dead": "Burned",
    "0x0000000000000000000000000000000000000000": "Zero addr",
}

# ── THRESHOLDS ────────────────────────────────────────────────────────────────
MIN_LIQ_MICRO        = 1_500
MIN_LIQ_STD          = 4_000

MIN_SCORE_NEW        = 35
MIN_SCORE_WAKE       = 40
MIN_SCORE_STD        = 50
NEAR_MISS_MIN_SCORE  = 25    # track tokens with score >= this even if not alerted

NEW_PAIR_MAX_AGE_H   = 72
ONCHAIN_WAKE_MIN_AGE_H = 24  # must be older than 24h to be called "on-chain wakeup"
WAKE_PREV_MAX_VOL    = 20_000
WAKE_MIN_VOL         = 2_000
WAKE_SPIKE_MULT      = 1.8

MAX_SELL_RATIO       = 0.75
MIN_LIQ_MCAP         = 0.04
MAX_DROP_1H          = -40

FAV_ALERT_PCT        = 50
NEAR_MISS_MAX        = 50    # keep last 50 near-misses
DEV_WALLET_NEW_DAYS  = 30    # flag dev wallet if under 30 days old

RESERVE_CHANGE_PCT   = 3.0
RESERVE_MIN_STABLE   = 4
RESERVE_BATCH_SIZE   = 60
DB_BUILD_BATCH       = 100
DB_LOOKBACK_DAYS     = 30
BSC_BLOCKS_PER_DAY   = 28_800

SCAN_INTERVAL         = 60
RPC_NEW_PAIR_INTERVAL = 30
RESERVE_SCAN_INTERVAL = 45
DB_BUILD_INTERVAL     = 300
FAV_CHECK_INTERVAL    = 120

# ── MC LABELS ─────────────────────────────────────────────────────────────────
def mc_label(mc: float) -> str:
    if mc <= 0:          return ""
    if mc < 20_000:      return "🔬 MICRO"
    if mc < 100_000:     return "💎 LOW"
    if mc < 200_000:     return "📊 LOW-MID"
    if mc < 1_000_000:   return "📈 MID CAP"
    if mc < 20_000_000:  return "🔥 HIGH CAP"
    return "🏆 VERY HIGH"

# ── STATE ─────────────────────────────────────────────────────────────────────
subscribed_chats = set()
alerted_tokens   = {}     # addr -> {ts, price}
token_history    = {}     # addr -> snapshot
holder_history   = {}     # addr -> deque of (timestamp, count) for growth rate
seen_new_pairs   = set()
favourites       = {}     # chat_id -> {addr -> info}
pinned_msg_ids   = {}     # chat_id -> message_id
near_miss_log    = deque(maxlen=NEAR_MISS_MAX)  # near-miss tokens

pair_database    = {}     # pair_addr -> token_addr
pair_reserves    = {}     # pair_addr -> reserve snapshot
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
        d  = result[2:]
        r0 = int(d[0:64], 16)
        r1 = int(d[64:128], 16)
        return r0, r1
    except Exception:
        return None


async def rpc_get_logs(session, from_block: int, to_block: int) -> list:
    params = [{"fromBlock": hex(from_block), "toBlock": hex(to_block),
               "address": PANCAKE_V2_FACTORY, "topics": [PAIR_CREATED_TOPIC]}]
    result = await rpc_call(session, "eth_getLogs", params)
    return result if isinstance(result, list) else []


async def rpc_balance_of(session, token: str, wallet: str) -> Optional[int]:
    padded = wallet.replace("0x", "").zfill(64)
    result = await rpc_call(session, "eth_call", [{"to": token, "data": f"0x70a08231{padded}"}, "latest"])
    if result and result != "0x":
        try: return int(result, 16)
        except Exception: pass
    return None


async def rpc_total_supply(session, token: str) -> Optional[int]:
    result = await rpc_call(session, "eth_call", [{"to": token, "data": "0x18160ddd"}, "latest"])
    if result and result != "0x":
        try: return int(result, 16)
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

    # Re-check sleeping tokens
    sleeping = [
        addr for addr, h in token_history.items()
        if h.get("vol_1h", 0) < WAKE_PREV_MAX_VOL and addr.lower() not in seen
    ][:40]
    if sleeping:
        r2 = await asyncio.gather(*[dex_token(session, a) for a in sleeping], return_exceptions=True)
        for pair in r2:
            if pair and not isinstance(pair, Exception):
                addr = pair.get("baseToken", {}).get("address", "")
                if addr and addr.lower() not in seen:
                    seen.add(addr.lower())
                    results.append(pair)

    logger.info(f"DexScreener: {len(results)} pairs")
    return results


# ── SECURITY & DATA ───────────────────────────────────────────────────────────

async def honeypot_check(session, addr: str) -> dict:
    return await http_get(session, f"https://api.honeypot.is/v2/IsHoneypot?address={addr}&chainID=56") or {}


async def rugdoc_check(session, addr: str) -> dict:
    """RugDoc honeypot check as second opinion"""
    data = await http_get(session, f"https://rugdoc.io/api/honeypot-v2.php?address={addr}&chain=bsc")
    return data or {}


async def goplus_check(session, addr: str) -> dict:
    data = await http_get(session, f"https://api.gopluslabs.io/api/v1/token_security/56?contract_addresses={addr}")
    if data:
        r = data.get("result", {})
        if r:
            return list(r.values())[0]
    return {}


async def get_top_holders(session, addr: str) -> list:
    url = (f"{BSCSCAN_URL}?module=token&action=tokenholderlist"
           f"&contractaddress={addr}&page=1&offset=15&apikey={BSCSCAN_KEY}")
    data = await http_get(session, url)
    return data.get("result", []) if data and data.get("status") == "1" else []


async def get_deployer(session, addr: str) -> Optional[str]:
    url = (f"{BSCSCAN_URL}?module=contract&action=getcontractcreation"
           f"&contractaddresses={addr}&apikey={BSCSCAN_KEY}")
    data = await http_get(session, url)
    if data and data.get("status") == "1" and data.get("result"):
        return data["result"][0].get("contractCreator")
    return None


async def get_wallet_age_days(session, wallet: str) -> Optional[float]:
    """Get wallet age in days by finding its first transaction"""
    url = (f"{BSCSCAN_URL}?module=account&action=txlist"
           f"&address={wallet}&startblock=0&endblock=99999999"
           f"&page=1&offset=1&sort=asc&apikey={BSCSCAN_KEY}")
    data = await http_get(session, url)
    if data and data.get("status") == "1" and data.get("result"):
        first_tx = data["result"][0]
        ts = int(first_tx.get("timeStamp", 0))
        if ts:
            age_days = (time.time() - ts) / 86400
            return round(age_days, 1)
    return None


async def check_liq_lock(session, pair_addr: str) -> dict:
    """
    Check if LP tokens for a pair have been sent to known lock contracts.
    Uses BSCScan token transfer events on the pair address.
    """
    url = (f"{BSCSCAN_URL}?module=account&action=tokentx"
           f"&address={pair_addr}&page=1&offset=50&sort=desc&apikey={BSCSCAN_KEY}")
    data = await http_get(session, url)
    if not data or data.get("status") != "1":
        # Also try UNCX API directly
        uncx = await http_get(session, f"https://api.uncx.network/api/v1/locks/bsc/token/{pair_addr}")
        if uncx and uncx.get("records"):
            return {"locked": True, "protocol": "UNCX", "amount_pct": None}
        return {"locked": False, "protocol": None, "amount_pct": None}

    txns = data.get("result", [])
    for tx in txns:
        to_addr = tx.get("to", "").lower()
        if to_addr in LIQ_LOCK_CONTRACTS:
            protocol = LIQ_LOCK_CONTRACTS[to_addr]
            return {"locked": True, "protocol": protocol, "amount_pct": None}

    return {"locked": False, "protocol": None, "amount_pct": None}


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
        "mint(":        ("Mintable", 25),
        "blacklist":    ("Blacklist", 20),
        "setfee":       ("Changeable fees", 15),
        "pause()":      ("Pausable", 20),
        "selfdestruct": ("Selfdestruct", 35),
        "delegatecall": ("Dangerous proxy", 25),
    }
    src = source.lower()
    for k, (msg, p) in checks.items():
        if k in src:
            flags.append(msg)
            score -= p
    return {"verified": True, "flags": flags, "score": max(0, score)}


async def get_dev_holding(session, token_addr, deployer, total_supply) -> Optional[float]:
    if not deployer or not total_supply or total_supply == 0:
        return None
    balance = await rpc_balance_of(session, token_addr, deployer)
    if balance is None:
        return None
    return (balance / total_supply) * 100


def track_holder_growth(addr: str, holder_count: Optional[int]) -> Optional[float]:
    """
    Track holder count over time and return growth rate (new holders/hr).
    Uses a rolling window of observations.
    """
    if not holder_count:
        return None
    if addr not in holder_history:
        holder_history[addr] = deque(maxlen=10)
    holder_history[addr].append((time.time(), holder_count))

    history = holder_history[addr]
    if len(history) < 2:
        return None

    oldest_ts, oldest_count = history[0]
    latest_ts, latest_count = history[-1]
    elapsed_hrs = (latest_ts - oldest_ts) / 3600

    if elapsed_hrs < 0.01:
        return None

    growth_per_hr = (latest_count - oldest_count) / elapsed_hrs
    return round(growth_per_hr, 1)


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
        return True, f"{round(sells/total*100)}% sells"
    if v1h > 3000 and c1h < MAX_DROP_1H:
        return True, f"Dumping {c1h}%"
    if mc > 0 and liq > 0 and liq / mc < MIN_LIQ_MCAP:
        return True, f"Liq {round(liq/mc*100,1)}% of mcap"
    if liq > 0 and v1h > liq * 15:
        return True, "Wash trading"
    if c6h > 500 and c1h < -20:
        return True, f"Already pumped {c6h}%"
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
        return True, f"First volume after silence — ${v1h:,.0f}/hr"
    if v1h >= prev_vol * WAKE_SPIKE_MULT:
        m = round(v1h / prev_vol, 1)
        return True, f"Volume {m}x spike — ${prev_vol:,.0f} → ${v1h:,.0f}/hr"
    return False, ""


# ── SCORING ───────────────────────────────────────────────────────────────────

def score_token(pair_data: dict, prev: Optional[dict], is_waking: bool) -> dict:
    score = 0
    signals = []
    try:
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
            score += 30; signals.append("Waking after silence")

        if v1h > 0 and v6h > 0:
            avg = v6h / 6
            if avg > 0:
                m = v1h / avg
                if m >= 4:    score += 20; signals.append(f"Vol {round(m,1)}x 6h avg")
                elif m >= 2:  score += 12; signals.append(f"Vol {round(m,1)}x 6h avg")
                elif m >= 1.3: score += 6; signals.append(f"Vol picking up {round(m,1)}x")

        t1h = b1h + s1h
        if t1h > 3:
            bp = b1h / t1h
            if bp >= 0.68:   score += 18; signals.append(f"{round(bp*100)}% buy pressure")
            elif bp >= 0.55: score += 10; signals.append(f"{round(bp*100)}% buy pressure")

        up = sum([c5m > 0, c1h > 0, c6h > 0])
        if up == 3:   score += 14; signals.append("Uptrend all TFs")
        elif up == 2: score += 7;  signals.append("Uptrend 2/3 TFs")

        if mc > 0 and v24h > 0:
            r = v24h / mc
            if r > 1.0:   score += 14; signals.append(f"Vol/MCap {round(r*100)}%")
            elif r > 0.3: score += 7;  signals.append(f"Vol/MCap {round(r*100)}%")

        if b5m >= 8:   score += 10; signals.append(f"{b5m} buys/5m")
        elif b5m >= 3: score += 5;  signals.append(f"{b5m} buys/5m")

        if 2000 <= v1h <= 60000:
            score += 8; signals.append("Early vol zone")

    except Exception as e:
        logger.warning(f"Score err: {e}")

    return {"score": min(100, score), "signals": signals}


def virality(pair_data: dict) -> dict:
    score = 0; signals = []
    b5m   = int(pair_data.get("txns", {}).get("m5", {}).get("buys", 0) or 0)
    b1h   = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    v5m   = float(pair_data.get("volume", {}).get("m5", 0) or 0)
    boost = pair_data.get("boosts", {}).get("active", 0) or 0

    if b5m >= 15:  score += 28; signals.append(f"{b5m} buys/5m 🔥")
    elif b5m >= 7: score += 15; signals.append(f"{b5m} buys/5m")
    if b1h >= 100: score += 28; signals.append(f"{b1h} buys/1h 📣")
    elif b1h >= 40: score += 15; signals.append(f"{b1h} buys/1h")
    if v5m > 10000: score += 22; signals.append(f"${v5m:,.0f} in 5m 💸")
    elif v5m > 3000: score += 10; signals.append(f"${v5m:,.0f} in 5m")
    if boost: score += 22; signals.append(f"DexScreener boosted ({boost})")

    return {"score": min(100, score), "signals": signals}


# ── NARRATIVE DETECTION ───────────────────────────────────────────────────────

NARRATIVES = {
    "🤖 AI / Agents":    ["ai", "agent", "gpt", "llm", "neural", "deepseek", "openai", "claude", "gemini", "robot", "agi", "sentient", "compute"],
    "🐸 Meme":           ["pepe", "doge", "shib", "inu", "cat", "frog", "moon", "wojak", "chad", "based", "bonk", "floki", "turbo", "cope", "gigachad", "noot"],
    "🏛️ Political":      ["trump", "maga", "elon", "biden", "vote", "president", "potus", "america", "kamala", "musk", "freedom"],
    "🎮 GameFi":         ["game", "play", "nft", "metaverse", "quest", "guild", "arena", "battle", "rpg", "p2e", "gaming"],
    "💰 DeFi":           ["defi", "yield", "farm", "stake", "swap", "vault", "lp", "liquidity", "borrow", "lend"],
    "🐂 BSC Native":     ["bnb", "binance", "pancake", "cake", "bsc"],
    "🌍 RWA":            ["rwa", "gold", "property", "real estate", "asset", "silver", "commodity"],
    "🔬 DeSci":          ["science", "research", "bio", "health", "dna", "molecule", "lab", "pharma"],
    "🏗️ DePIN":          ["depin", "infrastructure", "network", "node", "sensor", "iot", "device"],
    "💬 SocialFi":       ["social", "friend", "follow", "post", "creator", "fan", "community", "dao"],
    "🏃 Move-to-Earn":   ["run", "move", "step", "fit", "sport", "walk", "exercise", "health"],
    "🎨 NFT/Culture":    ["art", "culture", "music", "artist", "mint", "collection", "rare", "pixel"],
    "🌙 Space/Cosmic":   ["space", "moon", "rocket", "star", "galaxy", "mars", "nasa", "cosmos", "astro"],
    "🐉 Anime/Japan":    ["anime", "manga", "japan", "ninja", "samurai", "waifu", "kawaii", "otaku"],
    "🦊 Animal":         ["dog", "cat", "bear", "bull", "fox", "wolf", "tiger", "panda", "ape", "monkey", "hamster"],
    "💊 Degen":          ["degen", "gamble", "casino", "lottery", "bet", "risk", "yolo", "ape"],
    "🔮 Mystical":       ["magic", "wizard", "dragon", "witch", "dark", "demon", "ghost", "spirit", "soul"],
    "⚡ Layer2/Infra":   ["layer2", "l2", "bridge", "chain", "rollup", "zk", "optimism", "scaling"],
    "🍕 Food/Fun":       ["pizza", "burger", "food", "cook", "eat", "coffee", "beer", "wine", "sushi"],
    "🇺🇸 Patriotic":     ["eagle", "liberty", "patriot", "flag", "nation", "usa", "republic"],
}

def detect_narrative(name: str, symbol: str) -> list:
    text = f"{name} {symbol}".lower()
    found = []
    for label, keywords in NARRATIVES.items():
        if any(k in text for k in keywords):
            found.append(label)
    return found[:4]  # cap at 4 narratives


# ── RISK ──────────────────────────────────────────────────────────────────────

def risk_label(is_hp, cscore, top10, gp) -> tuple:
    """Returns (label, emoji, level 0-3)"""
    if is_hp:
        return "HONEYPOT — DO NOT BUY", "🔴", 3
    red_flags = sum([
        gp.get("can_take_back_ownership","0")=="1",
        gp.get("is_proxy","0")=="1",
        gp.get("hidden_owner","0")=="1",
    ])
    if red_flags >= 2 or cscore < 40 or top10 > 85:
        return "HIGH RISK", "🔴", 3
    if red_flags == 1 or cscore < 65 or top10 > 65:
        return "MEDIUM RISK", "🟡", 2
    owner = gp.get("owner_address", "")
    if owner == "0x0000000000000000000000000000000000000000" and cscore >= 75:
        return "LOW RISK", "🟢", 1
    return "MEDIUM RISK", "🟡", 2


def predict(pair_data, acc, waking, dump) -> str:
    if dump: return "AVOID"
    c1h = float(pair_data.get("priceChange", {}).get("h1", 0) or 0)
    c6h = float(pair_data.get("priceChange", {}).get("h6", 0) or 0)
    if waking and acc >= 60: return "HIGH POTENTIAL"
    if waking: return "EARLY SIGNAL"
    if acc >= 75 and c1h > 0 and c6h > 0: return "STRONG BULLISH"
    if acc >= 55 and c1h > 0: return "BULLISH"
    if acc >= 40: return "WATCH"
    return "NEUTRAL"


# ── ALERT BUILDER ─────────────────────────────────────────────────────────────

async def build_report(
    session,
    pair_data: dict,
    is_waking: bool = False,
    wake_signal: str = "",
    is_new: bool = False,
    new_age_str: str = "",
    reserve_detected: bool = False,
    token_age_h: float = 0,
) -> tuple:
    """
    Returns (text, InlineKeyboardMarkup)
    Clean modern layout — compact, scannable, button-driven.
    """
    addr      = pair_data.get("baseToken", {}).get("address", "")
    name      = pair_data.get("baseToken", {}).get("name", "Unknown")
    symbol    = pair_data.get("baseToken", {}).get("symbol", "???")
    pair_addr = pair_data.get("pairAddress", "")
    dex_name  = pair_data.get("dexId", "BSC").replace("-", " ").title()
    dex_url   = pair_data.get("url", f"https://dexscreener.com/bsc/{pair_addr}")
    price_str = pair_data.get("priceUsd", "N/A")
    mc        = float(pair_data.get("marketCap", 0) or 0)
    liq       = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)
    v5m       = float(pair_data.get("volume", {}).get("m5", 0) or 0)
    v1h       = float(pair_data.get("volume", {}).get("h1", 0) or 0)
    v24h      = float(pair_data.get("volume", {}).get("h24", 0) or 0)
    c5m       = pair_data.get("priceChange", {}).get("m5", "0")
    c1h_s     = pair_data.get("priceChange", {}).get("h1", "0")
    c6h_s     = pair_data.get("priceChange", {}).get("h6", "0")
    c24h      = pair_data.get("priceChange", {}).get("h24", "0")
    b1h       = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    s1h       = int(pair_data.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
    b5m_c     = int(pair_data.get("txns", {}).get("m5", {}).get("buys", 0) or 0)
    s5m_c     = int(pair_data.get("txns", {}).get("m5", {}).get("sells", 0) or 0)

    ts = pair_data.get("pairCreatedAt")
    if ts:
        ah  = (time.time() - int(ts)/1000) / 3600
        if ah < 1:    age = f"{round(ah*60)}m"
        elif ah < 48: age = f"{round(ah,1)}h"
        else:         age = f"{round(ah/24,1)}d"
    else:
        ah = token_age_h
        age = f"{round(ah/24,1)}d" if ah > 48 else f"{round(ah,1)}h"

    t1h = b1h + s1h
    bp  = round(b1h/t1h*100) if t1h > 0 else 0
    lm  = round(liq/mc*100, 1) if mc > 0 else 0
    mclbl = mc_label(mc)

    prev = token_history.get(addr)
    acc  = score_token(pair_data, prev, is_waking)
    vir  = virality(pair_data)

    token_history[addr] = {
        "vol_1h": v1h, "vol_24h": v24h, "price": price_str,
        "ticker": symbol, "mcap": mc, "timestamp": time.time(),
        "scan_count": (prev.get("scan_count",0) if prev else 0) + 1,
    }

    # Parallel data fetches
    fetched = await asyncio.gather(
        honeypot_check(session, addr),
        rugdoc_check(session, addr),
        get_contract_source(session, addr),
        goplus_check(session, addr),
        gmgn_holders(session, addr),
        get_top_holders(session, addr),
        get_deployer(session, addr),
        rpc_total_supply(session, addr),
        check_liq_lock(session, pair_addr),
        return_exceptions=True
    )

    hp_data, rd_data, src, gp_data, gmgn, top_holders, deployer, total_supply, liq_lock = [
        x if not isinstance(x, Exception) else None for x in fetched
    ]

    hp  = hp_data or {}
    rd  = rd_data or {}
    gp  = gp_data or {}
    src = src if isinstance(src, str) else ""
    top_holders  = top_holders  if isinstance(top_holders, list) else []
    deployer     = deployer     if isinstance(deployer, str)     else None
    total_supply = total_supply if isinstance(total_supply, int) else None
    gmgn         = gmgn         if isinstance(gmgn, int)         else None
    liq_lock     = liq_lock     if isinstance(liq_lock, dict)    else {"locked": False}

    is_hp     = hp.get("isHoneypot", False)
    buy_tax   = hp.get("simulationResult", {}).get("buyTax")
    sell_tax  = hp.get("simulationResult", {}).get("sellTax")

    # RugDoc second opinion
    rd_hp = rd.get("isHoneypot", rd.get("honeypot", ""))
    rd_status = ""
    if rd_hp in ("1", 1, True, "true"):
        rd_status = " · RugDoc: ⚠️ Flagged"
    elif rd_hp in ("0", 0, False, "false"):
        rd_status = " · RugDoc: ✅"

    contract  = analyze_contract(src)
    renounced     = gp.get("owner_address","") == "0x0000000000000000000000000000000000000000"
    is_mintable   = gp.get("is_mintable","0") == "1"
    is_proxy      = gp.get("is_proxy","0") == "1"
    hidden_owner  = gp.get("hidden_owner","0") == "1"
    can_take_back = gp.get("can_take_back_ownership","0") == "1"

    # Top 10 holders (BSCScan accurate)
    top10_pct = 0.0
    top10_str = "N/A"
    if top_holders:
        try:
            pcts = [float(h.get("percentage", 0) or 0) for h in top_holders[:10]]
            total_pct = sum(pcts)
            if total_pct > 0:
                top10_pct = total_pct
                top10_str = f"{round(total_pct, 1)}%"
            elif total_supply and total_supply > 0:
                quantities = [int(h.get("TokenHolderQuantity", 0) or 0) for h in top_holders[:10]]
                top10_pct = sum(quantities) / total_supply * 100
                top10_str = f"{round(top10_pct, 1)}%"
        except Exception:
            pass

    # Holder count + growth
    gp_holders = gp.get("holder_count")
    holder_sources = {}
    if gp_holders: holder_sources["GP"] = int(gp_holders)
    if gmgn:       holder_sources["GMGN"] = int(gmgn)
    best_holders = max(holder_sources.values()) if holder_sources else None
    growth_rate  = track_holder_growth(addr, best_holders)

    holders_str = f"{best_holders:,}" if best_holders else "N/A"
    if growth_rate and growth_rate > 0:
        holders_str += f"  (+{round(growth_rate)}/hr)"
    elif growth_rate and growth_rate < 0:
        holders_str += f"  ({round(growth_rate)}/hr 📉)"

    # Dev holding + wallet age
    dev_pct_str = "N/A"
    dev_age_str = ""
    if deployer and total_supply:
        dev_pct = await get_dev_holding(session, addr, deployer, total_supply)
        dev_age = await get_wallet_age_days(session, deployer)
        if dev_pct is not None:
            if dev_pct == 0:
                dev_pct_str = "0% (sold/burned)"
            else:
                flag = " 🔴" if dev_pct > 20 else (" ⚠️" if dev_pct > 10 else " ✅")
                dev_pct_str = f"{round(dev_pct, 1)}%{flag}"
        if dev_age is not None:
            if dev_age < DEV_WALLET_NEW_DAYS:
                dev_age_str = f"  ⚠️ New wallet ({round(dev_age)}d old)"
            else:
                dev_age_str = f"  ({round(dev_age)}d old)"

    # Liq lock
    if liq_lock.get("locked"):
        lock_str = f"🔒 Locked ({liq_lock['protocol']})"
    else:
        lock_str = "🔓 Not confirmed locked"

    # Risk
    risk_text, risk_emoji, risk_level = risk_label(is_hp, contract["score"], top10_pct, gp)

    # Narratives
    narratives = detect_narrative(name, symbol)
    narr_str   = "  ·  ".join(narratives) if narratives else "None"

    # Prediction
    dump_flag, dump_reason = is_dump(pair_data)
    pred = predict(pair_data, acc["score"], is_waking, dump_flag)

    # Alert type — strict age gate for on-chain wakeup
    if reserve_detected and ah >= ONCHAIN_WAKE_MIN_AGE_H:
        atype  = "⛓  ON-CHAIN WAKEUP"
        aemoji = "💤🔥"
    elif is_new or ah < ONCHAIN_WAKE_MIN_AGE_H:
        atype  = "🆕  NEW LAUNCH"
        aemoji = "🆕"
    elif is_waking:
        atype  = "💤🔥  SLEEPING GIANT"
        aemoji = "💤🔥"
    else:
        atype  = "📡  ACCUMULATION"
        aemoji = "📡"

    # Price change arrows
    def arrow(v):
        try:
            return "▲" if float(v) >= 0 else "▼"
        except Exception:
            return ""

    # Contract flags as compact string
    cflags = " · ".join(contract["flags"]) if contract["flags"] else "Clean"
    tax_str = (
        f"Buy {buy_tax}% / Sell {sell_tax}%"
        if buy_tax is not None and sell_tax is not None
        else "Unknown"
    )
    dex_paid = "✅ Paid" if pair_data.get("boosts", {}).get("active", 0) else "❌ Unpaid"
    boosted  = pair_data.get("boosts", {}).get("active", 0)

    # ── COMPACT MODERN LAYOUT ──────────────────────────────────────────────────
    msg = (
        f"{aemoji} *{atype}*\n"
        f"*{name}*  `${symbol}`  ·  {mclbl}\n"
        f"{dex_name}  ·  {age} old  ·  `{addr[:6]}...{addr[-4:]}`\n"
    )

    if wake_signal:
        msg += f"┄  _{wake_signal}_\n"

    msg += (
        f"\n"
        f"💰 *${price_str}*   {arrow(c1h_s)} {c1h_s}% _(1h)_  {arrow(c24h)} {c24h}% _(24h)_\n"
        f"📈 MCap `${mc:,.0f}`   💧 Liq `${liq:,.0f}` _{lock_str}_\n"
        f"📦 Vol  5m `${v5m:,.0f}`  ·  1h `${v1h:,.0f}`  ·  24h `${v24h:,.0f}`\n"
        f"🔄 Txns  5m {b5m_c}B/{s5m_c}S  ·  1h {b1h}B/{s1h}S  _(_{bp}%_ buys)_\n"
        f"\n"
        f"┄┄┄ *SIGNALS* ┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        f"⚡ Score *{acc['score']}/100*  ·  Viral *{vir['score']}/100*\n"
    )

    all_signals = acc["signals"] + vir["signals"]
    if all_signals:
        msg += "› " + "\n› ".join(all_signals[:5]) + "\n"

    msg += (
        f"\n"
        f"┄┄┄ *SECURITY* ┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        f"{risk_emoji} *{risk_text}*{rd_status}\n"
        f"🍯 Honeypot: {'🔴 YES — AVOID' if is_hp else '🟢 Clean'}  ·  Tax: {tax_str}\n"
        f"📄 {'✅ Verified' if contract['verified'] else '❌ Unverified'}  ·  "
        f"🔑 {'✅ Renounced' if renounced else '⚠️ Not renounced'}\n"
        f"🖨️ Mint {'🔴' if is_mintable else '✅'}  "
        f"Proxy {'🔴' if is_proxy else '✅'}  "
        f"HiddenOwner {'🔴' if hidden_owner else '✅'}\n"
        f"⚠️ Contract: {cflags}\n"
        f"👥 Holders: {holders_str}\n"
        f"🐋 Top 10: {top10_str}  ·  👨‍💻 Dev: {dev_pct_str}{dev_age_str}\n"
        f"💳 DEX: {dex_paid}{f'  ({boosted} boosts)' if boosted else ''}\n"
        f"\n"
        f"┄┄┄ *NARRATIVE* ┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        f"{narr_str}\n"
        f"\n"
        f"🤖 *{pred}*\n"
    )

    if dump_flag:
        msg += f"⛔ Dump signal: _{dump_reason}_\n"

    # Inline buttons
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("📊 Chart", url=dex_url),
        InlineKeyboardButton("🔍 BSCScan", url=f"https://bscscan.com/token/{addr}"),
        InlineKeyboardButton("⭐ Fav", callback_data=f"addfav_{addr}"),
    ]])

    return msg, keyboard


# ── NEAR-MISS LOGGER ──────────────────────────────────────────────────────────

def log_near_miss(pair_data: dict, acc: dict, reason_not_alerted: str):
    """Store tokens that were interesting but didn't make the alert threshold"""
    if acc["score"] < NEAR_MISS_MIN_SCORE:
        return

    addr   = pair_data.get("baseToken", {}).get("address", "")
    name   = pair_data.get("baseToken", {}).get("name", "?")
    symbol = pair_data.get("baseToken", {}).get("symbol", "???")
    mc     = float(pair_data.get("marketCap", 0) or 0)
    v1h    = float(pair_data.get("volume", {}).get("h1", 0) or 0)
    url    = pair_data.get("url", f"https://dexscreener.com/bsc/{addr}")

    # Don't duplicate
    existing = [x for x in near_miss_log if x.get("addr") == addr]
    if existing:
        return

    # Why is it interesting?
    interesting = []
    if acc["score"] >= 40: interesting.append(f"Score {acc['score']}/100")
    interesting.extend(acc["signals"][:3])

    near_miss_log.append({
        "addr":               addr,
        "name":               name,
        "symbol":             symbol,
        "mc":                 mc,
        "vol_1h":             v1h,
        "url":                url,
        "score":              acc["score"],
        "interesting":        interesting,
        "reason_not_alerted": reason_not_alerted,
        "ts":                 time.time(),
    })


# ── DB BUILDER ────────────────────────────────────────────────────────────────

async def build_pair_database(session):
    global pair_database
    current_block = await rpc_block_number(session)
    if not current_block:
        return
    lookback = DB_LOOKBACK_DAYS * BSC_BLOCKS_PER_DAY
    from_block = current_block - lookback
    existing = len(pair_database)

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
        added = 0
        for log in logs:
            parsed = parse_pair_log(log)
            if parsed:
                pa, ta = parsed
                if pa not in pair_database:
                    pair_database[pa] = ta
                    added += 1
        logger.info(f"DB page {page}: +{added} (total {len(pair_database)})")
        await asyncio.sleep(0.3)

    if len(pair_database) > existing:
        logger.info(f"Pair DB: {existing} → {len(pair_database)}")


# ── RESERVE MONITOR ───────────────────────────────────────────────────────────

async def scan_reserves(session, app: Application):
    global db_scan_pointer
    if not pair_database or not subscribed_chats:
        return

    pairs_list = list(pair_database.items())
    total = len(pairs_list)
    start = db_scan_pointer % total
    end   = min(start + RESERVE_BATCH_SIZE, total)
    batch = pairs_list[start:end]
    if len(batch) < RESERVE_BATCH_SIZE and total > RESERVE_BATCH_SIZE:
        batch += pairs_list[:RESERVE_BATCH_SIZE - len(batch)]
    db_scan_pointer = end % total

    flagged = []

    for pair_addr, token_addr in batch:
        try:
            reserves = await rpc_get_reserves(session, pair_addr)
            if not reserves:
                continue
            r0, r1 = reserves
            if r0 == 0 or r1 == 0:
                continue

            prev = pair_reserves.get(pair_addr)
            if not prev:
                pair_reserves[pair_addr] = {"r0": r0, "r1": r1, "stable_count": 0, "last_ts": time.time()}
                continue

            max_change = max(
                abs(r0 - prev["r0"]) / prev["r0"] * 100 if prev["r0"] > 0 else 0,
                abs(r1 - prev["r1"]) / prev["r1"] * 100 if prev["r1"] > 0 else 0,
            )

            if max_change < RESERVE_CHANGE_PCT:
                pair_reserves[pair_addr]["stable_count"] = min(prev["stable_count"] + 1, 200)
                pair_reserves[pair_addr].update({"r0": r0, "r1": r1, "last_ts": time.time()})
                continue

            if prev["stable_count"] >= RESERVE_MIN_STABLE:
                flagged.append((token_addr, pair_addr, prev["stable_count"], max_change))

            pair_reserves[pair_addr] = {"r0": r0, "r1": r1, "stable_count": 0, "last_ts": time.time()}
            await asyncio.sleep(0.05)

        except Exception as e:
            logger.warning(f"Reserve {pair_addr[:8]}: {e}")

    for token_addr, pair_addr, stable, change in flagged:
        try:
            if token_addr in alerted_tokens:
                continue

            pair_data = await dex_token(session, token_addr)
            if not pair_data:
                continue

            # Age gate — must be >24h for on-chain wakeup label
            ts = pair_data.get("pairCreatedAt")
            age_h = (time.time() - int(ts)/1000) / 3600 if ts else 999

            liq = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)
            mc  = float(pair_data.get("marketCap", 0) or 0)
            if liq < MIN_LIQ_MICRO:
                continue

            dump_flag, _ = is_dump(pair_data)
            if dump_flag:
                continue

            prev = token_history.get(token_addr)
            acc  = score_token(pair_data, prev, True)
            wake_signal = (
                f"Reserves moved {round(change,1)}% after "
                f"{stable} stable checks — first on-chain activity"
            )

            report, markup = await build_report(
                session, pair_data,
                is_waking=True, wake_signal=wake_signal,
                reserve_detected=True, token_age_h=age_h
            )

            price = float(pair_data.get("priceUsd") or 0)
            alerted_tokens[token_addr] = {"ts": time.time(), "price": price}
            name = pair_data.get("baseToken", {}).get("name", "?")
            logger.info(f"RESERVE ALERT: {name} mc=${mc:,.0f} age={round(age_h,1)}h")

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
            logger.warning(f"Reserve flagged processing: {e}")


# ── RPC NEW PAIR SCAN ─────────────────────────────────────────────────────────

async def rpc_scan_new_pairs(session, app: Application):
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

    logger.info(f"RPC: {len(logs)} new pairs blocks {from_block}-{to_block}")

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

            await asyncio.sleep(5)
            pair_data = await dex_token(session, token_addr)
            if not pair_data:
                token_history[token_addr] = {
                    "vol_1h": 0, "vol_24h": 0, "price": None,
                    "ticker": "???", "mcap": 0, "timestamp": time.time(), "scan_count": 0,
                }
                continue

            liq = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)
            if liq < MIN_LIQ_MICRO:
                continue

            dump_flag, _ = is_dump(pair_data)
            prev = token_history.get(token_addr)
            acc  = score_token(pair_data, prev, False)

            if dump_flag or acc["score"] < MIN_SCORE_NEW:
                log_near_miss(pair_data, acc,
                    "Dump pattern" if dump_flag else f"Score {acc['score']} < {MIN_SCORE_NEW}")
                continue

            ts = pair_data.get("pairCreatedAt")
            age_h = (time.time() - int(ts)/1000) / 3600 if ts else 0
            new_age_str = f"{round(age_h*60)}m old" if age_h < 1 else f"{round(age_h,1)}h old"

            seen_new_pairs.add(token_addr)
            mc = float(pair_data.get("marketCap", 0) or 0)
            name = pair_data.get("baseToken", {}).get("name", "?")
            logger.info(f"RPC NEW: {name} mc=${mc:,.0f} score={acc['score']}")

            report, markup = await build_report(
                session, pair_data, is_new=True, new_age_str=new_age_str, token_age_h=age_h
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

            min_liq = MIN_LIQ_MICRO if mc <= 200_000 else MIN_LIQ_STD

            prev = token_history.get(addr)
            token_history[addr] = {
                "vol_1h": v1h, "vol_24h": v24h,
                "price": pair_data.get("priceUsd"),
                "ticker": pair_data.get("baseToken", {}).get("symbol", "???"),
                "mcap": mc, "timestamp": time.time(),
                "scan_count": (prev.get("scan_count",0) if prev else 0) + 1,
            }

            dump_flag, dump_reason = is_dump(pair_data)
            acc = score_token(pair_data, prev, False)

            if liq < min_liq:
                if acc["score"] >= NEAR_MISS_MIN_SCORE:
                    log_near_miss(pair_data, acc, f"Liq ${liq:,.0f} < ${min_liq:,}")
                continue

            if dump_flag:
                if acc["score"] >= NEAR_MISS_MIN_SCORE:
                    log_near_miss(pair_data, acc, f"Dump: {dump_reason}")
                continue

            ts = pair_data.get("pairCreatedAt")
            age_h = (time.time() - int(ts)/1000) / 3600 if ts else 999

            is_new_flag = False
            new_age_str = ""
            if ts and age_h <= NEW_PAIR_MAX_AGE_H and addr not in seen_new_pairs:
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
                if acc["score"] >= NEAR_MISS_MIN_SCORE:
                    log_near_miss(pair_data, acc, f"Score {acc['score']} below threshold")
                continue

            already  = addr in alerted_tokens
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
            logger.info(f"ALERT {name} score={acc['score']} mc=${mc:,.0f}")

            report, markup = await build_report(
                session, pair_data, is_waking, wake_signal, is_new_flag, new_age_str, token_age_h=age_h
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
                            text=f"⭐ *{ticker}*  moved {d} *{c1h}%* in 1h\n💰 ${price}\n[Chart]({url})",
                            parse_mode=ParseMode.MARKDOWN,
                            disable_web_page_preview=True
                        )
                except Exception as e:
                    logger.warning(f"Fav move {addr}: {e}")


# ── CALLBACK ──────────────────────────────────────────────────────────────────

async def handle_addfav_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        await query.answer("Already in Favlist ⭐", show_alert=False)
        return

    ticker = token_history.get(addr, {}).get("ticker", "???")
    price  = float(token_history.get(addr, {}).get("price") or 0)
    favourites[chat_id][addr] = {"ticker": ticker, "added_ts": time.time(), "last_price": price}
    await query.answer(f"⭐ Added ${ticker} to Favlist!", show_alert=False)

    try:
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("📊 Chart", url=f"https://dexscreener.com/bsc/{addr}"),
            InlineKeyboardButton("🔍 BSCScan", url=f"https://bscscan.com/token/{addr}"),
            InlineKeyboardButton("⭐ In Fav ✅", callback_data=f"addfav_{addr}"),
        ]]))
    except Exception:
        pass

    await update_pinned_favs(context.application, chat_id)


# ── FAVOURITES ────────────────────────────────────────────────────────────────

async def build_fav_board(session, chat_id: int) -> str:
    favs = favourites.get(chat_id, {})
    if not favs:
        return "⭐ *FAVOURITES*\n\nEmpty — tap ⭐ on any alert to add."
    lines = ["⭐ *FAVOURITES*\n"]
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
            e      = "🟢" if float(c1h or 0) >= 0 else "🔴"
            favs[addr]["ticker"] = ticker
            lines.append(
                f"[*${ticker}*]({url})  {mc_label(mc)}\n"
                f"💰 ${price}   MCap `${mc:,.0f}`\n"
                f"{e} {c1h}% _(1h)_  ·  {c24h}% _(24h)_\n"
                f"📦 Vol `${v1h:,.0f}`   💧 Liq `${liq:,.0f}`\n"
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
            await app.bot.pin_chat_message(chat_id=chat_id, message_id=msg.message_id, disable_notification=True)
    except Exception as e:
        logger.warning(f"Pin update: {e}")


# ── COMMANDS ──────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    subscribed_chats.add(chat_id)
    await context.bot.set_my_commands([
        BotCommand("start",     "Activate scanner"),
        BotCommand("stop",      "Pause alerts"),
        BotCommand("scan",      "Scan any BSC token"),
        BotCommand("radar",     "Near-miss tokens worth a look"),
        BotCommand("watchlist", "Tracked tokens by MCap"),
        BotCommand("fav",       "Add token to favourites"),
        BotCommand("unfav",     "Remove from favourites"),
        BotCommand("favlist",   "Live prices for favourites"),
        BotCommand("status",    "Scanner stats"),
        BotCommand("filters",   "Current settings"),
    ])
    await update.message.reply_text(
        "🟡 *IMPULSE BSC v5.1*\n\n"
        "⛓ On-chain reserve monitoring — catches old tokens before DexScreener\n"
        "🆕 Direct blockchain new pair detection\n"
        "📡 DexScreener accumulation signals\n"
        "📻 /radar — near-miss tokens you might want to check\n\n"
        "One alert per token · Tap ⭐ to add to Favlist\n"
        "Favlist tokens re-alert on ±50% moves\n\n"
        "↓ Menu below",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    subscribed_chats.discard(update.effective_chat.id)
    await update.message.reply_text("⏸ Paused. /start to resume.")


async def cmd_radar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Near-miss tokens — scanned but not alerted"""
    if not near_miss_log:
        await update.message.reply_text(
            "📻 *RADAR — NEAR MISSES*\n\nNothing tracked yet. Give it a few scan cycles.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # Sort by score desc, show last 15
    sorted_misses = sorted(near_miss_log, key=lambda x: x.get("score", 0), reverse=True)[:15]
    lines = ["📻 *RADAR — TOKENS WORTH A LOOK*\n_(Scanned but not alerted)_\n"]

    for t in sorted_misses:
        age_mins = round((time.time() - t["ts"]) / 60)
        age_str  = f"{age_mins}m ago" if age_mins < 60 else f"{round(age_mins/60,1)}h ago"
        mc_str   = f"${t['mc']:,.0f}" if t['mc'] else "N/A"
        why_interesting  = " · ".join(t.get("interesting", []))
        why_not_alerted  = t.get("reason_not_alerted", "Below threshold")

        lines.append(
            f"[*${t['symbol']}*  {t['name']}]({t['url']})  ·  {age_str}\n"
            f"MCap `{mc_str}`   Score `{t['score']}/100`\n"
            f"💡 _{why_interesting}_\n"
            f"⛔ _{why_not_alerted}_\n"
            f"`{t['addr']}`\n"
        )

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    fav_count = sum(len(v) for v in favourites.values())
    await update.message.reply_text(
        f"*IMPULSE BSC v5.1*\n\n"
        f"✅ Running\n"
        f"⛓ Pair DB: `{len(pair_database):,}` pairs\n"
        f"📊 Reserve snapshots: `{len(pair_reserves):,}`\n"
        f"🪙 Tokens in memory: `{len(token_history):,}`\n"
        f"📻 Radar queue: `{len(near_miss_log)}`\n"
        f"🔔 Alerted: `{len(alerted_tokens):,}`\n"
        f"⭐ Favourites: `{fav_count}`\n"
        f"⛓ Last block: `{last_rpc_block:,}`",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_filters(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"*IMPULSE v5.1 FILTERS*\n\n"
        f"⛓ Reserve change: {RESERVE_CHANGE_PCT}% · Min stable: {RESERVE_MIN_STABLE}\n"
        f"🕐 On-chain wakeup min age: {ONCHAIN_WAKE_MIN_AGE_H}h\n\n"
        f"🆕 New pairs (≤{NEW_PAIR_MAX_AGE_H}h): score ≥{MIN_SCORE_NEW}\n"
        f"💤 Sleeping giant: score ≥{MIN_SCORE_WAKE}\n"
        f"📡 Standard: score ≥{MIN_SCORE_STD}\n"
        f"📻 Radar (near-miss): score ≥{NEAR_MISS_MIN_SCORE}\n\n"
        f"💧 Min liq micro: ${MIN_LIQ_MICRO:,} · STD: ${MIN_LIQ_STD:,}\n"
        f"⛔ Max sells: {round(MAX_SELL_RATIO*100)}% · Max drop: {MAX_DROP_1H}%\n"
        f"⭐ Fav re-alert: ±{FAV_ALERT_PCT}%\n"
        f"👛 Dev wallet new threshold: <{DEV_WALLET_NEW_DAYS}d\n\n"
        f"*MC Labels*\n"
        f"🔬 <$20k  💎 $20k–100k  📊 $100k–200k\n"
        f"📈 $200k–1m  🔥 $1m–20m  🏆 $20m+",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not token_history:
        await update.message.reply_text("No tokens in memory yet — give it a few minutes.")
        return
    sorted_tokens = sorted(token_history.items(), key=lambda x: x[1].get("mcap", 0), reverse=True)[:20]
    lines = ["*TOP 20 TOKENS BY MCAP*\n"]
    for addr, h in sorted_tokens:
        mc     = h.get("mcap", 0)
        scans  = h.get("scan_count", 0)
        ticker = h.get("ticker", "???")
        link   = f"https://dexscreener.com/bsc/{addr}"
        lines.append(
            f"[*${ticker}*]({link})  {mc_label(mc)}\n"
            f"`{addr}`  ·  MCap `${mc:,.0f}`  ·  Scans `{scans}`"
        )
    await update.message.reply_text(
        "\n\n".join(lines), parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True
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
    await msg.edit_text(f"⭐ Added *${ticker}*", parse_mode=ParseMode.MARKDOWN)
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
        await update.message.reply_text(f"Removed *${ticker}*", parse_mode=ParseMode.MARKDOWN)
        await update_pinned_favs(context.application, chat_id)
    else:
        await update.message.reply_text("Not in favourites.")


async def cmd_favlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not favourites.get(chat_id):
        await update.message.reply_text("No favourites. Tap ⭐ on an alert or /fav <address>.")
        return
    msg = await update.message.reply_text("⭐ Fetching...")
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
            await msg.edit_text("❌ Token not found.")
            return
        prev = token_history.get(addr)
        is_waking, wake_signal = is_sleeping_giant(pair_data, prev)
        ts = pair_data.get("pairCreatedAt")
        age_h = (time.time() - int(ts)/1000) / 3600 if ts else 0
        report, markup = await build_report(
            session, pair_data, is_waking, wake_signal, token_age_h=age_h
        )
    try:
        await msg.edit_text(report, parse_mode=ParseMode.MARKDOWN,
                            reply_markup=markup, disable_web_page_preview=True)
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
    app.add_handler(CallbackQueryHandler(handle_addfav_callback, pattern=r"^addfav_"))

    async def dex_job(ctx):
        async with aiohttp.ClientSession() as session:
            await run_scan(session, app)

    async def rpc_new_job(ctx):
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
    app.job_queue.run_repeating(rpc_new_job, interval=RPC_NEW_PAIR_INTERVAL, first=10)
    app.job_queue.run_repeating(reserve_job, interval=RESERVE_SCAN_INTERVAL, first=30)
    app.job_queue.run_repeating(dex_job,     interval=SCAN_INTERVAL,         first=20)
    app.job_queue.run_repeating(fav_job,     interval=FAV_CHECK_INTERVAL,    first=90)

    logger.info("IMPULSE BSC v5.1 starting")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
