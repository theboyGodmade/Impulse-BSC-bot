"""
IMPULSE BSC SCANNER v5.1
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Changes from v5:
  • Pending watchlist — tokens that almost qualified, /pending to view
  • RugDoc risk scoring integrated
  • Enhanced safety: holder count, top-10 breakdown, dev wallet age
  • Liquidity lock check — no alert without lock (unless locking in progress)
  • Dev wallet age flag — brand new deployer wallets flagged
  • Wallet growth rate — new holders/hr tracked and displayed
  • Improved narrative detection — weighted multi-signal scoring
  • Full alert + UI redesign — clean, modern, easy to read
  • Fixed alert type logic — new tokens never labeled as On-chain Wakeup
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

PANCAKE_V2_FACTORY = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"
PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
WBNB               = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
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

RESERVE_CHANGE_PCT   = 3.0
RESERVE_MIN_STABLE   = 4
RESERVE_BATCH_SIZE   = 60
DB_BUILD_BATCH       = 100
DB_LOOKBACK_DAYS     = 30
BSC_BLOCKS_PER_DAY   = 28_800

SCAN_INTERVAL        = 60
RPC_NEW_PAIR_INTERVAL = 30
RESERVE_SCAN_INTERVAL = 45
DB_BUILD_INTERVAL    = 300
FAV_CHECK_INTERVAL   = 120

# Pending watchlist — score must be at least this to be stored
MIN_SCORE_PENDING    = 20
MAX_PENDING          = 100   # cap stored entries
PENDING_TTL          = 3600 * 6  # drop after 6h

# Dev wallet age — flag if first tx is within this many days
DEV_NEW_WALLET_DAYS  = 30

# ── MC LABELS ─────────────────────────────────────────────────────────────────
def mc_label(mc: float) -> str:
    if mc <= 0:         return ""
    if mc < 20_000:     return "MICRO"
    if mc < 100_000:    return "LOW"
    if mc < 200_000:    return "LOW-MID"
    if mc < 1_000_000:  return "MID"
    if mc < 20_000_000: return "HIGH"
    return "VERY HIGH"

def mc_emoji(mc: float) -> str:
    if mc <= 0:         return "🔬"
    if mc < 20_000:     return "🔬"
    if mc < 100_000:    return "💎"
    if mc < 200_000:    return "📊"
    if mc < 1_000_000:  return "📈"
    if mc < 20_000_000: return "🔥"
    return "🏆"

# ── STATE ─────────────────────────────────────────────────────────────────────
subscribed_chats = set()
alerted_tokens   = {}
token_history    = {}
seen_new_pairs   = set()
favourites       = {}
pinned_msg_ids   = {}

pair_database    = {}
pair_reserves    = {}
db_scan_pointer  = 0
last_rpc_block   = 0

# Pending watchlist: addr -> {ticker, mc, ca, reason_interesting, reason_skipped, ts, url}
pending_tokens   = {}

# Holder snapshot for growth rate: addr -> [(timestamp, holder_count), ...]
holder_snapshots = {}


# ── RPC ───────────────────────────────────────────────────────────────────────

async def rpc_call(session, method, params):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    for ep in RPC_ENDPOINTS:
        try:
            async with session.post(ep, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    data = await r.json()
                    if "result" in data:
                        return data["result"]
        except Exception as e:
            logger.warning(f"RPC {ep[:40]}: {e}")
    return None

async def rpc_block_number(session):
    r = await rpc_call(session, "eth_blockNumber", [])
    return int(r, 16) if r else None

async def rpc_get_reserves(session, pair_addr):
    result = await rpc_call(session, "eth_call", [
        {"to": pair_addr, "data": GET_RESERVES_SELECTOR}, "latest"
    ])
    if not result or result == "0x" or len(result) < 194:
        return None
    try:
        data = result[2:]
        return int(data[0:64], 16), int(data[64:128], 16)
    except Exception:
        return None

async def rpc_get_logs(session, from_block, to_block):
    params = [{"fromBlock": hex(from_block), "toBlock": hex(to_block),
               "address": PANCAKE_V2_FACTORY, "topics": [PAIR_CREATED_TOPIC]}]
    result = await rpc_call(session, "eth_getLogs", params)
    return result if isinstance(result, list) else []

async def rpc_balance_of(session, token, wallet):
    padded = wallet.replace("0x", "").zfill(64)
    result = await rpc_call(session, "eth_call", [
        {"to": token, "data": f"0x70a08231{padded}"}, "latest"
    ])
    if result and result != "0x":
        try: return int(result, 16)
        except Exception: pass
    return None

async def rpc_total_supply(session, token):
    result = await rpc_call(session, "eth_call", [
        {"to": token, "data": "0x18160ddd"}, "latest"
    ])
    if result and result != "0x":
        try: return int(result, 16)
        except Exception: pass
    return None

def parse_pair_log(log):
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


# ── HTTP HELPER ───────────────────────────────────────────────────────────────

async def http_get(session, url, headers=None):
    try:
        async with session.get(url, headers=headers or {}, timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status == 200:
                return await r.json()
    except Exception as e:
        logger.warning(f"HTTP GET {url[:60]}: {e}")
    return None


# ── DEXSCREENER ───────────────────────────────────────────────────────────────

async def dex_token(session, address):
    data = await http_get(session, f"https://api.dexscreener.com/latest/dex/tokens/{address}")
    if not data:
        return None
    pairs = [p for p in data.get("pairs", []) if p.get("chainId") == "bsc"]
    if pairs:
        return max(pairs, key=lambda x: float(x.get("liquidity", {}).get("usd", 0) or 0))
    return None

async def fetch_bsc_pairs(session):
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
            logger.warning(f"DexScreener batch error {i}: {e}")

    sleeping = [
        addr for addr, h in token_history.items()
        if h.get("vol_1h", 0) < WAKE_PREV_MAX_VOL and addr.lower() not in seen
    ][:40]
    if sleeping:
        res2 = await asyncio.gather(*[dex_token(session, a) for a in sleeping], return_exceptions=True)
        for pair in res2:
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
        r = data["result"][0]
        return r.get("contractCreator"), r.get("txHash")
    return None, None

async def get_contract_source(session, addr):
    url = (f"{BSCSCAN_URL}?module=contract&action=getsourcecode"
           f"&address={addr}&apikey={BSCSCAN_KEY}")
    data = await http_get(session, url)
    if data and data.get("status") == "1" and data.get("result"):
        return data["result"][0].get("SourceCode", "")
    return ""

async def get_wallet_age_days(session, wallet_addr: str) -> Optional[float]:
    """Get the age of a wallet in days by finding its first transaction."""
    url = (f"{BSCSCAN_URL}?module=account&action=txlist"
           f"&address={wallet_addr}&startblock=0&endblock=99999999"
           f"&page=1&offset=1&sort=asc&apikey={BSCSCAN_KEY}")
    data = await http_get(session, url)
    if data and data.get("status") == "1" and data.get("result"):
        try:
            ts = int(data["result"][0].get("timeStamp", 0))
            return (time.time() - ts) / 86400
        except Exception:
            pass
    return None

async def rugdoc_check(session, addr: str) -> dict:
    """
    RugDoc honeypot/risk API (BSC chain id = 56).
    Returns a dict with risk level and details.
    """
    try:
        data = await http_get(
            session,
            f"https://api.rugdoc.io/api/honeypot.js?address={addr}&chain=bsc",
            headers={"User-Agent": "Mozilla/5.0"}
        )
        if data:
            status = data.get("status", "UNKNOWN")
            # RugDoc statuses: OK, NO_LOCKING_MECHANISM, HONEYPOT, POTENTIAL_RUGPULL, etc.
            return {
                "status": status,
                "tax_buy":  data.get("buyTax"),
                "tax_sell": data.get("sellTax"),
                "raw": data
            }
    except Exception as e:
        logger.warning(f"RugDoc error {addr[:10]}: {e}")
    return {"status": "UNKNOWN"}

async def check_liquidity_lock(session, pair_addr: str) -> dict:
    """
    Check if liquidity is locked via DxSale, Pinksale, Unicrypt, or Team.Finance.
    Queries BSCScan for LP token transfers to known lock contracts.
    Returns dict with locked bool, locker name, and unlock time if available.
    """
    LOCK_CONTRACTS = {
        "0x407993575c91ce7643a4d4ccacc9a98c36ee1bbe": "PinkLock",
        "0x71b5759d73262fbb223956913ecf4ecc51057641": "Unicrypt",
        "0xc77aab3c6d7dab46248f3cc3033c856171878bd5": "DxLock",
        "0xe2fe530c047f2d85298b07d9333c05737f1435fb": "Team.Finance",
        "0xd9d89dae66b5462b5ee7e14f0e58d2ba6a38d1e2": "Mudra",
    }
    url = (f"{BSCSCAN_URL}?module=account&action=tokentx"
           f"&address={pair_addr}&page=1&offset=50"
           f"&sort=desc&apikey={BSCSCAN_KEY}")
    data = await http_get(session, url)
    if not data or data.get("status") != "1":
        return {"locked": False, "locker": None, "note": "Unable to verify"}

    txs = data.get("result", [])
    for tx in txs:
        to_addr = tx.get("to", "").lower()
        if to_addr in LOCK_CONTRACTS:
            return {
                "locked": True,
                "locker": LOCK_CONTRACTS[to_addr],
                "note": f"LP sent to {LOCK_CONTRACTS[to_addr]}"
            }
    return {"locked": False, "locker": None, "note": "No lock detected"}

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
        return {"verified": False, "flags": ["Unverified contract"], "score": 20}
    flags = []
    score = 100
    checks = {
        "mint(":        ("Mintable supply", 25),
        "blacklist":    ("Blacklist function", 20),
        "setfee":       ("Changeable fees", 15),
        "pause()":      ("Can pause trading", 20),
        "selfdestruct": ("Selfdestruct present", 35),
        "delegatecall": ("Dangerous proxy call", 25),
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
        return f"Yes — {active} active boost{'s' if active > 1 else ''}"
    if pair_data.get("info"):
        return "Free listing"
    return "Not listed"

def rugdoc_label(status: str) -> str:
    labels = {
        "OK":                    "🟢 OK",
        "NO_LOCKING_MECHANISM":  "🟡 No lock mechanism",
        "HONEYPOT":              "🔴 HONEYPOT",
        "POTENTIAL_RUGPULL":     "🔴 Potential rug",
        "RESTRICTED_TRANSFER":   "🟠 Restricted transfer",
        "UNKNOWN":               "⚪ Unknown",
    }
    return labels.get(status, f"⚪ {status}")


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
        return True, f"{round(sells/total*100)}% sells dominant"
    if v1h > 3000 and c1h < MAX_DROP_1H:
        return True, f"Dumping {c1h}% on high volume"
    if mc > 0 and liq > 0 and liq / mc < MIN_LIQ_MCAP:
        return True, f"Liq only {round(liq/mc*100,1)}% of mcap"
    if liq > 0 and v1h > liq * 15:
        return True, "Wash trading suspected"
    if c6h > 500 and c1h < -20:
        return True, f"Pumped {c6h}% and reversing"
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
        return True, f"First volume detected after silence — ${v1h:,.0f}/hr"
    if v1h >= prev_vol * WAKE_SPIKE_MULT:
        m = round(v1h / prev_vol, 1)
        return True, f"Volume {m}x spike — ${prev_vol:,.0f} → ${v1h:,.0f}/hr"
    return False, ""


# ── IMPROVED NARRATIVE DETECTION ──────────────────────────────────────────────

NARRATIVE_SIGNALS = {
    "🤖 AI / Agents": {
        "keywords": ["ai", "agent", "gpt", "llm", "neural", "deepseek", "openai", "copilot", "agi", "bot", "compute"],
        "weight": 3
    },
    "🐸 Meme Culture": {
        "keywords": ["pepe", "doge", "shib", "inu", "cat", "frog", "moon", "wojak", "chad", "based",
                     "bonk", "floki", "baby", "elon", "musk", "rocket", "420", "69", "lol", "haha"],
        "weight": 2
    },
    "🏛️ Political / Cultural": {
        "keywords": ["trump", "maga", "biden", "vote", "gop", "potus", "america", "freedom", "liberty",
                     "patriot", "nation", "president"],
        "weight": 2
    },
    "🎮 Gaming / NFT": {
        "keywords": ["game", "play", "nft", "metaverse", "guild", "rpg", "quest", "pixel", "arena",
                     "battle", "hero", "legend"],
        "weight": 2
    },
    "💰 DeFi / Yield": {
        "keywords": ["defi", "yield", "farm", "stake", "swap", "vault", "lend", "earn", "protocol",
                     "liquidity", "amm"],
        "weight": 2
    },
    "🐂 BSC Native": {
        "keywords": ["bnb", "binance", "pancake", "bsc", "bep20"],
        "weight": 1
    },
    "🌐 Web3 / Infra": {
        "keywords": ["web3", "dao", "governance", "chain", "node", "rpc", "oracle", "bridge", "layer"],
        "weight": 2
    },
    "🐾 Animal / Cute": {
        "keywords": ["dog", "cat", "bear", "bull", "monkey", "ape", "panda", "hamster", "rabbit",
                     "wolf", "fox", "lion", "tiger"],
        "weight": 1
    },
    "🌍 Real World Asset": {
        "keywords": ["rwa", "gold", "silver", "oil", "real", "estate", "property", "commodity"],
        "weight": 3
    },
}

def narrative(name: str, symbol: str) -> str:
    text = f"{name} {symbol}".lower()
    matches = []
    for label, cfg in NARRATIVE_SIGNALS.items():
        hits = [kw for kw in cfg["keywords"] if kw in text]
        if hits:
            matches.append((label, cfg["weight"], hits))

    if not matches:
        return "None detected"

    # Sort by weight desc, then by number of keyword hits
    matches.sort(key=lambda x: (x[1], len(x[2])), reverse=True)

    # Return top 3, label only
    top = [m[0] for m in matches[:3]]
    return "  ·  ".join(top)


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
            signals.append("Waking up after silence")

        if v1h > 0 and v6h > 0:
            avg = v6h / 6
            if avg > 0:
                m = v1h / avg
                if m >= 4:     score += 20; signals.append(f"Volume {round(m,1)}x the 6h avg")
                elif m >= 2:   score += 12; signals.append(f"Volume {round(m,1)}x the 6h avg")
                elif m >= 1.3: score += 6;  signals.append(f"Volume picking up {round(m,1)}x")

        t1h = b1h + s1h
        if t1h > 3:
            bp = b1h / t1h
            if bp >= 0.68:   score += 18; signals.append(f"{round(bp*100)}% buy pressure in 1h")
            elif bp >= 0.55: score += 10; signals.append(f"{round(bp*100)}% buy pressure in 1h")

        up = sum([c5m > 0, c1h > 0, c6h > 0])
        if up == 3:   score += 14; signals.append("Uptrend across all timeframes")
        elif up == 2: score += 7;  signals.append("Uptrend on 2 of 3 timeframes")

        if mc > 0 and v24h > 0:
            r = v24h / mc
            if r > 1.0:   score += 14; signals.append(f"Vol/MCap ratio {round(r*100)}%")
            elif r > 0.3: score += 7;  signals.append(f"Vol/MCap ratio {round(r*100)}%")

        if b5m >= 8:   score += 10; signals.append(f"{b5m} buys in last 5 min")
        elif b5m >= 3: score += 5;  signals.append(f"{b5m} buys in last 5 min")

        if 2000 <= v1h <= 60000:
            score += 8; signals.append(f"Early volume zone ${v1h:,.0f}/hr")

    except Exception as e:
        logger.warning(f"Score error: {e}")

    return {"score": min(100, score), "signals": signals}

def virality(pair_data: dict) -> dict:
    score = 0; signals = []
    b5m   = int(pair_data.get("txns", {}).get("m5", {}).get("buys", 0) or 0)
    b1h   = int(pair_data.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
    v5m   = float(pair_data.get("volume", {}).get("m5", 0) or 0)
    boost = pair_data.get("boosts", {}).get("active", 0) or 0

    if b5m >= 15:   score += 28; signals.append(f"{b5m} buys in 5m")
    elif b5m >= 7:  score += 15; signals.append(f"{b5m} buys in 5m")
    if b1h >= 100:  score += 28; signals.append(f"{b1h} buys in 1h")
    elif b1h >= 40: score += 15; signals.append(f"{b1h} buys in 1h")
    if v5m > 10000: score += 22; signals.append(f"${v5m:,.0f} volume in 5m")
    elif v5m > 3000: score += 10; signals.append(f"${v5m:,.0f} volume in 5m")
    if boost: score += 22; signals.append(f"DexScreener boosted ({boost})")

    return {"score": min(100, score), "signals": signals}

def risk_label(is_hp, cscore, top10, gp):
    if is_hp: return "🔴 HONEYPOT"
    if any([
        gp.get("can_take_back_ownership","0")=="1",
        gp.get("is_proxy","0")=="1",
        gp.get("hidden_owner","0")=="1"
    ]): return "🔴 HIGH RISK"
    if cscore < 40 or top10 > 85: return "🔴 HIGH RISK"
    if cscore < 65 or top10 > 65: return "🟡 MEDIUM RISK"
    if gp.get("owner_address","") == "0x0000000000000000000000000000000000000000" and cscore >= 75:
        return "🟢 LOW RISK"
    return "🟡 MEDIUM RISK"

def predict(pair_data, acc, waking, dump):
    if dump: return "🔴 Dump pattern — avoid"
    c1h = float(pair_data.get("priceChange", {}).get("h1", 0) or 0)
    c6h = float(pair_data.get("priceChange", {}).get("h6", 0) or 0)
    if waking and acc >= 60: return "🚀 High potential — waking after silence"
    if waking:               return "👀 Watch — first activity appearing"
    if acc >= 75 and c1h > 0 and c6h > 0: return "🚀 Strong bullish"
    if acc >= 55 and c1h > 0:             return "📈 Bullish — accumulation forming"
    if acc >= 40:                         return "🟡 Early signal — watch closely"
    return "⚪ Neutral"


# ── HOLDER GROWTH RATE ────────────────────────────────────────────────────────

def record_holder_snapshot(addr: str, count: int):
    """Store a timestamped holder count for growth rate calculation."""
    if addr not in holder_snapshots:
        holder_snapshots[addr] = []
    snaps = holder_snapshots[addr]
    snaps.append((time.time(), count))
    # Keep last 60 snapshots (~1h at 1/min)
    holder_snapshots[addr] = snaps[-60:]

def holder_growth_rate(addr: str) -> Optional[str]:
    """
    Returns a human-readable growth rate string, e.g. "+12/hr" or "+3/min".
    Returns None if insufficient data.
    """
    snaps = holder_snapshots.get(addr, [])
    if len(snaps) < 2:
        return None
    oldest_ts, oldest_count = snaps[0]
    latest_ts,  latest_count = snaps[-1]
    elapsed_hrs = (latest_ts - oldest_ts) / 3600
    if elapsed_hrs < 0.01:
        return None
    delta = latest_count - oldest_count
    rate_per_hr = delta / elapsed_hrs
    if abs(rate_per_hr) < 1:
        rate_per_min = rate_per_hr / 60
        sign = "+" if rate_per_min >= 0 else ""
        return f"{sign}{rate_per_min:.1f}/min"
    sign = "+" if rate_per_hr >= 0 else ""
    return f"{sign}{round(rate_per_hr)}/hr"


# ── PENDING WATCHLIST ─────────────────────────────────────────────────────────

def add_to_pending(addr: str, pair_data: dict, reason_interesting: str, reason_skipped: str):
    """
    Store a token that was scanned but not alerted.
    Only keep the best MAX_PENDING tokens by score.
    """
    if addr in alerted_tokens:
        return  # Already alerted — don't add to pending

    mc     = float(pair_data.get("marketCap", 0) or 0)
    ticker = pair_data.get("baseToken", {}).get("symbol", "???")
    name   = pair_data.get("baseToken", {}).get("name", "?")
    addr_  = pair_data.get("baseToken", {}).get("address", addr)
    pair_a = pair_data.get("pairAddress", "")
    url    = pair_data.get("url", f"https://dexscreener.com/bsc/{pair_a}")

    pending_tokens[addr] = {
        "ticker":             ticker,
        "name":               name,
        "mc":                 mc,
        "ca":                 addr_,
        "url":                url,
        "reason_interesting": reason_interesting,
        "reason_skipped":     reason_skipped,
        "ts":                 time.time(),
    }

    # Evict old entries
    now = time.time()
    expired = [k for k, v in pending_tokens.items() if now - v["ts"] > PENDING_TTL]
    for k in expired:
        del pending_tokens[k]

    # If over limit, remove oldest
    if len(pending_tokens) > MAX_PENDING:
        oldest = sorted(pending_tokens.items(), key=lambda x: x[1]["ts"])
        for k, _ in oldest[:len(pending_tokens) - MAX_PENDING]:
            del pending_tokens[k]


# ── FULL REPORT (v5.1 redesign) ───────────────────────────────────────────────

async def build_report(
    session,
    pair_data: dict,
    is_waking: bool = False,
    wake_signal: str = "",
    is_new: bool = False,
    new_age_str: str = "",
    reserve_detected: bool = False,
) -> tuple:

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
    c1h_s     = pair_data.get("priceChange", {}).get("h1", "0")
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
        age = "?"

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

    # Parallel security fetch
    results = await asyncio.gather(
        honeypot_check(session, addr),
        get_contract_source(session, addr),
        goplus_check(session, addr),
        gmgn_holders(session, addr),
        get_top_holders(session, addr),
        get_deployer(session, addr),
        rpc_total_supply(session, addr),
        rugdoc_check(session, addr),
        check_liquidity_lock(session, pair_addr),
        return_exceptions=True
    )

    hp_data, src, gp_data, gmgn, top_holders, deployer_info, total_supply, rugdoc, liq_lock = results

    hp           = hp_data       if isinstance(hp_data, dict)       else {}
    gp           = gp_data       if isinstance(gp_data, dict)       else {}
    src          = src           if isinstance(src, str)             else ""
    top_holders  = top_holders   if isinstance(top_holders, list)   else []
    deployer, _  = deployer_info if isinstance(deployer_info, tuple) else (None, None)
    total_supply = total_supply  if isinstance(total_supply, int)   else None
    gmgn         = gmgn          if isinstance(gmgn, int)           else None
    rugdoc       = rugdoc        if isinstance(rugdoc, dict)        else {"status": "UNKNOWN"}
    liq_lock     = liq_lock      if isinstance(liq_lock, dict)      else {"locked": False}

    is_hp         = hp.get("isHoneypot", False)
    hp_reason     = hp.get("honeypotResult", {}).get("reason", "")
    buy_tax       = hp.get("simulationResult", {}).get("buyTax")
    sell_tax      = hp.get("simulationResult", {}).get("sellTax")
    contract      = analyze_contract(src)
    renounced     = gp.get("owner_address","") == "0x0000000000000000000000000000000000000000"
    is_mintable   = gp.get("is_mintable","0") == "1"
    is_proxy      = gp.get("is_proxy","0") == "1"
    hidden_owner  = gp.get("hidden_owner","0") == "1"

    # Top 10 holders
    top10_pct = 0.0
    top10_lines = []
    if top_holders:
        try:
            for i, h in enumerate(top_holders[:10], 1):
                pct = float(h.get("percentage", 0) or 0)
                if pct == 0 and total_supply:
                    qty = int(h.get("TokenHolderQuantity", 0) or 0)
                    pct = qty / total_supply * 100
                top10_pct += pct
                wallet = h.get("TokenHolderAddress", "?")
                short  = f"{wallet[:6]}...{wallet[-4:]}"
                top10_lines.append(f"  {i:>2}. `{short}` — {round(pct,2)}%")
        except Exception as e:
            logger.warning(f"Top10 calc: {e}")

    # Holder count + growth rate
    gp_holders = gp.get("holder_count")
    holder_sources = {}
    if gp_holders: holder_sources["GoPlus"] = int(gp_holders)
    if gmgn:       holder_sources["GMGN"]   = int(gmgn)
    if holder_sources:
        best = max(holder_sources.values())
        record_holder_snapshot(addr, best)
        growth = holder_growth_rate(addr)
        growth_str = f"  ↗ {growth}" if growth else ""
        if len(holder_sources) == 2:
            gv, gm = holder_sources.get("GoPlus",0), holder_sources.get("GMGN",0)
            hdisplay = f"{best:,}{growth_str}"
        else:
            hdisplay = f"{best:,} (via {list(holder_sources.keys())[0]}){growth_str}"
    else:
        hdisplay = "—"

    # Dev holding + wallet age
    dev_str = "—"
    dev_age_flag = ""
    if deployer and total_supply:
        dev_pct = await get_dev_holding(session, addr, deployer, total_supply)
        dev_age = await get_wallet_age_days(session, deployer)
        short_dep = f"{deployer[:6]}...{deployer[-4:]}"

        if dev_age is not None and dev_age < DEV_NEW_WALLET_DAYS:
            dev_age_flag = f"  ⚠️ New wallet ({round(dev_age)}d old)"

        if dev_pct is not None:
            if dev_pct > 20:      dev_str = f"{round(dev_pct,2)}%  🔴 Very high"
            elif dev_pct > 10:    dev_str = f"{round(dev_pct,2)}%  ⚠️ High"
            elif dev_pct == 0:    dev_str = f"0%  ✅ Sold/burned"
            else:                 dev_str = f"{round(dev_pct,2)}%  ✅"
        else:
            dev_str = f"`{short_dep}` (balance unavailable)"
        if dev_age_flag:
            dev_str += dev_age_flag
    elif deployer:
        short_dep = f"{deployer[:6]}...{deployer[-4:]}"
        dev_str = f"`{short_dep}`"

    dex_paid = dex_paid_status(pair_data)
    dump_flag, dump_reason = is_dump(pair_data)
    risk  = risk_label(is_hp, contract["score"], top10_pct, gp)
    narr  = narrative(name, symbol)
    pred  = predict(pair_data, acc["score"], is_waking, dump_flag)
    mcel  = mc_emoji(mc)

    # Lock display
    if liq_lock.get("locked"):
        lock_str = f"✅ Locked — {liq_lock['locker']}"
    else:
        lock_str = f"❌ Not locked  ({liq_lock.get('note','')})"

    # RugDoc
    rd_str = rugdoc_label(rugdoc.get("status", "UNKNOWN"))

    # Alert type — strict logic
    # On-chain Wakeup = reserve_detected (must be from reserve monitor)
    # Sleeping Giant  = DexScreener volume spike on old token
    # New Launch      = token age ≤ 72h
    # Accumulation    = everything else
    if reserve_detected:
        atype_icon = "⛓"
        atype_label = "ON-CHAIN WAKEUP"
    elif is_new:
        atype_icon = "🆕"
        atype_label = f"NEW LAUNCH  ·  {new_age_str}"
    elif is_waking:
        atype_icon = "💤"
        atype_label = "SLEEPING GIANT"
    else:
        atype_icon = "📡"
        atype_label = "ACCUMULATION"

    # Score bar
    def score_bar(s, length=10):
        filled = round(s / 100 * length)
        return "█" * filled + "░" * (length - filled)

    def pct_fmt(v):
        try:
            f = float(v)
            return f"+{f}%" if f > 0 else f"{f}%"
        except Exception:
            return f"{v}%"

    cflags_str = ""
    if contract["flags"]:
        cflags_str = "\n" + "\n".join(f"  ⚠ {f}" for f in contract["flags"])

    # ── Build message ─────────────────────────────────────────────────────────
    msg = (
        f"{atype_icon} *{atype_label}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"*{name}*  `${symbol}`\n"
        f"{mcel} {mc_label(mc)}  ·  {dex_name}  ·  Age {age}\n"
        f"`{addr}`\n"
    )

    if wake_signal:
        msg += f"\n🔔 _{wake_signal}_\n"

    msg += (
        f"\n*Market*\n"
        f"Price  `${price_str}`\n"
        f"MCap   `${mc:,.0f}`   Liq `${liq:,.0f}` _{lm}% of mcap_\n"
        f"Vol    5m `${v5m:,.0f}`  1h `${v1h:,.0f}`  24h `${v24h:,.0f}`\n"
        f"Δ Price  5m `{pct_fmt(c5m)}`  1h `{pct_fmt(c1h_s)}`  6h `{pct_fmt(c6h_s)}`  24h `{pct_fmt(c24h)}`\n"
        f"Txns   5m {b5m_t}B / {s5m_t}S   1h {b1h}B / {s1h}S  _{bp}% buys_\n"
    )

    if dump_flag:
        msg += f"\n⛔ *DUMP ALERT* — {dump_reason}\n"

    msg += (
        f"\n*Safety*  {risk}\n"
        f"Honeypot   {'🔴 YES — AVOID' if is_hp else '🟢 Clean'}\n"
    )
    if hp_reason:
        msg += f"  _└ {hp_reason}_\n"

    msg += (
        f"RugDoc     {rd_str}\n"
        f"Tax        Buy `{f'{buy_tax}%' if buy_tax is not None else '?'}`  "
        f"Sell `{f'{sell_tax}%' if sell_tax is not None else '?'}`\n"
        f"Liq Lock   {lock_str}\n"
        f"Contract   `{contract['score']}/100`  "
        f"Verified {'✅' if contract['verified'] else '❌'}  "
        f"Renounced {'✅' if renounced else '⚠️'}\n"
        f"Flags      {'None ✅' if not contract['flags'] else ''}{cflags_str}\n"
        f"Mintable {'🔴' if is_mintable else '✅'}  "
        f"Proxy {'🔴' if is_proxy else '✅'}  "
        f"Hidden owner {'🔴' if hidden_owner else '✅'}\n"
        f"\n*Holders*\n"
        f"Total      {hdisplay}\n"
        f"Dev wallet {dev_str}\n"
        f"Top 10     `{round(top10_pct,1)}%` of supply\n"
    )

    if top10_lines:
        msg += "\n".join(top10_lines[:5]) + "\n"  # Show top 5, keep alert compact
        if len(top10_lines) > 5:
            msg += f"  _...and {len(top10_lines)-5} more_\n"

    msg += (
        f"\n*Score*\n"
        f"Signal   `{acc['score']}/100`  {score_bar(acc['score'])}\n"
        f"Viral    `{vir['score']}/100`  {score_bar(vir['score'])}\n"
    )
    if acc["signals"]:
        msg += "\n".join(f"  · {s}" for s in acc["signals"][:4]) + "\n"

    msg += (
        f"\n*Narrative*  {narr}\n"
        f"\n*Outlook*  {pred}\n"
        f"\n[DexScreener]({dex_url})  ·  "
        f"[BSCScan](https://bscscan.com/token/{addr})"
    )

    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("⭐ Watchlist", callback_data=f"addfav_{addr}"),
        InlineKeyboardButton("📊 Chart", url=dex_url),
        InlineKeyboardButton("🔍 BSCScan", url=f"https://bscscan.com/token/{addr}"),
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


# ── DATABASE BUILDER ──────────────────────────────────────────────────────────

async def build_pair_database(session):
    global pair_database
    current_block = await rpc_block_number(session)
    if not current_block:
        return
    lookback_blocks = DB_LOOKBACK_DAYS * BSC_BLOCKS_PER_DAY
    from_block = current_block - lookback_blocks
    existing = len(pair_database)
    try:
        for page in range(1, 6):
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
                    pa, ta = parsed
                    if pa not in pair_database:
                        pair_database[pa] = ta
                        added += 1
            logger.info(f"DB build page {page}: +{added} (total: {len(pair_database)})")
            await asyncio.sleep(0.3)
    except Exception as e:
        logger.warning(f"DB build error: {e}")
    new_total = len(pair_database)
    if new_total > existing:
        logger.info(f"Pair DB: {existing} → {new_total} (+{new_total - existing})")


# ── RESERVE MONITOR ───────────────────────────────────────────────────────────

async def scan_reserves(session, app):
    global db_scan_pointer
    if not pair_database or not subscribed_chats:
        return
    pairs_list = list(pair_database.items())
    total      = len(pairs_list)
    start      = db_scan_pointer % total
    end        = min(start + RESERVE_BATCH_SIZE, total)
    batch      = pairs_list[start:end]
    if len(batch) < RESERVE_BATCH_SIZE and total > RESERVE_BATCH_SIZE:
        batch += pairs_list[:RESERVE_BATCH_SIZE - len(batch)]
    db_scan_pointer = end % total
    logger.info(f"Reserve scan: {len(batch)} pairs (DB: {total})")
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
                pair_reserves[pair_addr] = {"r0": r0, "r1": r1, "stable_count": 0,
                                             "last_ts": time.time(), "alerted": False}
                continue
            change_r0 = abs(r0 - prev["r0"]) / prev["r0"] * 100 if prev["r0"] > 0 else 0
            change_r1 = abs(r1 - prev["r1"]) / prev["r1"] * 100 if prev["r1"] > 0 else 0
            max_change = max(change_r0, change_r1)
            if max_change < RESERVE_CHANGE_PCT:
                pair_reserves[pair_addr]["stable_count"] = min(prev["stable_count"] + 1, 100)
                pair_reserves[pair_addr]["r0"] = r0
                pair_reserves[pair_addr]["r1"] = r1
                continue
            if prev["stable_count"] >= RESERVE_MIN_STABLE:
                flagged.append((token_addr, pair_addr, prev["stable_count"], max_change))
            pair_reserves[pair_addr] = {"r0": r0, "r1": r1, "stable_count": 0,
                                         "last_ts": time.time(), "alerted": False}
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.warning(f"Reserve check {pair_addr[:10]}: {e}")

    for token_addr, pair_addr, stable_cycles, reserve_change in flagged:
        try:
            if token_addr in alerted_tokens:
                continue
            pair_data = await dex_token(session, token_addr)
            if not pair_data:
                continue
            liq = float(pair_data.get("liquidity", {}).get("usd", 0) or 0)
            if liq < MIN_LIQ_MICRO:
                continue
            if is_dump(pair_data)[0]:
                continue

            # Check liquidity lock before alerting
            liq_lock_res = await check_liquidity_lock(session, pair_addr)
            if not liq_lock_res.get("locked"):
                # Check if it has good potential to decide whether to queue or skip
                acc = score_token(pair_data, token_history.get(token_addr), True)
                if acc["score"] >= MIN_SCORE_WAKE:
                    add_to_pending(
                        token_addr, pair_data,
                        reason_interesting=f"On-chain reserve moved {round(reserve_change,1)}% after {stable_cycles} stable cycles",
                        reason_skipped="Liquidity not locked — monitoring for lock"
                    )
                continue

            acc = score_token(pair_data, token_history.get(token_addr), True)
            wake_signal = (
                f"On-chain reserves moved {round(reserve_change,1)}% "
                f"after {stable_cycles} stable cycles"
            )
            report, markup = await build_report(
                session, pair_data, is_waking=True,
                wake_signal=wake_signal, reserve_detected=True
            )
            price = float(pair_data.get("priceUsd") or 0)
            alerted_tokens[token_addr] = {"ts": time.time(), "price": price}
            name = pair_data.get("baseToken", {}).get("name", "?")
            logger.info(f"RESERVE ALERT: {name} mc=${float(pair_data.get('marketCap',0)):,.0f}")

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
            logger.warning(f"Reserve flagged token error: {e}")


# ── RPC NEW PAIR SCAN ─────────────────────────────────────────────────────────

async def rpc_scan_new_pairs(session, app):
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
            if pair_addr not in pair_database:
                pair_database[pair_addr] = token_addr
            if token_addr in alerted_tokens or token_addr in seen_new_pairs:
                continue
            await asyncio.sleep(5)
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
            if is_dump(pair_data)[0]:
                continue

            acc = score_token(pair_data, token_history.get(token_addr), False)
            if acc["score"] < MIN_SCORE_NEW:
                # Store in pending if it has some interest
                if acc["score"] >= MIN_SCORE_PENDING:
                    add_to_pending(
                        token_addr, pair_data,
                        reason_interesting="; ".join(acc["signals"]) or "Early activity",
                        reason_skipped=f"Score {acc['score']}/100 — below new launch threshold ({MIN_SCORE_NEW})"
                    )
                continue

            # Check liquidity lock for new launches
            liq_lock_res = await check_liquidity_lock(session, pair_addr)
            if not liq_lock_res.get("locked"):
                add_to_pending(
                    token_addr, pair_data,
                    reason_interesting="; ".join(acc["signals"][:2]) or "New launch with early signals",
                    reason_skipped="Liquidity not locked — will alert once confirmed"
                )
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

async def run_scan(session, app):
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
            if is_dump(pair_data)[0]:
                continue

            # Determine token type strictly by age
            ts = pair_data.get("pairCreatedAt")
            age_h = (time.time() - int(ts)/1000) / 3600 if ts else 999
            is_new_flag = age_h <= NEW_PAIR_MAX_AGE_H and addr not in seen_new_pairs
            new_age_str = ""
            if is_new_flag:
                new_age_str = f"{round(age_h,1)}h old" if age_h >= 1 else f"{round(age_h*60)}m old"

            # Only allow is_waking on tokens older than 24h
            is_waking, wake_signal = (False, "")
            if age_h > 24:
                is_waking, wake_signal = is_sleeping_giant(pair_data, prev)

            acc = score_token(pair_data, prev, is_waking)

            should_alert = False
            skip_reason  = ""
            if is_new_flag and acc["score"] >= MIN_SCORE_NEW:
                should_alert = True
                seen_new_pairs.add(addr)
            elif is_waking and acc["score"] >= MIN_SCORE_WAKE:
                should_alert = True
            elif acc["score"] >= MIN_SCORE_STD:
                should_alert = True
            else:
                skip_reason = f"Score {acc['score']}/100 — below threshold"

            if not should_alert:
                if acc["score"] >= MIN_SCORE_PENDING:
                    add_to_pending(
                        addr, pair_data,
                        reason_interesting="; ".join(acc["signals"][:2]) or "Activity detected",
                        reason_skipped=skip_reason
                    )
                continue

            # Check liquidity lock before sending alert
            pair_addr = pair_data.get("pairAddress", "")
            liq_lock_res = await check_liquidity_lock(session, pair_addr)
            if not liq_lock_res.get("locked"):
                add_to_pending(
                    addr, pair_data,
                    reason_interesting="; ".join(acc["signals"][:2]) or "Accumulation signal",
                    reason_skipped="Liquidity not locked"
                )
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

async def check_fav_moves(app):
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
                                f"⭐ *{ticker}  ·  Watchlist Move*\n\n"
                                f"Moved {d} *{c1h}%* in 1h\n"
                                f"Price  `${price}`\n\n"
                                f"[View chart]({url})"
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
        await query.answer("Error reading address.", show_alert=True)
        return

    if chat_id not in favourites:
        favourites[chat_id] = {}
    if addr in favourites[chat_id]:
        await query.answer("Already in your Watchlist ⭐", show_alert=False)
        return

    ticker = token_history.get(addr, {}).get("ticker", "???")
    price  = float(token_history.get(addr, {}).get("price") or 0)
    favourites[chat_id][addr] = {"ticker": ticker, "added_ts": time.time(), "last_price": price}
    await query.answer(f"⭐ Added ${ticker} to Watchlist!", show_alert=False)
    try:
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("⭐ In Watchlist ✅", callback_data=f"addfav_{addr}"),
            InlineKeyboardButton("📊 Chart", url=f"https://dexscreener.com/bsc/{addr}"),
            InlineKeyboardButton("🔍 BSCScan", url=f"https://bscscan.com/token/{addr}"),
        ]]))
    except Exception:
        pass
    await update_pinned_favs(context.application, chat_id)


# ── WATCHLIST BOARD ───────────────────────────────────────────────────────────

async def build_fav_board(session, chat_id: int) -> str:
    favs = favourites.get(chat_id, {})
    if not favs:
        return "⭐ *Watchlist*\n\nEmpty. Tap ⭐ on any alert to add a token."
    lines = ["⭐ *Watchlist  ·  Live*\n"]
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
                f"[${ticker}]({url})  `{addr[:8]}...`\n"
                f"Price `${price}`   MCap `${mc:,.0f}` {mc_emoji(mc)}\n"
                f"Vol 1h `${v1h:,.0f}`   Liq `${liq:,.0f}`\n"
                f"{e} 1h `{c1h}%`   24h `{c24h}%`\n"
            )
        else:
            lines.append(f"${info.get('ticker','???')}  `{addr}` — _unavailable_\n")
    return "\n".join(lines)

async def update_pinned_favs(app, chat_id: int):
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
                chat_id=chat_id, message_id=msg.message_id, disable_notification=True
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
        BotCommand("watchlist", "Your watchlist — live prices"),
        BotCommand("fav",       "Add token to watchlist"),
        BotCommand("unfav",     "Remove from watchlist"),
        BotCommand("pending",   "Tokens scanned but not yet alerted"),
        BotCommand("status",    "Scanner stats"),
        BotCommand("filters",   "Current filter settings"),
    ])
    await update.message.reply_text(
        "⚡ *Impulse BSC  v5.1*\n\n"
        "Three detection layers running:\n"
        "  ⛓  On-chain reserve monitor — old tokens\n"
        "  🆕  New pair detection — direct from blockchain\n"
        "  📡  DexScreener accumulation signals\n\n"
        "All alerts require liquidity to be locked.\n"
        "One alert per token. Watchlist tokens re-alert on ±50% moves.\n\n"
        "Use /pending to view tokens that scored well but haven't been alerted yet.",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    subscribed_chats.discard(update.effective_chat.id)
    await update.message.reply_text("⏸ Paused. /start to resume.")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    fav_count = sum(len(v) for v in favourites.values())
    await update.message.reply_text(
        f"*Impulse BSC v5.1  ·  Status*\n\n"
        f"Scanner       ✅ Running\n"
        f"Pair DB       {len(pair_database):,} pairs\n"
        f"Reserve snaps {len(pair_reserves):,}\n"
        f"Tokens seen   {len(token_history):,}\n"
        f"Alerts sent   {len(alerted_tokens):,}\n"
        f"Pending       {len(pending_tokens)}\n"
        f"Watchlist     {fav_count} tokens\n"
        f"Subscribers   {len(subscribed_chats)}\n"
        f"Last block    {last_rpc_block:,}",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_filters(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"*Impulse v5.1  ·  Filters*\n\n"
        f"*On-chain monitor*\n"
        f"  DB lookback          {DB_LOOKBACK_DAYS}d\n"
        f"  Reserve change       ≥{RESERVE_CHANGE_PCT}%\n"
        f"  Min stable cycles    {RESERVE_MIN_STABLE}\n"
        f"  Batch size           {RESERVE_BATCH_SIZE} pairs/cycle\n\n"
        f"*Score thresholds*\n"
        f"  New launch           ≥{MIN_SCORE_NEW}\n"
        f"  Sleeping giant       ≥{MIN_SCORE_WAKE}\n"
        f"  Standard             ≥{MIN_SCORE_STD}\n"
        f"  Pending storage      ≥{MIN_SCORE_PENDING}\n\n"
        f"*Liquidity*\n"
        f"  Min liq (micro MC)   ${MIN_LIQ_MICRO:,}\n"
        f"  Min liq (standard)   ${MIN_LIQ_STD:,}\n"
        f"  Lock required        Yes\n\n"
        f"*Safety*\n"
        f"  Dev wallet age flag  <{DEV_NEW_WALLET_DAYS}d\n"
        f"  Fav re-alert         ±{FAV_ALERT_PCT}% in 1h\n"
        f"  Pending TTL          {PENDING_TTL//3600}h",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_pending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show tokens that scored well but weren't alerted yet."""
    if not pending_tokens:
        await update.message.reply_text(
            "📋 *Pending Watchlist*\n\nNothing here yet. Tokens that score well but don't meet alert "
            "criteria will appear here.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # Sort by timestamp descending (newest first)
    sorted_pending = sorted(pending_tokens.items(), key=lambda x: x[1]["ts"], reverse=True)

    lines = [f"📋 *Pending  ·  {len(sorted_pending)} token{'s' if len(sorted_pending) != 1 else ''}*\n"]
    for addr, info in sorted_pending[:20]:
        age_mins = round((time.time() - info["ts"]) / 60)
        age_str  = f"{age_mins}m ago" if age_mins < 60 else f"{round(age_mins/60,1)}h ago"
        mc_str   = f"${info['mc']:,.0f}" if info['mc'] > 0 else "—"
        url      = info.get("url", f"https://dexscreener.com/bsc/{addr}")
        lines.append(
            f"[${info['ticker']}]({url})  _{age_str}_\n"
            f"MC `{mc_str}`   `{addr[:8]}...{addr[-4:]}`\n"
            f"💡 {info['reason_interesting']}\n"
            f"⏳ _{info['reason_skipped']}_\n"
        )

    if len(sorted_pending) > 20:
        lines.append(f"\n_...and {len(sorted_pending) - 20} more_")

    text = "\n".join(lines)
    try:
        await update.message.reply_text(
            text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True
        )
    except Exception:
        await update.message.reply_text(text[:4000], disable_web_page_preview=True)


async def cmd_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not favourites.get(chat_id):
        await update.message.reply_text("Nothing in your watchlist. Tap ⭐ on any alert or use /fav <address>.")
        return
    msg = await update.message.reply_text("⭐ Fetching live data...")
    async with aiohttp.ClientSession() as session:
        board = await build_fav_board(session, chat_id)
    try:
        await msg.edit_text(board, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
    except Exception:
        await msg.edit_text(board[:4000], disable_web_page_preview=True)
    await update_pinned_favs(context.application, chat_id)


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
        await update.message.reply_text("Already in your watchlist.")
        return
    msg = await update.message.reply_text("Adding...")
    async with aiohttp.ClientSession() as session:
        pair = await dex_token(session, addr)
    ticker = pair.get("baseToken",{}).get("symbol","???") if pair else "???"
    price  = float(pair.get("priceUsd",0) or 0) if pair else 0
    favourites[chat_id][addr] = {"ticker": ticker, "added_ts": time.time(), "last_price": price}
    await msg.edit_text(f"⭐ Added *${ticker}* to watchlist.", parse_mode=ParseMode.MARKDOWN)
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
        await update.message.reply_text(f"Removed *${ticker}* from watchlist.", parse_mode=ParseMode.MARKDOWN)
        await update_pinned_favs(context.application, chat_id)
    else:
        await update.message.reply_text("Not in your watchlist.")


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
        ts    = pair_data.get("pairCreatedAt")
        age_h = (time.time() - int(ts)/1000) / 3600 if ts else 999
        prev  = token_history.get(address)
        is_waking, wake_signal = (False, "")
        if age_h > 24:
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
    app.add_handler(CommandHandler("pending",   cmd_pending))
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

    app.job_queue.run_repeating(db_build_job,      interval=DB_BUILD_INTERVAL,     first=5)
    app.job_queue.run_repeating(rpc_new_pair_job,  interval=RPC_NEW_PAIR_INTERVAL, first=10)
    app.job_queue.run_repeating(reserve_job,       interval=RESERVE_SCAN_INTERVAL, first=30)
    app.job_queue.run_repeating(dex_job,           interval=SCAN_INTERVAL,         first=20)
    app.job_queue.run_repeating(fav_job,           interval=FAV_CHECK_INTERVAL,    first=90)

    logger.info("Impulse BSC v5.1 starting")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
