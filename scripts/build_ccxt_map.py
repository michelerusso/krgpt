# scripts/build_ccxt_map.py
import os, glob, json
import pandas as pd
import ccxt

DAILY_DIR = "data/daily"
OUT_DIR = "data/exchange_map"
os.makedirs(OUT_DIR, exist_ok=True)

EXCHANGE_ID = os.environ.get("CCXT_EXCHANGE", "binanceus").lower()  # default: binanceus
QUOTES = os.environ.get("CCXT_QUOTES", "USDT,USD,FDUSD,USDC,TUSD,BTC,ETH,EUR").split(",")
MAX_COINS = int(os.environ.get("CCXT_MAX_COINS_MAP", "0"))  # 0 = nessun limite
SKIP_BASES = set(os.environ.get("CCXT_SKIP_BASES", "USDT,USDC,FDUSD,DAI,TUSD,EUR,USD").split(","))
OVERRIDES_PATH = os.path.join(OUT_DIR, f"{EXCHANGE_ID}_overrides.csv")
MAP_PATH = os.path.join(OUT_DIR, f"{EXCHANGE_ID}_map.csv")

def latest_snapshot_path():
    files = sorted(glob.glob(f"{DAILY_DIR}/*.json"))
    if not files:
        raise SystemExit("Nessun snapshot in data/daily/")
    return files[-1]

def load_snapshot_rows(path):
    data = json.load(open(path, "r"))
    df = pd.DataFrame(data)
    if "total_volume" in df:
        df = df.sort_values("total_volume", ascending=False)
    cols = [c for c in ["id","symbol","name","market_cap"] if c in df.columns]
    df = df[cols].dropna(subset=["id","symbol"])
    if MAX_COINS > 0:
        df = df.head(MAX_COINS)
    return df

def load_overrides():
    if os.path.exists(OVERRIDES_PATH):
        ov = pd.read_csv(OVERRIDES_PATH)
        # columns: coingecko_id,ccxt_symbol (es. "PEPE/USDT")
        return {r["coingecko_id"]: r["ccxt_symbol"] for _, r in ov.iterrows() if pd.notna(r.get("ccxt_symbol"))}
    return {}

def main():
    snap = latest_snapshot_path()
    coins = load_snapshot_rows(snap)

    try:
        ex_class = getattr(ccxt, EXCHANGE_ID)
    except AttributeError:
        raise SystemExit(f"Exchange CCXT sconosciuto: {EXCHANGE_ID}")

    ex = ex_class({"enableRateLimit": True})
    try:
        markets = ex.load_markets()
    except Exception as e:
        print(f"[WARN] {EXCHANGE_ID}.load_markets() failed: {e}")
        print("[WARN] Nessuna mappa scritta. Il workflow può proseguire (userai OHLC CoinGecko).")
        return

    # indicizzazione per base->(quote,symbol)
    by_base = {}
    for sym, m in markets.items():
        base, quote = m.get("base"), m.get("quote")
        if not base or not quote:
            continue
        by_base.setdefault(base.upper(), []).append((quote.upper(), sym))

    overrides = load_overrides()
    rows = []

    for _, r in coins.iterrows():
        cid = r["id"]
        base = str(r["symbol"]).upper()
        name = r.get("name")
        if base in SKIP_BASES:
            continue

        # override manuale?
        if cid in overrides and overrides[cid] in markets:
            bsym = overrides[cid]
            rows.append({
                "coingecko_id": cid, "cg_symbol": base, "name": name,
                "ccxt_symbol": bsym,
                "base": markets[bsym]["base"], "quote": markets[bsym]["quote"]
            })
            continue

        # cerca tra i quote desiderati, in ordine
        candidates = by_base.get(base, [])
        chosen = None
        for q in QUOTES:
            q = q.upper()
            for q_, sym_full in candidates:
                if q_ == q:
                    chosen = (sym_full, q)
                    break
            if chosen:
                break

        if chosen:
            sym_full, q = chosen
            rows.append({
                "coingecko_id": cid, "cg_symbol": base, "name": name,
                "ccxt_symbol": sym_full, "base": base, "quote": q
            })

    if not rows:
        print(f"[INFO] Nessuna coppia trovata su {EXCHANGE_ID}. Aggiungi override in {OVERRIDES_PATH} se serve.")
        return

    df = pd.DataFrame(rows).drop_duplicates(subset=["coingecko_id"])
    df.to_csv(MAP_PATH, index=False)
    print(f"[OK] Scritta mappa: {MAP_PATH} ({len(df)} righe)")

if __name__ == "__main__":
    main()