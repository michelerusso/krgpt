# scripts/build_binance_map.py
import os, glob, json
import pandas as pd
import ccxt

DAILY_DIR = "data/daily"
OUT_DIR = "data/exchange_map"
OVERRIDES_PATH = os.path.join(OUT_DIR, "binance_overrides.csv")
MAP_PATH = os.path.join(OUT_DIR, "binance_map.csv")

os.makedirs(OUT_DIR, exist_ok=True)

# Config via env
QUOTES = os.environ.get("BINANCE_QUOTES", "USDT,FDUSD,USDC,TUSD,BTC,ETH").split(",")
MAX_COINS = int(os.environ.get("BINANCE_MAX_COINS", "0"))  # 0 = nessun limite
SKIP_STABLES = set(os.environ.get("BINANCE_SKIP_BASES", "USDT,USDC,FDUSD,DAI,TUSD,EUR,USD").split(","))

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
        # columns: coingecko_id,binance_symbol (es. "PEPE/USDT")
        return {r["coingecko_id"]: r["binance_symbol"] for _, r in ov.iterrows() if pd.notna(r.get("binance_symbol"))}
    return {}

def main():
    snap = latest_snapshot_path()
    coins = load_snapshot_rows(snap)

    ex = ccxt.binance({"enableRateLimit": True})
    markets = ex.load_markets()
    # indicizzazione per base
    by_base = {}
    for sym, m in markets.items():
        base = m.get("base")
        quote = m.get("quote")
        if not base or not quote: 
            continue
        by_base.setdefault(base.upper(), []).append((quote.upper(), sym))

    overrides = load_overrides()
    rows = []

    for _, r in coins.iterrows():
        cid = r["id"]
        base = str(r["symbol"]).upper()
        name = r.get("name")
        if base in SKIP_STABLES:
            continue

        # override manuale?
        if cid in overrides:
            bsym = overrides[cid]
            if bsym in markets:
                rows.append({
                    "coingecko_id": cid, "cg_symbol": base, "name": name,
                    "binance_symbol": bsym,
                    "base": markets[bsym]["base"], "quote": markets[bsym]["quote"]
                })
            continue

        # altrimenti prova i quote in ordine
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
                "binance_symbol": sym_full, "base": base, "quote": q
            })
        # se non trovato, lo saltiamo (puoi coprire via overrides)

    if not rows:
        print("Nessuna coppia Binance mappata. Aggiungi overrides se necessario.")
        return

    df = pd.DataFrame(rows).drop_duplicates(subset=["coingecko_id"])
    df.to_csv(MAP_PATH, index=False)
    print(f"Scritta mappa: {MAP_PATH} ({len(df)} righe)")

if __name__ == "__main__":
    main()
