# scripts/build_timeseries.py
import os, json, glob, datetime as dt
import pandas as pd

DAILY_DIR = "data/daily"
OUT_DIR = "data/time_series"
MAP_PATH = "data/coin_map.csv"  # id,symbol,name,last_seen

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(os.path.dirname(MAP_PATH), exist_ok=True)

# 1) Leggi tutti gli snapshot (assumo: ogni file è una lista di dict stile CoinGecko:
# [{"id": "...", "symbol": "btc", "name":"Bitcoin", "current_price":..., "total_volume":..., "market_cap":...}, ...])
files = sorted(glob.glob(f"{DAILY_DIR}/*.json"))
rows = []
for fp in files:
    date_str = os.path.splitext(os.path.basename(fp))[0]  # es: 2025-09-10.json -> 2025-09-10
    try:
        snap_date = dt.date.fromisoformat(date_str)
    except Exception:
        # salta file non conformi
        continue
    data = json.load(open(fp, "r"))
    for c in data:
        rows.append({
            "date": snap_date.isoformat(),
            "id": c.get("id"),
            "symbol": (c.get("symbol") or "").upper(),
            "name": c.get("name"),
            "price_usd": c.get("current_price"),
            "volume_usd": c.get("total_volume"),
            "market_cap_usd": c.get("market_cap"),
            "source": "daily_snapshot"
        })

df = pd.DataFrame(rows).dropna(subset=["id", "symbol", "date"])
# normalizza duplicati (stesso id/date): tieni l'ultima occorrenza
df = df.sort_values(["date"]).drop_duplicates(subset=["date","id"], keep="last")

# 2) aggiorna la mappa ID↔symbol
map_cols = ["id","symbol","name","last_seen"]
if os.path.exists(MAP_PATH):
    mp = pd.read_csv(MAP_PATH)
else:
    mp = pd.DataFrame(columns=map_cols)

last_seen = df.groupby("id")["date"].max().reset_index().rename(columns={"date":"last_seen"})
latest_symbol = df.sort_values("date").groupby("id").tail(1)[["id","symbol","name"]]
mp_new = pd.merge(latest_symbol, last_seen, on="id", how="left")
mp = pd.concat([mp, mp_new]).drop_duplicates(subset=["id"], keep="last")[map_cols]
mp.to_csv(MAP_PATH, index=False)

# 3) salva un CSV per asset: <SYMBOL>__<ID>.csv (SYMBOL aiuta; ID garantisce univocità)
for coin_id, g in df.groupby("id"):
    sym = (g["symbol"].iloc[-1] or "UNK").upper()
    safe_sym = "".join(ch for ch in sym if ch.isalnum() or ch in ("-","_"))
    out = os.path.join(OUT_DIR, f"{safe_sym}__{coin_id}.csv")
    g_out = g[["date","price_usd","volume_usd","market_cap_usd","symbol","name","id","source"]].sort_values("date")
    g_out.to_csv(out, index=False)

print(f"Written {df['id'].nunique()} coin time series to {OUT_DIR}")
