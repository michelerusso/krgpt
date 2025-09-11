# scripts/fetch_ohlc.py
import os, json, glob, time, math, datetime as dt
import pandas as pd
import requests

DAILY_DIR = "data/daily"
OUT_DIR = "data/ohlc"
os.makedirs(OUT_DIR, exist_ok=True)

# Configurazione semplice via env
DAYS = int(os.environ.get("OHLC_DAYS", "365"))         # quanti giorni storici scaricare/aggiornare
SLEEP_S = float(os.environ.get("OHLC_SLEEP_S", "1.2")) # pausa tra chiamate (rate limit CoinGecko)
MAX_COINS = int(os.environ.get("OHLC_MAX_COINS", "0")) # 0 = nessun limite

CG_BASE = "https://api.coingecko.com/api/v3"

def cg_get(path, params=None):
    url = f"{CG_BASE}/{path}"
    r = requests.get(url, params=params or {}, timeout=60)
    r.raise_for_status()
    return r.json()

def latest_daily_snapshot():
    files = sorted(glob.glob(f"{DAILY_DIR}/*.json"))
    if not files:
        raise SystemExit("Nessun snapshot in data/daily/")
    return files[-1]

def choose_universe(snapshot_fp):
    data = json.load(open(snapshot_fp, "r"))
    # default: tutte le coin nello snapshot
    df = pd.DataFrame(data)
    # ordina per volume, così se limitiamo prendiamo le più liquide
    if "total_volume" in df:
        df = df.sort_values("total_volume", ascending=False)
    rows = df[["id","symbol","name"]].dropna().drop_duplicates()
    if MAX_COINS and MAX_COINS > 0:
        rows = rows.head(MAX_COINS)
    return rows.to_dict(orient="records")

def merge_ohlc_volume(ohlc_rows, vol_rows):
    """
    ohlc_rows: [[ts, o,h,l,c], ...]
    vol_rows: [[ts, volume], ...]  (da market_chart 'total_volumes')
    ritorna DataFrame con date (UTC), open, high, low, close, volume
    """
    if not ohlc_rows:
        return pd.DataFrame(columns=["date","open","high","low","close","volume"])
    ohlc = pd.DataFrame(ohlc_rows, columns=["ts","open","high","low","close"])
    ohlc["date"] = pd.to_datetime(ohlc["ts"], unit="ms", utc=True).dt.date.astype(str)
    ohlc = ohlc.drop(columns=["ts"])

    vol = pd.DataFrame(vol_rows, columns=["ts","volume"]) if vol_rows else pd.DataFrame(columns=["ts","volume"])
    if not vol.empty:
        vol["date"] = pd.to_datetime(vol["ts"], unit="ms", utc=True).dt.date.astype(str)
        vol = vol.drop(columns=["ts"]).groupby("date", as_index=False).last()  # ultimo valore per la data
        df = pd.merge(ohlc, vol, on="date", how="left")
    else:
        df = ohlc.copy()
        df["volume"] = None
    # dedup e sort
    df = df.drop_duplicates(subset=["date"]).sort_values("date")
    return df

def append_or_write(out_path, df_new):
    if os.path.exists(out_path):
        old = pd.read_csv(out_path)
        keep_cols = ["date","open","high","low","close","volume"]
        old = old[keep_cols] if set(keep_cols).issubset(old.columns) else old
        merged = pd.concat([old, df_new], ignore_index=True)
        merged = merged.drop_duplicates(subset=["date"], keep="last").sort_values("date")
        merged.to_csv(out_path, index=False)
    else:
        df_new.to_csv(out_path, index=False)

def fetch_one(coin_id, symbol):
    # 1) OHLC (no volume)
    ohlc = cg_get(f"coins/{coin_id}/ohlc", {"vs_currency":"usd", "days": DAYS})
    # 2) market_chart per volumi
    mc = cg_get(f"coins/{coin_id}/market_chart", {"vs_currency":"usd", "days": DAYS})
    vols = mc.get("total_volumes", [])
    df = merge_ohlc_volume(ohlc, vols)
    # salva
    sym = (symbol or "UNK").upper()
    safe_sym = "".join(ch for ch in sym if ch.isalnum() or ch in ("-","_"))
    out_path = os.path.join(OUT_DIR, f"{safe_sym}__{coin_id}.csv")
    append_or_write(out_path, df)
    return out_path, len(df)

def main():
    snap = latest_daily_snapshot()
    coins = choose_universe(snap)
    print(f"Snapshot: {os.path.basename(snap)} | Coin da aggiornare: {len(coins)} | days={DAYS}")
    ok, fail = 0, 0
    for i, c in enumerate(coins, 1):
        cid, sym = c["id"], str(c["symbol"]).upper()
        try:
            path, n = fetch_one(cid, sym)
            print(f"[{i}/{len(coins)}] {sym} ({cid}) -> {path} ({n} rows)")
            ok += 1
        except Exception as e:
            print(f"[{i}/{len(coins)}] {sym} ({cid}) FAILED: {e}")
            fail += 1
        time.sleep(SLEEP_S)
    print(f"Done. success={ok}, failed={fail}")

if __name__ == "__main__":
    main()
