# scripts/fetch_ohlc_ccxt.py
import os, time, datetime as dt
import pandas as pd
import ccxt

EXCHANGE_ID = os.environ.get("CCXT_EXCHANGE", "binanceus").lower()
MAP_PATH = f"data/exchange_map/{EXCHANGE_ID}_map.csv"
OUT_DIR = "data/ohlc"
os.makedirs(OUT_DIR, exist_ok=True)

TIMEFRAME = os.environ.get("CCXT_TIMEFRAME", "1d")
LIMIT = int(os.environ.get("CCXT_LIMIT", "1000"))
MAX_COINS = int(os.environ.get("CCXT_MAX_COINS", "0"))

def iter_pairs():
    if not os.path.exists(MAP_PATH):
        print(f"[WARN] Mappa {MAP_PATH} assente. Skip CCXT fetch.")
        return []
    df = pd.read_csv(MAP_PATH)
    if MAX_COINS > 0:
        df = df.head(MAX_COINS)
    for _, r in df.iterrows():
        yield str(r["cg_symbol"]).upper(), r["ccxt_symbol"]

def out_path_for(base, ccxt_symbol):
    # es: data/ohlc/PEPE__ccxt_binanceus_PEPEUSDT.csv
    market_code = ccxt_symbol.replace("/", "")
    return os.path.join(OUT_DIR, f"{base}__ccxt_{EXCHANGE_ID}_{market_code}.csv")

def last_timestamp_ms(path):
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path, parse_dates=["date"])
    if df.empty:
        return None
    last_date = pd.to_datetime(df["date"].iloc[-1])
    next_day = (last_date + pd.Timedelta(days=1)).normalize()
    return int(next_day.timestamp() * 1000)

def append_rows(path, rows):
    cols = ["date","open","high","low","close","volume"]
    new = pd.DataFrame(rows, columns=cols)
    if os.path.exists(path):
        old = pd.read_csv(path)
        merged = pd.concat([old, new], ignore_index=True)
        merged = merged.drop_duplicates(subset=["date"], keep="last").sort_values("date")
        merged.to_csv(path, index=False)
    else:
        new.to_csv(path, index=False)

def main():
    try:
        ex = getattr(ccxt, EXCHANGE_ID)({"enableRateLimit": True})
    except AttributeError:
        print(f"[WARN] Exchange CCXT sconosciuto: {EXCHANGE_ID}. Skip.")
        return

    ok = fail = 0
    for base, sym in iter_pairs():
        outp = out_path_for(base, sym)
        since = last_timestamp_ms(outp)
        try:
            ohlcv = ex.fetch_ohlcv(sym, timeframe=TIMEFRAME, since=since, limit=LIMIT)
            if not ohlcv:
                print(f"{EXCHANGE_ID}:{sym} nessun nuovo dato.")
                continue
            rows = []
            for ts, o, h, l, c, v in ohlcv:
                date = dt.datetime.utcfromtimestamp(ts/1000).date().isoformat()
                rows.append([date, o, h, l, c, v])
            append_rows(outp, rows)
            print(f"{EXCHANGE_ID}:{sym} -> {outp} (+{len(rows)} righe)")
            ok += 1
        except Exception as e:
            print(f"{EXCHANGE_ID}:{sym} FAILED: {e}")
            fail += 1
        time.sleep(0.25)

    print(f"Done CCXT {EXCHANGE_ID}. success={ok}, failed={fail}")

if __name__ == "__main__":
    main()
