# scripts/fetch_ohlc_ccxt.py
import os, time, datetime as dt
import pandas as pd
import ccxt

MAP_PATH = "data/exchange_map/binance_map.csv"
OUT_DIR = "data/ohlc"
os.makedirs(OUT_DIR, exist_ok=True)

# Env
TIMEFRAME = os.environ.get("CCXT_TIMEFRAME", "1d")   # 1d consigliato per coerenza con CG
LIMIT = int(os.environ.get("CCXT_LIMIT", "1000"))    # barre massime per fetch
MAX_COINS = int(os.environ.get("CCXT_MAX_COINS", "0"))

def iter_pairs():
    df = pd.read_csv(MAP_PATH)
    if MAX_COINS > 0:
        df = df.head(MAX_COINS)
    for _, r in df.iterrows():
        yield r["cg_symbol"].upper(), r["binance_symbol"]

def out_path_for(base, binance_symbol):
    # es: data/ohlc/PEPE__binance_PEPEUSDT.csv
    market_code = binance_symbol.replace("/", "")
    return os.path.join(OUT_DIR, f"{base}__binance_{market_code}.csv")

def last_timestamp_ms(path):
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path, parse_dates=["date"])
    if df.empty:
        return None
    last_date = pd.to_datetime(df["date"].iloc[-1])
    # next day 00:00 UTC
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
    if not os.path.exists(MAP_PATH):
        raise SystemExit("Mappa Binance mancante. Esegui scripts/build_binance_map.py")

    ex = ccxt.binance({"enableRateLimit": True})
    ok, fail = 0, 0

    for base, sym in iter_pairs():
        outp = out_path_for(base, sym)
        since = last_timestamp_ms(outp)
        try:
            # fetchOHLCV: [[ts, o,h,l,c,v], ...]
            ohlcv = ex.fetch_ohlcv(sym, timeframe=TIMEFRAME, since=since, limit=LIMIT)
            if not ohlcv:
                print(f"{sym}: nessun nuovo dato.")
                continue
            rows = []
            for ts, o, h, l, c, v in ohlcv:
                date = dt.datetime.utcfromtimestamp(ts/1000).date().isoformat()
                rows.append([date, o, h, l, c, v])
            append_rows(outp, rows)
            print(f"{sym} -> {outp} (+{len(rows)} righe)")
            ok += 1
        except Exception as e:
            print(f"{sym} FAILED: {e}")
            fail += 1
        time.sleep(0.2)  # ulteriore rate limit gentile

    print(f"Done. success={ok}, failed={fail}")

if __name__ == "__main__":
    main()
