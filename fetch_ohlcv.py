import os, time, datetime as dt
import pandas as pd
import ccxt

EX = ccxt.kraken()
SYMBOLS = ['BTC/EUR', 'ETH/EUR', 'SOL/EUR', 'QI/EUR']  # modifica qui
TIMEFRAME = '1d'
LIMIT = 500  # ~1,5 anni

def fetch(symbol):
    ohlcv = EX.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=LIMIT)
    df = pd.DataFrame(ohlcv, columns=['ts','open','high','low','close','volume'])
    df['date'] = pd.to_datetime(df['ts'], unit='ms', utc=True).dt.tz_convert('UTC')
    # indicatori utili
    df['ema50']  = df['close'].ewm(span=50, adjust=False).mean()
    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
    tr = pd.concat([
        (df['high']-df['low']),
        (df['high']-df['close'].shift()).abs(),
        (df['low'] -df['close'].shift()).abs()
    ], axis=1).max(axis=1)
    df['atr14'] = tr.rolling(14).mean()
    return df

def main():
    asof = dt.datetime.utcnow().strftime('%Y-%m-%d')
    outdir = os.path.join('data', asof)
    os.makedirs(outdir, exist_ok=True)
    for s in SYMBOLS:
        try:
            df = fetch(s)
            df.to_csv(os.path.join(outdir, f"{s.replace('/','_')}.csv"), index=False)
            print("saved", s, len(df))
            time.sleep(EX.rateLimit/1000 + 0.2)
        except Exception as e:
            print("ERR", s, e)

if __name__ == "__main__":
    main()