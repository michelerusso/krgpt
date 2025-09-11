# scripts/prepare_context.py
import os, glob, json
import pandas as pd

TS_DIR = "data/time_series"
PORT_PATH = "portfolio/positions.json"
OUT = "data/context.json"
TOP_N = int(os.environ.get("CONTEXT_TOP_N", "200"))

def load_portfolio():
    if not os.path.exists(PORT_PATH):
        return {"cash": 100000.0, "positions": {}, "nav_history": [], "fills": []}
    return json.load(open(PORT_PATH, "r"))

def load_universe(top_n=200):
    rows = []
    for p in glob.glob(f"{TS_DIR}/*.csv"):
        df = pd.read_csv(p, parse_dates=["date"]).sort_values("date")
        if df.empty: 
            continue
        last = df.iloc[-1]
        rows.append({
            "symbol": str(last["symbol"]).upper(),
            "id": last.get("id"),
            "name": last.get("name"),
            "price": float(last["price_usd"]),
            "volume_usd": float(last.get("volume_usd")) if pd.notna(last.get("volume_usd")) else None,
            "market_cap_usd": float(last.get("market_cap_usd")) if pd.notna(last.get("market_cap_usd")) else None,
            "r7": (df["price_usd"].iloc[-1] / df["price_usd"].iloc[-8] - 1) if len(df) > 8 else None,
            "r30": (df["price_usd"].iloc[-1] / df["price_usd"].iloc[-31] - 1) if len(df) > 31 else None,
            "vol20": df["price_usd"].pct_change().tail(20).std() if len(df) >= 21 else None,
        })
    U = pd.DataFrame(rows)
    if U.empty:
        return []
    # ordina per volume poi mcap
    U = U.sort_values(["volume_usd","market_cap_usd"], ascending=[False, True])
    return U.head(top_n).to_dict(orient="records")

def main():
    os.makedirs("data", exist_ok=True)
    context = {
        "portfolio": load_portfolio(),
        "universe": load_universe(TOP_N),
        "notes": "Generated context for weekend research. Metrics based on time_series closes."
    }
    json.dump(context, open(OUT, "w"), indent=2)
    print(f"Wrote {OUT} with {len(context['universe'])} assets")

if __name__ == "__main__":
    main()
