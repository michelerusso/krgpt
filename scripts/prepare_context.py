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

def _safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def _ret(df: pd.DataFrame, col: str, n: int):
    if col not in df.columns or len(df) <= n:
        return None
    try:
        return float(df[col].iloc[-1] / df[col].iloc[-(n+1)] - 1)
    except Exception:
        return None

def load_universe(top_n=200):
    rows = []
    for p in glob.glob(f"{TS_DIR}/*.csv"):
        df = pd.read_csv(p, parse_dates=["date"], dayfirst=False)
        if df.empty:
            continue
        df = df.sort_values("date")

        # colonne attese; gestisci nomi mancanti
        price_col = "price_usd"
        vol_col = "volume_usd" if "volume_usd" in df.columns else ("total_volume" if "total_volume" in df.columns else None)
        mcap_col = "market_cap_usd" if "market_cap_usd" in df.columns else ("market_cap" if "market_cap" in df.columns else None)

        last = df.iloc[-1]
        rows.append({
            "symbol": str(last.get("symbol", "")).upper(),
            "id": last.get("id"),
            "name": last.get("name"),
            "price": _safe_float(last.get(price_col)),
            "volume_usd": _safe_float(last.get(vol_col)) if vol_col else None,
            "market_cap_usd": _safe_float(last.get(mcap_col)) if mcap_col else None,
            "r7": _ret(df, price_col, 7),
            "r30": _ret(df, price_col, 30),
            "vol20": (df[price_col].pct_change().tail(20).std() if price_col in df.columns and len(df) >= 21 else None),
        })

    U = pd.DataFrame(rows)
    if U.empty:
        return []

    # Ordina: più volume prima, micro-cap prima (mcap crescente)
    U["__vol"] = U["volume_usd"].fillna(0.0)
    U["__mcap"] = U["market_cap_usd"].fillna(1e18)
    U = U.sort_values(["__vol", "__mcap"], ascending=[False, True]).drop(columns=["__vol","__mcap"])

    return U.head(top_n).to_dict(orient="records")

def main():
    os.makedirs("data", exist_ok=True)
    context = {
        "portfolio": load_portfolio(),
        "universe": load_universe(TOP_N),
        "notes": "Generated context for weekend research. Metrics based on time_series closes.",
    }
    json.dump(context, open(OUT, "w"), indent=2)
    print(f"Wrote {OUT} with {len(context['universe'])} assets")

if __name__ == "__main__":
    main()
