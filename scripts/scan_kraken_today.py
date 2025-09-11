# scripts/scan_kraken_today.py
import os, glob, json, datetime as dt
import pandas as pd

TODAY = dt.datetime.utcnow().date().isoformat()
OUT_REPORT = f"reports/kraken_scan_{TODAY}.md"
ORDERS_PATH = "portfolio/next_orders.json"
TS_DIR = "data/time_series"
OHLC_DIR = "data/ohlc"
MAP_PATH = "data/exchange_map/kraken_map.csv"

# ---- parametri base (puoi trasformarli in env) ----
MAX_MCAP_USD = float(os.environ.get("MAX_MCAP_USD", "300000000"))
MIN_DOLLAR_VOL = float(os.environ.get("MIN_DOLLAR_VOL", "75000"))  # volume $ minimo su Kraken
RISK_PER_TRADE_BPS = float(os.environ.get("RISK_PER_TRADE_BPS", "125"))  # 1.25% NAV
STOP_LOSS_PCT = float(os.environ.get("STOP_LOSS_PCT", "0.20"))
TAKE_PROFIT_PCT = float(os.environ.get("TAKE_PROFIT_PCT", "0.40"))
MAX_ALLOC_PCT = float(os.environ.get("MAX_ALLOC_PCT", "0.10"))
MIN_ALLOC_PCT = float(os.environ.get("MIN_ALLOC_PCT", "0.02"))
MAX_NEW_POS = int(os.environ.get("MAX_NEW_POS", "6"))
MAX_POSITIONS = int(os.environ.get("MAX_POSITIONS", "14"))

def ensure_dirs():
    os.makedirs("reports", exist_ok=True)
    os.makedirs("portfolio", exist_ok=True)

def load_portfolio():
    p = "portfolio/positions.json"
    if not os.path.exists(p):
        return {"cash": 100000.0, "positions": {}, "nav_history": [], "fills": []}
    return json.load(open(p, "r"))

def load_ts_last(symbol):
    paths = sorted(glob.glob(f"{TS_DIR}/{symbol.upper()}__*.csv"))
    if not paths:
        return {}
    df = pd.read_csv(paths[-1]).sort_values("date")
    last = df.iloc[-1]
    vcol = "volume_usd" if "volume_usd" in df.columns else ("total_volume" if "total_volume" in df.columns else None)
    mcol = "market_cap_usd" if "market_cap_usd" in df.columns else ("market_cap" if "market_cap" in df.columns else None)
    return {
        "mcap": float(last.get(mcol)) if mcol and pd.notna(last.get(mcol)) else None,
        "vol_usd": float(last.get(vcol)) if vcol and pd.notna(last.get(vcol)) else None,
    }

def iter_kraken_files():
    for p in glob.glob(f"{OHLC_DIR}/*__ccxt_kraken_*.csv"):
        symbol = os.path.basename(p).split("__")[0].upper()
        yield symbol, p

def build_universe():
    rows = []
    for sym, path in iter_kraken_files():
        df = pd.read_csv(path, parse_dates=["date"])
        if df.empty or "close" not in df.columns:
            continue
        df = df.sort_values("date")
        last = df.iloc[-1]
        # volume CCXT è in base units; approx $ = close * volume
        vol_usd_est = float(last["close"]) * float(last.get("volume", 0.0))
        r7 = (df["close"].iloc[-1] / df["close"].iloc[-8] - 1) if len(df) > 8 else None
        r30 = (df["close"].iloc[-1] / df["close"].iloc[-31] - 1) if len(df) > 31 else None
        vol20 = df["close"].pct_change().tail(20).std() if len(df) >= 21 else None

        ts = load_ts_last(sym)
        rows.append({
            "symbol": sym,
            "price": float(last["close"]),
            "mcap": ts.get("mcap"),
            "kraken_dollar_vol": vol_usd_est,
            "r7": r7, "r30": r30, "vol20": vol20,
        })
    U = pd.DataFrame(rows)
    if U.empty:
        return U
    # filtri micro-cap + liquidità su kraken
    U = U[(U["mcap"].notna()) & (U["mcap"] > 0) & (U["mcap"] < MAX_MCAP_USD)]
    U = U[U["kraken_dollar_vol"] >= MIN_DOLLAR_VOL].copy()
    if U.empty:
        return U
    U["r7"] = U["r7"].fillna(0.0)
    U["r30"] = U["r30"].fillna(0.0)
    U["vol20"] = U["vol20"].fillna(U["vol20"].median() if U["vol20"].notna().any() else 0.0)
    U["score"] = 0.6*U["r7"] + 0.4*U["r30"] - 0.2*U["vol20"]
    return U.sort_values("score", ascending=False)

def sizing_plan(port, ranked):
    nav = float(port.get("cash", 0.0))
    for s, q in port.get("positions", {}).items():
        # usa ultimo prezzo disponibile dai file kraken
        paths = sorted(glob.glob(f"{OHLC_DIR}/{s}__ccxt_kraken_*.csv"))
        if paths:
            df = pd.read_csv(paths[-1]).sort_values("date")
            if not df.empty:
                nav += float(q) * float(df["close"].iloc[-1])

    risk_per_trade = (RISK_PER_TRADE_BPS/10000.0) * nav
    max_alloc = MAX_ALLOC_PCT * nav
    min_alloc = MIN_ALLOC_PCT * nav
    room = max(0, MAX_POSITIONS - len(port.get("positions", {})))
    n_to_open = min(MAX_NEW_POS, room, len(ranked))

    orders = []
    cash = float(port.get("cash", 0.0))
    for _, r in ranked.head(n_to_open).iterrows():
        vol_k = 1.0 / max(r["vol20"], 1e-4)
        target_risk_dollars = risk_per_trade * min(vol_k, 3.0)
        alloc = target_risk_dollars / max(STOP_LOSS_PCT, 1e-6)
        alloc = float(min(max(alloc, min_alloc), max_alloc, cash * 0.5))
        if alloc < min_alloc * 0.6:
            continue
        qty = round(alloc / r["price"], 6)
        orders.append({
            "symbol": r["symbol"],
            "side": "BUY",
            "order_type": "MARKET",
            "notional_usd": round(alloc, 2),
            "quantity": qty,
            "stop_loss_pct": STOP_LOSS_PCT,
            "take_profit_pct": TAKE_PROFIT_PCT,
            "notes": f"score={round(r['score'],4)}, r7={round(r['r7'],3)}, r30={round(r['r30'],3)}, vol20={round(r['vol20'],4)}, vol_kraken_usd≈{int(r['kraken_dollar_vol'])}"
        })
        cash -= alloc
        if cash <= nav * 0.02:
            break
    return orders, nav

def df_to_md(df):
    try:
        return df.to_markdown(index=False)
    except Exception:
        return "```\n" + df.to_csv(index=False) + "\n```"

def main():
    ensure_dirs()
    port = load_portfolio()
    ranked = build_universe()

    lines = [f"# Kraken ALT scan — {TODAY}"]
    if ranked.empty:
        lines += ["\nNessun candidato (controlla che i file `data/ohlc/*__ccxt_kraken_*.csv` esistano)."]
        json.dump({"as_of": TODAY, "orders": [], "assumptions": {}}, open(ORDERS_PATH, "w"), indent=2)
    else:
        view = ranked.head(12)[["symbol","price","mcap","kraken_dollar_vol","r7","r30","vol20","score"]].copy()
        view.columns = ["Symbol","Price","MCap","KrakenVol$","R7","R30","Vol20","Score"]
        lines += ["\n## Top 12 per score\n", df_to_md(view), ""]
        orders, nav = sizing_plan(port, ranked)
        json.dump({"as_of": TODAY, "orders": orders, "assumptions": {
            "MAX_MCAP_USD": MAX_MCAP_USD, "MIN_DOLLAR_VOL": MIN_DOLLAR_VOL,
            "RISK_PER_TRADE_BPS": RISK_PER_TRADE_BPS, "STOP_LOSS_PCT": STOP_LOSS_PCT,
            "TAKE_PROFIT_PCT": TAKE_PROFIT_PCT, "MAX_NEW_POS": MAX_NEW_POS,
            "MAX_POSITIONS": MAX_POSITIONS, "MAX_ALLOC_PCT": MAX_ALLOC_PCT, "MIN_ALLOC_PCT": MIN_ALLOC_PCT
        }}, open(ORDERS_PATH, "w"), indent=2)

        lines += ["## Ordini proposti\n"]
        if orders:
            for o in orders:
                lines.append(f"- **{o['side']} {o['symbol']}** ${o['notional_usd']:,.2f} (~{o['quantity']}u), SL {int(o['stop_loss_pct']*100)}%, TP {int(o['take_profit_pct']*100)}% — {o['notes']}")
        else:
            lines.append("- Nessun ordine proposto (vincoli liquidità/rischio).")

    open(OUT_REPORT, "w").write("\n".join(lines))
    print(f"Report: {OUT_REPORT} | Orders: {ORDERS_PATH}")

if __name__ == "__main__":
    mai
