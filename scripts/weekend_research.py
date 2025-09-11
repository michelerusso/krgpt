# scripts/weekend_research.py
import os, json, glob, datetime as dt
import pandas as pd

TS_DIR = "data/time_series"
REPORTS_DIR = "reports"
PORT_DIR = "portfolio"
POS_PATH = os.path.join(PORT_DIR, "positions.json")
ORDERS_PATH = os.path.join(PORT_DIR, "next_orders.json")
TODAY = dt.datetime.utcnow().date().isoformat()

os.makedirs(REPORTS_DIR, exist_ok=True)
os.makedirs(PORT_DIR, exist_ok=True)

def cfg():
    # Legge parametri dai env; default robusti
    return {
        "MAX_MCAP_USD": float(os.environ.get("MAX_MCAP_USD", "300000000")),
        "LIQ_PERCENTILE": float(os.environ.get("LIQ_PERCENTILE", "60")),
        "RISK_PER_TRADE_BPS": float(os.environ.get("RISK_PER_TRADE_BPS", "125")),  # 1.25% NAV
        "STOP_LOSS_PCT": float(os.environ.get("STOP_LOSS_PCT", "0.20")),
        "TAKE_PROFIT_PCT": float(os.environ.get("TAKE_PROFIT_PCT", "0.40")),
        "MAX_NEW_POS": int(os.environ.get("MAX_NEW_POS", "8")),
        "MAX_POSITIONS": int(os.environ.get("MAX_POSITIONS", "14")),
        "MAX_ALLOC_PCT": float(os.environ.get("MAX_ALLOC_PCT", "0.10")),
        "MIN_ALLOC_PCT": float(os.environ.get("MIN_ALLOC_PCT", "0.02")),
    }

def df_to_md(df: pd.DataFrame) -> str:
    """Safe markdown table: se 'tabulate' non è installato o pandas fallisce, fai fallback in CSV inline."""
    try:
        return df.to_markdown(index=False)
    except Exception:
        return "```\n" + df.to_csv(index=False) + "\n```"

def ensure_portfolio():
    if not os.path.exists(POS_PATH):
        port = {"cash": 100000.0, "positions": {}, "nav_history": [], "fills": []}
        json.dump(port, open(POS_PATH, "w"), indent=2)
    return json.load(open(POS_PATH, "r"))

# ---- DATI: preferisci Binance OHLCV, poi CG OHLC, poi time_series ----
def latest_price_symbol_map():
    rows = []
    # 1) CCXT (solo kraken)
    for path in glob.glob("data/ohlc/*__ccxt_kraken_*.csv"):
        df = pd.read_csv(path, parse_dates=["date"]).sort_values("date")
        if df.empty:
            continue
        last = df.iloc[-1]
        symbol = os.path.basename(path).split("__")[0].upper()
        price = float(last["close"])
        volume = float(last["volume"]) if "volume" in last and pd.notna(last["volume"]) else None

        twin = sorted(glob.glob(f"{TS_DIR}/{symbol}__*.csv"))
        mcap = None
        if twin:
            tdf = pd.read_csv(twin[-1]).sort_values("date")
            tlast = tdf.iloc[-1]
            mcap = float(tlast.get("market_cap_usd")) if pd.notna(tlast.get("market_cap_usd")) else None
            if volume is None and "volume_usd" in tdf.columns:
                volume = float(tlast.get("volume_usd")) if pd.notna(tlast.get("volume_usd")) else None

        rows.append({
            "symbol": symbol, "price": price, "volume": volume, "mcap": mcap,
            "nrows": len(df),
            "r7": (df["close"].iloc[-1] / df["close"].iloc[-8] - 1) if len(df) > 8 else None,
            "r30": (df["close"].iloc[-1] / df["close"].iloc[-31] - 1) if len(df) > 31 else None,
            "r90": (df["close"].iloc[-1] / df["close"].iloc[-91] - 1) if len(df) > 91 else None,
            "vol20": df["close"].pct_change().tail(20).std() if len(df) >= 21 else None,
        })

    # 2) CG OHLC
    seen = {r["symbol"] for r in rows}
    for path in glob.glob("data/ohlc/*.csv"):
        if "__ccxt_" in path:
            continue
        symbol = os.path.basename(path).split("__")[0].upper()
        if symbol in seen:
            continue
        df = pd.read_csv(path, parse_dates=["date"]).sort_values("date")
        if df.empty:
            continue
        last = df.iloc[-1]
        price = float(last["close"])
        volume = float(last["volume"]) if "volume" in last and pd.notna(last["volume"]) else None

        twin = sorted(glob.glob(f"{TS_DIR}/{symbol}__*.csv"))
        mcap = None
        if twin:
            tdf = pd.read_csv(twin[-1]).sort_values("date")
            tlast = tdf.iloc[-1]
            mcap = float(tlast.get("market_cap_usd")) if pd.notna(tlast.get("market_cap_usd")) else None
            if volume is None and "volume_usd" in tdf.columns:
                volume = float(tlast.get("volume_usd")) if pd.notna(tlast.get("volume_usd")) else None

        rows.append({
            "symbol": symbol, "price": price, "volume": volume, "mcap": mcap,
            "nrows": len(df),
            "r7": (df["close"].iloc[-1] / df["close"].iloc[-8] - 1) if len(df) > 8 else None,
            "r30": (df["close"].iloc[-1] / df["close"].iloc[-31] - 1) if len(df) > 31 else None,
            "r90": (df["close"].iloc[-1] / df["close"].iloc[-91] - 1) if len(df) > 91 else None,
            "vol20": df["close"].pct_change().tail(20).std() if len(df) >= 21 else None,
        })

    # 3) Fallback time_series
    seen = {r["symbol"] for r in rows}
    for path in glob.glob(f"{TS_DIR}/*.csv"):
        symbol = os.path.basename(path).split("__")[0].upper()
        if symbol in seen:
            continue
        df = pd.read_csv(path, parse_dates=["date"]).sort_values("date")
        if df.empty:
            continue
        last = df.iloc[-1]
        rows.append({
            "symbol": symbol,
            "price": float(last["price_usd"]),
            "volume": float(last.get("volume_usd")) if "volume_usd" in df.columns and pd.notna(last.get("volume_usd")) else None,
            "mcap": float(last.get("market_cap_usd")) if pd.notna(last.get("market_cap_usd")) else None,
            "nrows": len(df),
            "r7": (df["price_usd"].iloc[-1] / df["price_usd"].iloc[-8] - 1) if len(df) > 8 else None,
            "r30": (df["price_usd"].iloc[-1] / df["price_usd"].iloc[-31] - 1) if len(df) > 31 else None,
            "r90": (df["price_usd"].iloc[-1] / df["price_usd"].iloc[-91] - 1) if len(df) > 91 else None,
            "vol20": df["price_usd"].pct_change().tail(20).std() if len(df) >= 21 else None,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("No series found (ccxt/cg/time_series)")
    return df


def compute_nav(portfolio, prices_df):
    px = {r["symbol"]: r["price"] for _, r in prices_df.iterrows()}
    nav = float(portfolio.get("cash", 0.0))
    for sym, qty in portfolio.get("positions", {}).items():
        nav += float(qty) * float(px.get(sym, 0.0))
    return nav

def select_candidates(df, C):
    df = df.dropna(subset=["price"]).copy()
    # filtro micro-cap se disponibile
    df = df[df["mcap"].notna() & (df["mcap"] > 0) & (df["mcap"] < C["MAX_MCAP_USD"])]
    if df.empty:
        return df
    # liquidità: percentile su volume (se mancante, riempi con 0)
    df["volume"] = df["volume"].fillna(0.0)
    vol_thr = df["volume"].quantile(C["LIQ_PERCENTILE"]/100.0)
    df = df[df["volume"] >= vol_thr].copy()
    # score: momentum penalizzato per volatilità
    df["r7"] = df["r7"].fillna(0.0)
    df["r30"] = df["r30"].fillna(0.0)
    df["vol20"] = df["vol20"].fillna(df["vol20"].median() if df["vol20"].notna().any() else 0.0)
    df["score"] = 0.5*df["r7"] + 0.5*df["r30"] - 0.2*df["vol20"]
    return df.sort_values("score", ascending=False)

def sizing_plan(nav, cash, candidates, current_positions, C):
    risk_per_trade = (C["RISK_PER_TRADE_BPS"] / 10_000.0) * nav
    stop_pct = C["STOP_LOSS_PCT"]
    max_alloc = C["MAX_ALLOC_PCT"] * nav
    min_alloc = C["MIN_ALLOC_PCT"] * nav
    room = max(0, C["MAX_POSITIONS"] - len(current_positions))
    n_to_open = min(C["MAX_NEW_POS"], room, len(candidates))

    planned = []
    for _, r in candidates.head(n_to_open).iterrows():
        sym, price = r["symbol"], r["price"]
        vol_k = 1.0 / max(r["vol20"], 1e-4)
        target_risk_dollars = risk_per_trade * min(vol_k, 3.0)
        alloc_usd = target_risk_dollars / max(stop_pct, 1e-6)
        alloc_usd = float(min(max(alloc_usd, min_alloc), max_alloc, cash * 0.5))
        if alloc_usd < min_alloc * 0.6:
            continue
        qty = round(alloc_usd / price, 6)
        planned.append({
            "symbol": sym,
            "side": "BUY",
            "order_type": "MARKET",
            "notional_usd": round(alloc_usd, 2),
            "quantity": qty,
            "stop_loss_pct": stop_pct,
            "take_profit_pct": C["TAKE_PROFIT_PCT"],
            "notes": f"score={round(r['score'],4)}, r7={round(r['r7'],3)}, r30={round(r['r30'],3)}, vol20={round(r['vol20'],4)}"
        })
        cash -= alloc_usd
        if cash <= nav * 0.02:
            break
    return planned

def decide_exits(df, current_positions, C):
    if df.empty or not current_positions:
        return []
    cutoff = df["score"].quantile(0.30)
    low = df[df["score"] <= cutoff]["symbol"].tolist()
    exits = []
    for sym in current_positions.keys():
        if sym in low:
            exits.append({
                "symbol": sym,
                "side": "SELL",
                "order_type": "MARKET",
                "quantity": "ALL",
                "notes": "Exit: score in bottom 30% of universe"
            })
    return exits

def main():
    C = cfg()
    portfolio = ensure_portfolio()
    price_df = latest_price_symbol_map()
    nav = compute_nav(portfolio, price_df)
    cash = float(portfolio.get("cash", 0.0))

    filtered = select_candidates(price_df, C)
    buys = sizing_plan(nav, cash, filtered, portfolio.get("positions", {}), C)
    sells = decide_exits(filtered, portfolio.get("positions", {}), C)
    orders = sells + buys

    json.dump({
        "as_of": TODAY,
        "orders": orders,
        "assumptions": C
    }, open(ORDERS_PATH, "w"), indent=2)

    report_path = os.path.join(REPORTS_DIR, f"{TODAY}.md")
    lines = []
    lines.append(f"# Weekend Crypto Research — {TODAY}\n")
    lines.append(f"- **NAV stimato**: ${nav:,.2f}")
    lines.append(f"- **Cash**: ${cash:,.2f}")
    lines.append(f"- **Posizioni correnti**: {len(portfolio.get('positions', {}))}\n")
    if not filtered.empty:
        top = filtered.head(12)[["symbol","price","mcap","volume","r7","r30","vol20","score"]].copy()
        top.columns = ["Symbol","Price","MCap","Volume","R7","R30","Vol20","Score"]
        lines.append("## Top candidati\n")
        lines.append(df_to_md(top))
        lines.append("")
    lines.append("## Ordini proposti\n")
    if orders:
        for o in orders:
            if o["side"] == "BUY":
                lines.append(f"- **{o['side']} {o['symbol']}** ${o['notional_usd']:,.2f} (~{o['quantity']}u), SL {int(o['stop_loss_pct']*100)}%, TP {int(o['take_profit_pct']*100)}% — {o['notes']}")
            else:
                qty = o['quantity']
                lines.append(f"- **{o['side']} {o['symbol']}** qty={qty} — {o['notes']}")
    else:
        lines.append("- Nessun ordine proposto.")
    open(report_path, "w").write("\n".join(lines))
    print(f"Wrote {ORDERS_PATH} and {report_path}")

if __name__ == "__main__":
    main()
