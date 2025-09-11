# scripts/weekend_research.py
import os, json, glob, math, datetime as dt
import pandas as pd

TS_DIR = "data/time_series"
REPORTS_DIR = "reports"
PORT_DIR = "portfolio"
POS_PATH = os.path.join(PORT_DIR, "positions.json")
ORDERS_PATH = os.path.join(PORT_DIR, "next_orders.json")

os.makedirs(REPORTS_DIR, exist_ok=True)
os.makedirs(PORT_DIR, exist_ok=True)

TODAY = dt.datetime.utcnow().date().isoformat()

def ensure_portfolio():
    if not os.path.exists(POS_PATH):
        port = {"cash": 100000.0, "positions": {}, "nav_history": [], "fills": []}
        with open(POS_PATH, "w") as f:
            json.dump(port, f, indent=2)
    with open(POS_PATH, "r") as f:
        return json.load(f)

def latest_price_symbol_map():
    rows = []

    # 1) priorità: file Binance
    for path in glob.glob("data/ohlc/*__binance_*.csv"):
        df = pd.read_csv(path, parse_dates=["date"]).sort_values("date")
        if df.empty: 
            continue
        last = df.iloc[-1]
        symbol = os.path.basename(path).split("__")[0].upper()
        price = float(last["close"])
        volume = float(last["volume"]) if "volume" in last and pd.notna(last["volume"]) else None

        # arricchisci con mcap/vol dai time_series se esiste
        twin = sorted(glob.glob(f"data/time_series/{symbol}__*.csv"))
        mcap = None
        if twin:
            tdf = pd.read_csv(twin[-1]).sort_values("date")
            tlast = tdf.iloc[-1]
            mcap = float(tlast.get("market_cap_usd")) if pd.notna(tlast.get("market_cap_usd")) else None
            if volume is None and "volume_usd" in tdf.columns:
                volume = float(tlast.get("volume_usd")) if pd.notna(tlast.get("volume_usd")) else None

        rows.append({
            "path": path, "symbol": symbol, "id": None,
            "price": price, "volume": volume, "mcap": mcap, "nrows": len(df),
            "r7": (df["close"].iloc[-1] / df["close"].iloc[-8] - 1) if len(df) > 8 else None,
            "r30": (df["close"].iloc[-1] / df["close"].iloc[-31] - 1) if len(df) > 31 else None,
            "r90": (df["close"].iloc[-1] / df["close"].iloc[-91] - 1) if len(df) > 91 else None,
            "vol20": df["close"].pct_change().tail(20).std() if len(df) >= 21 else None,
        })

    # 2) completa con CG/time_series per le coin rimaste
    seen = {r["symbol"] for r in rows}
    for path in glob.glob("data/ohlc/*.csv"):
        if "__binance_" in path: 
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

        twin = sorted(glob.glob(f"data/time_series/{symbol}__*.csv"))
        mcap = None
        if twin:
            tdf = pd.read_csv(twin[-1]).sort_values("date")
            tlast = tdf.iloc[-1]
            mcap = float(tlast.get("market_cap_usd")) if pd.notna(tlast.get("market_cap_usd")) else None
            if volume is None and "volume_usd" in tdf.columns:
                volume = float(tlast.get("volume_usd")) if pd.notna(tlast.get("volume_usd")) else None

        rows.append({
            "path": path, "symbol": symbol, "id": None,
            "price": price, "volume": volume, "mcap": mcap, "nrows": len(df),
            "r7": (df["close"].iloc[-1] / df["close"].iloc[-8] - 1) if len(df) > 8 else None,
            "r30": (df["close"].iloc[-1] / df["close"].iloc[-31] - 1) if len(df) > 31 else None,
            "r90": (df["close"].iloc[-1] / df["close"].iloc[-91] - 1) if len(df) > 91 else None,
            "vol20": df["close"].pct_change().tail(20).std() if len(df) >= 21 else None,
        })

    # 3) fallback time_series (solo se manca OHLC del simbolo)
    seen = {r["symbol"] for r in rows}
    for path in glob.glob("data/time_series/*.csv"):
        symbol = os.path.basename(path).split("__")[0].upper()
        if symbol in seen:
            continue
        df = pd.read_csv(path, parse_dates=["date"]).sort_values("date")
        if df.empty:
            continue
        last = df.iloc[-1]
        rows.append({
            "path": path, "symbol": symbol, "id": None,
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
        raise SystemExit("No series found (binance/cg/time_series)")
    return df

def compute_nav(portfolio, prices_df):
    px = {r["symbol"]: r["price"] for _, r in prices_df.iterrows()}
    nav = float(portfolio.get("cash", 0.0))
    for sym, qty in portfolio.get("positions", {}).items():
        nav += float(qty) * float(px.get(sym, 0.0))
    return nav

def select_candidates(df):
    # filtri base
    df = df.dropna(subset=["price","mcap","volume"]).copy()
    df = df[(df["mcap"] > 0) & (df["mcap"] < 300_000_000)]
    if df.empty:
        return df
    # liquidità: sopra il 60° percentile
    vol_thr = df["volume"].quantile(0.6)
    df = df[df["volume"] >= vol_thr].copy()
    # punteggio: momentum 7/30 con penalità volatilità
    df["r7"] = df["r7"].fillna(0.0)
    df["r30"] = df["r30"].fillna(0.0)
    df["vol20"] = df["vol20"].fillna(df["vol20"].median() if df["vol20"].notna().any() else 0.0)
    df["score"] = 0.5*df["r7"] + 0.5*df["r30"] - 0.2*df["vol20"]
    df = df.sort_values("score", ascending=False)
    return df

def sizing_plan(nav, cash, candidates, current_positions):
    # Parametri di rischio
    max_new_positions = 8
    risk_per_trade = 0.0125 * nav          # 1.25% NAV per trade
    stop_pct = 0.20                         # stop “teorico” 20%
    max_alloc_per_pos = 0.10 * nav          # non oltre 10% NAV per singola
    min_alloc_per_pos = 0.02 * nav          # minimo 2% NAV
    portfolio_max_positions = 14            # cap posizioni totali

    # quante posizioni nuove possiamo aprire
    room = max(0, portfolio_max_positions - len(current_positions))
    n_to_open = min(max_new_positions, room, len(candidates))

    planned = []
    for _, r in candidates.head(n_to_open).iterrows():
        sym, price = r["symbol"], r["price"]
        # position sizing: dollar_at_risk / stop_distance
        # se vol20 molto alta, riduci dimensione
        vol_k = 1.0 / max(r["vol20"], 1e-4)
        target_risk_dollars = risk_per_trade * min(vol_k, 3.0)
        alloc_usd = target_risk_dollars / max(stop_pct, 1e-6)
        alloc_usd = float(min(max(alloc_usd, min_alloc_per_pos), max_alloc_per_pos, cash * 0.5))  # non spendere oltre 50% della cassa in un colpo
        if alloc_usd < min_alloc_per_pos * 0.6:  # troppo piccolo → salta
            continue
        qty = round(alloc_usd / price, 6)
        planned.append({
            "symbol": sym,
            "side": "BUY",
            "order_type": "MARKET",
            "notional_usd": round(alloc_usd, 2),
            "quantity": qty,
            "stop_loss_pct": stop_pct,
            "take_profit_pct": 0.40,   # TP 40% opzionale
            "notes": f"score={round(r['score'],4)}, r7={round(r['r7'],3)}, r30={round(r['r30'],3)}, vol20={round(r['vol20'],4)}"
        })
        cash -= alloc_usd
        if cash <= nav * 0.02:  # lascia un 2% buffer cash
            break
    return planned

def decide_exits(df, current_positions):
    # vende totalmente se score in bottom del 30% dell’universo filtrato
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
    portfolio = ensure_portfolio()
    price_df = latest_price_symbol_map()
    nav = compute_nav(portfolio, price_df)
    cash = float(portfolio.get("cash", 0.0))

    filtered = select_candidates(price_df)
    buys = sizing_plan(nav, cash, filtered, portfolio.get("positions", {}))
    sells = decide_exits(filtered, portfolio.get("positions", {}))

    orders = sells + buys

    # salva ordini
    with open(ORDERS_PATH, "w") as f:
        json.dump({
            "as_of": TODAY,
            "orders": orders,
            "assumptions": {
                "max_mcap_usd": 300_000_000,
                "liquidity_percentile": 60,
                "risk_per_trade_pct_nav": 1.25,
                "stop_loss_pct_default": 20,
                "take_profit_pct_default": 40
            }
        }, f, indent=2)

    # report markdown
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
        lines.append(top.to_markdown(index=False))
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
    with open(report_path, "w") as f:
        f.write("\n".join(lines))

    print(f"Wrote {ORDERS_PATH} and {report_path}")

if __name__ == "__main__":
    main()
