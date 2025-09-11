# scripts/simulator.py
import os, json, glob, datetime as dt
import pandas as pd

TS_DIR = "data/time_series"
PORT_DIR = "portfolio"
POS_PATH = os.path.join(PORT_DIR, "positions.json")
ORDERS_PATH = os.path.join(PORT_DIR, "next_orders.json")
FILLS_DIR = os.path.join(PORT_DIR, "fills")
os.makedirs(PORT_DIR, exist_ok=True)
os.makedirs(FILLS_DIR, exist_ok=True)

TODAY = dt.datetime.utcnow().date().isoformat()

SLIPPAGE_BPS = int(os.environ.get("SLIPPAGE_BPS", "25"))  # 0.25%
FEE_BPS = int(os.environ.get("FEE_BPS", "10"))            # 0.10%

def load_portfolio():
    if not os.path.exists(POS_PATH):
        return {"cash": 100000.0, "positions": {}, "nav_history": [], "fills": []}
    return json.load(open(POS_PATH, "r"))

def latest_price(symbol):
    sym = symbol.upper()
    # 1) CCXT Kraken soltanto
    ccxt_paths = sorted(glob.glob(f"data/ohlc/{sym}__ccxt_kraken_*.csv"))
    if ccxt_paths:
        df = pd.read_csv(ccxt_paths[-1]).sort_values("date")
        row = df.iloc[-1]
        return float(row["close"]), str(row["date"])
    # 2) CoinGecko OHLC
    cg_paths = [p for p in sorted(glob.glob(f"data/ohlc/{sym}__*.csv")) if "__ccxt_" not in p]
    if cg_paths:
        df = pd.read_csv(cg_paths[-1]).sort_values("date")
        row = df.iloc[-1]
        return float(row["close"]), str(row["date"])
    # 3) Fallback time_series
    ts_paths = sorted(glob.glob(f"data/time_series/{sym}__*.csv"))
    if ts_paths:
        df = pd.read_csv(ts_paths[-1]).sort_values("date")
        row = df.iloc[-1]
        return float(row["price_usd"]), str(row["date"])
    raise ValueError(f"No price data for {symbol}")

def compute_nav(port):
    nav = port.get("cash", 0.0)
    for sym, qty in port.get("positions", {}).items():
        try:
            px, _ = latest_price(sym)
        except Exception:
            px = 0.0
        nav += qty * px
    return float(nav)

def apply_orders():
    if not os.path.exists(ORDERS_PATH):
        print("No next_orders.json; nothing to do.")
        return False

    orders_blob = json.load(open(ORDERS_PATH, "r"))
    orders = orders_blob.get("orders", [])
    if not orders:
        print("Orders list empty.")
        return False

    port = load_portfolio()
    fills = []

    for o in orders:
        sym = o["symbol"].upper()
        side = o["side"].upper()
        order_type = o.get("order_type", "MARKET").upper()

        px, px_date = latest_price(sym)
        # slippage & fees (bps)
        eff_px = px * (1 + SLIPPAGE_BPS/10_000) if side == "BUY" else px * (1 - SLIPPAGE_BPS/10_000)
        fee_rate = FEE_BPS/10_000

        if side == "BUY":
            notional = float(o.get("notional_usd") or 0.0)
            qty = float(o.get("quantity") or 0.0)
            if notional and not qty:
                qty = round(notional / eff_px, 6)
            elif qty and not notional:
                notional = qty * eff_px
            elif notional == 0 and qty == 0:
                continue

            cost = qty * eff_px
            fee = cost * fee_rate
            total = cost + fee
            if port["cash"] < total:
                print(f"Skip BUY {sym}: insufficient cash")
                continue
            port["cash"] -= total
            port["positions"][sym] = round(port["positions"].get(sym, 0.0) + qty, 6)

            fills.append({
                "date": TODAY,
                "symbol": sym,
                "side": "BUY",
                "qty": qty,
                "price": round(eff_px, 8),
                "fee": round(fee, 6)
            })

        elif side == "SELL":
            qty_req = o.get("quantity")
            pos_qty = float(port["positions"].get(sym, 0.0))
            if qty_req == "ALL":
                qty = pos_qty
            else:
                qty = float(qty_req or 0.0)
            if qty <= 0 or pos_qty <= 0:
                continue
            qty = min(qty, pos_qty)
            proceeds = qty * eff_px
            fee = proceeds * fee_rate
            port["cash"] += proceeds - fee
            new_qty = round(pos_qty - qty, 6)
            if new_qty <= 0:
                port["positions"].pop(sym, None)
            else:
                port["positions"][sym] = new_qty

            fills.append({
                "date": TODAY,
                "symbol": sym,
                "side": "SELL",
                "qty": qty,
                "price": round(eff_px, 8),
                "fee": round(fee, 6)
            })

    # salva fills su file giornaliero e su positions.json
    if fills:
        fills_df = pd.DataFrame(fills)
        out_path = os.path.join(FILLS_DIR, f"{TODAY}.csv")
        if os.path.exists(out_path):
            old = pd.read_csv(out_path)
            fills_df = pd.concat([old, fills_df], ignore_index=True)
        fills_df.to_csv(out_path, index=False)
        port.setdefault("fills", []).extend(fills)

    # aggiorna NAV history
    nav = compute_nav(port)
    port.setdefault("nav_history", []).append({"date": TODAY, "nav": nav, "cash": port["cash"]})

    with open(POS_PATH, "w") as f:
        json.dump(port, f, indent=2)

    # una volta applicati, rimuovi next_orders per evitare doppi fill
    os.remove(ORDERS_PATH)
    print(f"Applied {len(fills)} fills; NAV now {nav:,.2f}")
    return True

if __name__ == "__main__":
    apply_orders()
