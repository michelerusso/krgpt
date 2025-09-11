import json, os, datetime as dt
import requests, pandas as pd

DATE = dt.datetime.utcnow().date().isoformat()
os.makedirs("data/daily", exist_ok=True)
os.makedirs("portfolio", exist_ok=True)

# 1) Fetch universe & quotes da CoinGecko (esempio semplice)
def cg(path, params=None):
    return requests.get(f"https://api.coingecko.com/api/v3/{path}", params=params or {}, timeout=30).json()

market = cg("coins/markets", {
    "vs_currency": "usd", "order": "volume_desc", "per_page": 250, "page": 1,
    "price_change_percentage": "24h"
})

# filtro micro-cap < $300M
universe = [c for c in market if (c.get("market_cap") or 0) < 300_000_000]

with open(f"data/daily/{DATE}.json", "w") as f:
    json.dump(universe, f, indent=2)

# 2) Simulazione portafoglio
pos_path = "portfolio/positions.json"
portfolio = {"cash": 100000.0, "positions": {}, "nav_history": []}
if os.path.exists(pos_path):
    portfolio = json.load(open(pos_path))

prices = {c["symbol"].upper(): c["current_price"] for c in universe}

# ricalcola NAV
nav = portfolio["cash"] + sum(qty * prices.get(sym, 0) for sym, qty in portfolio["positions"].items())
portfolio["nav_history"].append({"date": DATE, "nav": nav})

json.dump(portfolio, open(pos_path, "w"), indent=2)
