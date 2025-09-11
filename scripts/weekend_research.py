import os, json, glob, datetime as dt
import pandas as pd

# 1) Carica dati recenti
files = sorted(glob.glob("data/daily/*.json"))[-7:]
universe = []
for fp in files:
    universe.extend(json.load(open(fp)))

df = pd.DataFrame(universe).drop_duplicates(subset=["id"])  # ultimo valore per coin

# 2) Screening semplice
liquid = df[df["total_volume"] > df["total_volume"].quantile(0.6)]
candidates = liquid.sort_values("price_change_percentage_24h_in_currency", ascending=False).head(10)

# 3) Piano (esempio placeholder)
orders = [{"side": "BUY", "symbol": r["symbol"].upper(), "usd": 5000} for _, r in candidates.iterrows()]

# 4) Aggiorna portafoglio simulato
pos = json.load(open("portfolio/positions.json"))
for o in orders:
    price = df[df["symbol"].str.upper() == o["symbol"]]["current_price"].iloc[0]
    qty = round(o["usd"] / price, 6)
    pos["cash"] -= o["usd"]
    pos["positions"][o["symbol"]] = pos["positions"].get(o["symbol"], 0) + qty

json.dump(pos, open("portfolio/positions.json", "w"), indent=2)

# 5) Report
os.makedirs("reports", exist_ok=True)
today = dt.datetime.utcnow().date().isoformat()
with open(f"reports/{today}.md", "w") as f:
    f.write("# Weekend Crypto Research\n\n")
    f.write("## Nuove proposte & ordini simulati\n")
    for o in orders:
        f.write(f"- {o['side']} {o['symbol']} per ${o['usd']}\n")
