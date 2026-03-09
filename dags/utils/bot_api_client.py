import requests
import pandas as pd

BOT_API = "https://api.exchangerate.host/latest?base=THB"

def fetch_bot_rates():

    r = requests.get(BOT_API)

    data = r.json()

    rows = []

    for currency, rate in data["rates"].items():

        rows.append({
            "rate_date": data["date"],
            "currency": currency,
            "rate": rate
        })

    return pd.DataFrame(rows)