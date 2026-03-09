import requests
import pandas as pd

def fetch_exchange_rates(api_url: str) -> pd.DataFrame:
    """
    Fetches exchange rates from a generic API and returns a pandas DataFrame.

    :param api_url: The URL of the exchange rate API.
    :return: A DataFrame with exchange rate data.
    """
    response = requests.get(api_url)
    response.raise_for_status()  # Raise an exception for bad status codes

    data = response.json()

    if "rates" not in data or "date" not in data:
        raise KeyError("'rates' or 'date' key not found in API response")

    rows = []
    for currency, rate in data["rates"].items():
        rows.append({
            "rate_date": data["date"],
            "currency_code": currency,
            "rate": rate
        })

    return pd.DataFrame(rows)