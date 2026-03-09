def normalize_rates(data):

    rows = []

    for currency, rate in data["rates"].items():

        rows.append({
            "currency": currency,
            "rate": float(rate),
            "rate_date": data["date"]
        })

    return rows