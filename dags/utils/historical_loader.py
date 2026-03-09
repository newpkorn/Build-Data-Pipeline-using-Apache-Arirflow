import requests

def load_historical(start, end):

    url = f"https://api.exchangerate.host/timeseries?start_date={start}&end_date={end}&base=THB"

    r = requests.get(url)

    return r.json()