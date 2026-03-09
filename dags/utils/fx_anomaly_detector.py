def detect_fx_spike(df):

    df["pct_change"] = df["rate"].pct_change()

    anomalies = df[
        df["pct_change"].abs() > 0.05
    ]

    return anomalies