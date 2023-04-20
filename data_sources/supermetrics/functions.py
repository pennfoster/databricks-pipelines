import pandas as pd


def get_url_dataframe(env):
    df = pd.read_csv(
        f"/dbfs/mnt/sadataraw{env}001_control/supermetrics/urls_for_apis.csv"
    )
    df = df[df["C001_SearchName"].str.startswith("-") == False]
    df.reset_index(drop=True, inplace=True)
    return df
