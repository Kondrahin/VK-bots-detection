import numpy as np
import pandas as pd


def fill_blank_dataframe_from_dict(data: list[dict]):
    blank_df = pd.read_csv("app/blank_df.csv")
    blank_df["counters"] = np.nan

    for index, row in enumerate(data):
        for key, values in row.items():
            # Pandas lib bug
            if key == "counters" or type(values) == dict or type(values) == list:
                values = str(values)
            blank_df.at[index, key] = values
    return blank_df
