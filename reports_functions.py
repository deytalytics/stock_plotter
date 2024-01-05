import pandas as pd
import json
from sqlalchemy import text


def load_returns(engine,tablename, colname):

    # Query the data into a DataFrame
    df = pd.read_sql(f'SELECT * FROM stockplot.{tablename}', engine)

    # Group by stock_symbol and apply list to colname
    df_grouped = df.groupby('stock_symbol')[colname].apply(list)

    # Convert the grouped DataFrame to a dictionary
    data_dict = df_grouped.to_dict()

    # Convert the dictionary to a JSON string
    data_json = json.dumps(data_dict)

    return data_json


def load_min_max_changes(engine):

    # Query the data into a DataFrame
    df = pd.read_sql(f"SELECT stock_symbol,to_char(reported_date,'YYYY-MM-DD') as reported_date, close, prev_close, min_max_daily_change FROM stockplot.min_max_changes", engine)

    return df

