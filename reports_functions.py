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
    df = pd.read_sql(f"SELECT stock_symbol,to_char(reported_date,'YYYY-MM-DD') as reported_date, round(close::numeric,2) as close, round(prev_close::numeric,2) as prev_close, round(min_max_daily_change::numeric,2) as min_max_pct_daily_change FROM stockplot.min_max_changes", engine)

    return df

def load_stock_highs(engine):

    # Query the data into a DataFrame
    df = pd.read_sql(f"SELECT stock_symbol,stock_name, to_char(reported_date,'YYYY-MM-DD') as reported_date, round(high::numeric,2) as high  FROM stockplot.stock_highs order by to_char(reported_date,'YYYY-MM-DD') desc", engine)

    return df

