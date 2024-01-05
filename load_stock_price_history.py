import pandas as pd
import json
from sqlalchemy import text

def load_stock_price_history(engine):
    # Initialize a list to hold the DataFrames
    dfs = []

    # Get the maximum reported_date
    max_date_query = "SELECT MAX(reported_date) FROM stockplot.stock_price_history"
    max_date = pd.read_sql_query(max_date_query, engine).iloc[0, 0]

    # Loop over the range of years
    for x in range(26):  # 26 because range is exclusive of the stop value
        # Calculate the date x years before the max_date
        target_date = max_date - pd.DateOffset(years=x)

        # Fetch the stock_price_history for the nearest date <= target_date and four days before it (to take care of non-trading days)
        query = f"""
        SELECT *
        FROM stockplot.stock_price_history
        WHERE reported_date IN (
            SELECT distinct reported_date
            FROM stockplot.stock_price_history
            WHERE reported_date <= '{target_date.strftime('%Y-%m-%d')}'
            ORDER BY reported_date DESC
            LIMIT 5
        )
        """

        # Use pandas to execute the query and load the data into a DataFrame
        result = pd.read_sql_query(query, engine)

        # Append the DataFrame to the list
        dfs.append(result)

    # Concatenate all the DataFrames in the list
    stock_price_history = pd.concat(dfs)

    # Make sure 'reported_date' is a datetime object
    stock_price_history['reported_date'] = pd.to_datetime(stock_price_history['reported_date'])

    return stock_price_history