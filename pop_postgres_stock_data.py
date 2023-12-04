import yfinance as yf
import pandas as pd
import os
from stocks import ftse_100_stocks
from sqlalchemy import create_engine

def refresh_stocks(engine):
    try:
        # Create an empty DataFrame to store all the data
        all_data = pd.DataFrame()

        for company, symbol in ftse_100_stocks.items():
            # Download the stock data
            data = yf.download(symbol, period='10y')
            data = data.reset_index()

            # Add the stock symbol as a column
            data['stock_symbol'] = symbol
            data = data.rename(columns={'Date':'reported_date'})

            # Append the data to the all_data DataFrame
            all_data = pd.concat([all_data,data])

        # Store the data in PostgreSQL
        all_data.to_sql('stock_price_history', engine, if_exists='replace', index=False)
        return "Stocks refreshed"
    except e as Exception:
        return e