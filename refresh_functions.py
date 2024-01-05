from sqlalchemy import text
import pandas as pd
import datetime
import yfinance as yf

def fetch_last_refresh_timestamp(engine):
    with engine.connect() as connection:
        result = connection.execute(text("select last_refresh_timestamp from admin.app_info"))
        row = result.fetchone()
        return row[0].strftime('%Y-%m-%d %H:%M:%S')

def set_last_refresh_timestamp(engine):
    with engine.connect() as connection:
        connection.execute(text("update admin.app_info set last_refresh_timestamp = current_timestamp"))
        connection.commit()

def daily_refresh_stocks(engine, market_stocks):
    try:
        # Create an empty DataFrame to store all the data
        all_data = pd.DataFrame()

        # Get the date one week ago
        one_week_ago = datetime.datetime.now() - datetime.timedelta(weeks=1)
        delete_qry = f"DELETE FROM stockplot.stock_price_history WHERE reported_date >= '{one_week_ago.date()}'"
        # Delete the entries for the current day
        with engine.begin() as connection:
            connection.execute(text(delete_qry))

        for market, stocks in market_stocks.items():
            for symbol, details in stocks.items():
                # Download the stock data for the last week
                print(details['stock_name'])
                data = yf.download(symbol, start=one_week_ago)
                data = data.reset_index()

                # Add the stock symbol as a column
                data['stock_symbol'] = symbol
                data = data.rename(columns={'Date': 'reported_date', 'High': 'high', 'Low':'low', 'Open':'open','Close':'close', 'Adj Close':'adj_close', 'Volume':'volume'})

                # Concatenate the new data
                all_data = pd.concat([all_data, data])

        # Store the data in PostgreSQL
        all_data.to_sql('stock_price_history', engine, schema='stockplot', if_exists='append', index=False, method='multi',chunksize=5000)
        set_last_refresh_timestamp(engine)
        last_refresh_timestamp = fetch_last_refresh_timestamp(engine)

        # Store the data in PostgreSQL
        return f"Stocks refreshed at {last_refresh_timestamp}"
    except Exception as e:
        return str(e)


def refresh_stock(engine, symbol):
    try:
        # Create an empty DataFrame to store all the data
        all_data = pd.DataFrame()

        # Download the stock data
        data = yf.download(symbol, period='max')
        data = data.reset_index()

        # Add the stock symbol as a column
        data['stock_symbol'] = symbol
        data = data.rename(columns={'Date': 'reported_date', 'High': 'high', 'Low':'low', 'Open':'open','Close':'close', 'Adj Close':'adj_close', 'Volume':'volume'})
        data.to_sql('stock_price_history', engine, if_exists='append', schema='stockplot', index=False, method='multi',chunksize=5000)
        set_last_refresh_timestamp(engine)
        last_refresh_timestamp = fetch_last_refresh_timestamp(engine)

        # Store the data in PostgreSQL
        return f"Stock prices for {symbol} refreshed at { last_refresh_timestamp }"
    except Exception as e:
        return str(e)