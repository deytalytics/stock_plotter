from sqlalchemy import text
import pandas as pd
import datetime
import yfinance as yf
from load_stock_price_history import load_stock_price_history

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

def precalculate_returns(market_stocks, stock_price_history, time_periods):
    # Create dictionaries to store the pre-calculated cumulative and YoY returns for each stock
    precalculated_cumulative_returns = {}
    precalculated_yoy_returns = {}

    for market in market_stocks:
        # Loop over all market_stocks
        for stock in market_stocks[market]:
            # Filter the data for the specific stock
            stock_data = stock_price_history[stock_price_history['stock_symbol'] == stock]

            # Get the latest date for the stock
            max_date = stock_data['reported_date'].max()

            # Create arrays to store the returns for this stock
            stock_cumulative_returns = []
            stock_yoy_returns = []

            # Loop over all time periods
            for period, years in time_periods.items():
                # Get the data for the latest reported date
                latest_data = stock_data[stock_data['reported_date'] == max_date]

                # Get the data for the reported date x years ago or the closest prior trading day
                prior_date = max_date - pd.DateOffset(years=years)
                prior_data = stock_data[stock_data['reported_date'] <= prior_date].sort_values('reported_date').tail(1)

                # Combine the latest and prior data
                period_data = pd.concat([prior_data, latest_data])

                if len(period_data) > 1:  # Check if both latest and prior data are available
                    percentage_return = round(
                        (period_data['adj_close'].iloc[-1] - period_data['adj_close'].iloc[0]) / period_data['adj_close'].iloc[0] * 100,
                        2)
                    stock_cumulative_returns.append(percentage_return)
                else:
                    stock_cumulative_returns.append(None)

                # Calculate YoY returns
                if years > 1:

                    # Get the data for the reported date x-1 years ago or the closest prior trading day
                    prior_date_yoy = max_date - pd.DateOffset(years=years - 1)
                    prior_data_yoy = stock_data[stock_data['reported_date'] <= prior_date_yoy].sort_values(
                        'reported_date').tail(1)

                    # Combine the prior_data_yoy and prior data
                    period_data = pd.concat([prior_data, prior_data_yoy])

                    if len(period_data) > 1:  # Check if both prior_data_yoy and prior data are available
                        percentage_return = round(
                            (period_data['adj_close'].iloc[-1] - period_data['adj_close'].iloc[0]) / period_data['adj_close'].iloc[
                                0] * 100,
                            2)
                        stock_yoy_returns.append(percentage_return)
                    else:
                        stock_yoy_returns.append(None)
                elif years == 1:  # For the first year, YoY return is the same as the cumulative return
                    stock_yoy_returns.append(stock_cumulative_returns[-1])

            # Store the returns for this stock in the precalculated_returns dictionaries
            precalculated_cumulative_returns[stock] = stock_cumulative_returns
            precalculated_yoy_returns[stock] = stock_yoy_returns

    return precalculated_cumulative_returns, precalculated_yoy_returns


def save_returns(engine, tablename, colname, returns):

    # Convert the JSON data to a list of dictionaries
    data_list = [{'stock_symbol': k, 'year': i, colname: v} for k, values in returns.items() for i, v in
                 enumerate(values, start=1)]

    # Convert the list to a pandas DataFrame
    df = pd.DataFrame(data_list)

    # Write the DataFrame to your PostgreSQL table overwriting any previously saved cumulative returns
    df.to_sql(tablename, engine, if_exists='replace', schema="stockplot", index=False)

    with engine.connect() as connection:
        connection.execute(text(f"grant select on table stockplot.{tablename} to stockplot"))

def save_min_max_changes(engine):
    # Execute the query
    with engine.connect() as connection:
        connection.execute(text("truncate table stockplot.min_max_changes"))
        query = f"""
        with daily_change as (
        select stock_symbol, reported_date, adj_close,
        lag(adj_close,1,0) over (partition by stock_symbol order by reported_date) as prev_adj_close, volume
        from stockplot.stock_price_history),
        pct_daily_change as (
        select stock_symbol, reported_date, adj_close, prev_adj_close,
        100*(adj_close - prev_adj_close)/prev_adj_close as change, volume
        from daily_change
        where prev_adj_close<> 0)
        insert into stockplot.min_max_changes
        select stock_symbol, reported_date, adj_close, prev_adj_close, change as min_max_daily_change, volume 
        from pct_daily_change
        where (stock_symbol, change) in (
            select stock_symbol, max(change) 
            from pct_daily_change
            where volume <> 0
			and reported_date <> '2022-06-15'
            group by stock_symbol)
        or (stock_symbol, change) in (
            select stock_symbol, min(change)
            from pct_daily_change
            where volume <> 0
			and reported_date <> '2022-06-14'
            group by stock_symbol)
        """
        connection.execute(text(query))
        connection.commit()

def save_stock_highs(engine):
    # Execute the query
    with engine.connect() as connection:
        connection.execute(text("truncate table stockplot.stock_highs"))
        query = f"""
insert into stockplot.stock_highs        
select s.stock_symbol,ms.stock_name,ms.industry_name, s.high, s.reported_date
from stockplot.stock_price_history s
join (
  select stock_symbol, max(high) as max_high
  from stockplot.stock_price_history
  group by stock_symbol
) m on s.stock_symbol = m.stock_symbol and s.high = m.max_high
join stockplot.market_stocks ms on ms.stock_symbol = s.stock_symbol
        """
        connection.execute(text(query))
        connection.commit()

def refresh_data(engine, market_stocks, time_periods):
    print("Pulling stock prices from Yahoo Finance")
    daily_refresh_stocks(engine, market_stocks)
    print("loading stock_price_history")
    stock_price_history = load_stock_price_history(engine)
    cumulative_returns, yoy_returns = precalculate_returns(market_stocks, stock_price_history, time_periods)
    print("saving cumulative returns")
    save_returns(engine, 'cumulative_returns', 'cumulative_return', cumulative_returns)
    print("saving year on year returns")
    save_returns(engine, 'yoy_returns', 'yoy_return', yoy_returns)
    print("saving min & max daily changes")
    save_min_max_changes(engine)
    print("saving stock highs")
    save_stock_highs(engine)
    print("All daily data refreshed")



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