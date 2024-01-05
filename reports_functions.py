import pandas as pd
import json
from sqlalchemy import text


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
                        (period_data['close'].iloc[-1] - period_data['close'].iloc[0]) / period_data['close'].iloc[0] * 100,
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
                            (period_data['close'].iloc[-1] - period_data['close'].iloc[0]) / period_data['close'].iloc[
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


def save_min_max_changes(engine):
    # Execute the query
    with engine.connect() as connection:
        connection.execute(text("truncate table stockplot.min_max_changes"))
        query = f"""
        with daily_change as (
        select stock_symbol, reported_date, close,
        lag(close,1,0) over (partition by stock_symbol order by reported_date) as prev_close
        from stockplot.stock_price_history),
        pct_daily_change as (
        select stock_symbol, reported_date, close, prev_close,
        100*(close - prev_close)/prev_close as change
        from daily_change
        where prev_close<> 0)
        insert into stockplot.min_max_changes
        select stock_symbol, reported_date, close, prev_close, change as min_max_daily_change 
        from pct_daily_change
        where (stock_symbol, change) in (
            select stock_symbol, max(change) 
            from pct_daily_change
            where reported_date <> '2022-06-15'
            group by stock_symbol)
        or (stock_symbol, change) in (
            select stock_symbol, min(change) 
            from pct_daily_change
            where reported_date <> '2022-06-14'
            group by stock_symbol)
        """
        connection.execute(text(query))
        connection.commit()


def load_min_max_changes(engine):

    # Query the data into a DataFrame
    df = pd.read_sql(f"SELECT stock_symbol,to_char(reported_date,'YYYY-MM-DD') as reported_date, close, prev_close, min_max_daily_change FROM stockplot.min_max_changes", engine)

    return df

