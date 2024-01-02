from sqlalchemy import create_engine, text, Table, MetaData, select
from sqlalchemy.orm import sessionmaker
from multiprocessing import Pool
import plotly.graph_objs as go
import plotly.offline as pyo
import pandas as pd, os, datetime
import yfinance as yf


def connect_db(user=None):
    if user is None:
        username = os.getenv('USER')
    else:
        username = user
    password = os.getenv('PASSWORD')
    engine = create_engine(
        f'postgresql://{username}:{password}@postgres-srvr.postgres.database.azure.com/data_product_metadata')
    return engine

def delete_user_stocks(email,engine):
    # Establish a connection
    with engine.connect() as connection:
        # Start a new transaction
        with connection.begin():
            # Delete the existing rows for the given email address
            delete_query = text(f"DELETE FROM admin.user_stocks WHERE email = '{email}'")
            connection.execute(delete_query)

def save_user_stocks(engine, user_stocks):
    # Create and populate a dataframe
    df = pd.DataFrame(user_stocks)
    # Store the user_stocks in the database
    df.to_sql('user_stocks', engine, if_exists='replace', index=False,schema='admin')

def load_all_user_stocks(engine):
    #fetch user's stocks
    query = "select * from admin.user_stocks"

    # Create a connection and execute the query
    with engine.connect() as connection:
        result = connection.execute(text(query))

    # Fetch the resultset
    user_stocks_list = result.fetchall()

    return user_stocks_list

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

def precalculate_returns(stock_price_history, time_periods):
    # Create a dictionary to store the pre-calculated returns for each stock
    precalculated_returns = {}

    # Loop over all unique stocks in the stock_price_history
    for stock in stock_price_history['stock_symbol'].unique():
        # Filter the data for the specific stock
        stock_data = stock_price_history[stock_price_history['stock_symbol'] == stock]

        # Get the latest date for the stock
        max_date = stock_data['reported_date'].max()

        # Create an array to store the returns for this stock
        stock_returns = []

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
                stock_returns.append(percentage_return)
            else:
                stock_returns.append(None)

        # Store the returns for this stock in the precalculated_returns dictionary
        precalculated_returns[stock] = stock_returns

    return precalculated_returns

def fetch_last_refresh_timestamp():
    engine = connect_db()
    with engine.connect() as connection:
        result = connection.execute(text("select last_refresh_timestamp from admin.app_info"))
        row = result.fetchone()
        return row[0].strftime('%Y-%m-%d %H:%M:%S')

def set_last_refresh_timestamp():
    engine = connect_db()
    with engine.connect() as connection:
        connection.execute(text("update admin.app_info set last_refresh_timestamp = current_timestamp"))
        connection.commit()

#Create the plot for all of the stocks in the stock portfolio
def load_plots(returns, ftse100_stocks, dax_stocks, sp500_stocks):
    plots = []
    time_periods = {f"{i}y": i for i in range(1, 26)}
    for stock, ret in returns.items():
        hover_text = [
    f"{stock}, {ftse100_stocks.get(stock) or dax_stocks.get(stock) or sp500_stocks.get(stock)}, {period}, {r}%"
    for period, r in zip(time_periods, ret)
        ]
        trace = go.Scatter(x=list(time_periods.keys()), y=ret, mode='lines+markers', name=stock, hoverinfo='text', text=hover_text)
        plots.append(trace)


    last_refresh_timestamp = fetch_last_refresh_timestamp()

    layout = go.Layout(
        title=f'Stock Returns Over Time (last updated { last_refresh_timestamp } for close prices on last trading day)',
        xaxis=dict(title='Time Period'),
        yaxis=dict(title='%age Return')
    )
    fig = go.Figure(data=plots, layout=layout)
    plot_div = pyo.plot(fig, output_type='div')
    return plot_div

def get_username(session):
    user = session.get('user','')
    if 'name' in user:
        username=user['name']+" <a href='/logout'>Logout</a>"
    else:
        username=f"<a href='/login'>Login</a>"
    return username

def get_email(session):
    user = session.get('user','')
    if 'email' in user:
        email=user['email']
    else:
        email = None
    return email


from sqlalchemy import create_engine, text

def load_market_stocks(engine):
    with engine.connect() as connection:
        result = connection.execute(text("SELECT * FROM stockplot.market_stocks"))

        jsons = {}
        for row in result:
            market = row[0]
            stock_name = row[1]
            stock_symbol = row[2]
            industry_name = row[3]

            if market not in jsons:
                jsons[market] = {}

            jsons[market][stock_symbol] = {'stock_name': stock_name, 'industry_name': industry_name}

    return jsons

def download_and_insert(stock_dict):
    all_data = pd.DataFrame()
    for symbol, company in stock_dict.items():
        try:
            # Download the stock data
            data = yf.download(symbol, period='max')
            data = data.reset_index()

            # Add the stock symbol as a column
            data['stock_symbol'] = symbol
            data = data.rename(columns={'Date': 'reported_date', 'High': 'high', 'Low':'low', 'Open':'open','Close':'close', 'Adj Close':'adj_close', 'Volume':'volume'})

            # Append the data to the all_data DataFrame
            all_data = pd.concat([all_data, data], ignore_index=True)
        except Exception as e:
            print(str(e))

    # Insert all_data into the database
    try:
        engine = connect_db()
        all_data.to_sql('stock_price_history', engine, if_exists='append', schema='stockplot', index=False, method='multi', chunksize=5000)
    except Exception as e:
        print(str(e))

def refresh_stocks(engine, market_stocks):
    try:
        print("truncating stock_price_history")
        delete_qry = f"TRUNCATE TABLE stockplot.stock_price_history"
        # Delete the entries for the current day
        with engine.begin() as connection:
            connection.execute(text(delete_qry))
            connection.commit()

        # Download and insert the data in parallel
        with Pool() as pool:
            pool.map(download_and_insert, [market_stocks])

        set_last_refresh_timestamp()
        last_refresh_timestamp = fetch_last_refresh_timestamp()

        # Store the data in PostgreSQL
        return f"Stocks refreshed at { last_refresh_timestamp }"
    except Exception as e:
        return str(e)

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
        set_last_refresh_timestamp()
        last_refresh_timestamp = fetch_last_refresh_timestamp()

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
        set_last_refresh_timestamp()
        last_refresh_timestamp = fetch_last_refresh_timestamp()

        # Store the data in PostgreSQL
        return f"Stock prices for {symbol} refreshed at { last_refresh_timestamp }"
    except Exception as e:
        return str(e)

def load_stock_data(engine):
    time_periods = {f"{i}y": i for i in range(1, 26)}
    print("Loading user stocks")
    user_stocks = load_all_user_stocks(engine)
    print("Loading stock price history")
    stock_price_history = load_stock_price_history(engine)
    print("Precalculating returns")
    precalculated_returns = precalculate_returns(stock_price_history, time_periods)
    print("Stocks, price history and precalculated_returns all loaded")
    return user_stocks, stock_price_history, precalculated_returns
