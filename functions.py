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

        # Fetch the stock_price_history for the nearest date <= target_date
        query = f"""
        SELECT *
        FROM stockplot.stock_price_history
        WHERE reported_date = (
            SELECT MAX(reported_date)
            FROM stockplot.stock_price_history
            WHERE reported_date <= '{target_date.strftime('%Y-%m-%d')}'
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
            # Get the data for the latest reported date and the reported date x years ago
            period_data = stock_data[(stock_data['reported_date'] == max_date) |
                                     (stock_data['reported_date'] <= max_date - pd.DateOffset(years=years))].sort_values(
                'reported_date').tail(2)

            if len(period_data) > 0:  # Check if data is available
                percentage_return = round(
                    (period_data['close'].iloc[-1] - period_data['close'].iloc[0]) / period_data['close'].iloc[0] * 100,
                    2)
                stock_returns.append(percentage_return)
            else:
                stock_returns.append(None)

        # Store the returns for this stock in the precalculated_returns dictionary
        precalculated_returns[stock] = stock_returns

    return precalculated_returns

#Create the plot for all of the stocks in the stock portfolio
def load_plots(returns, time_periods, ftse_100_stocks, sp500_stocks, dax_stocks):
    plots = []
    for stock, ret in returns.items():
        hover_text = [
    f"{stock}, {ftse_100_stocks.get(stock) or sp500_stocks.get(stock) or dax_stocks.get(stock)}, {period}, {r}%"
    for period, r in zip(time_periods, ret)
        ]
        trace = go.Scatter(x=list(time_periods.keys()), y=ret, mode='lines+markers', name=stock, hoverinfo='text', text=hover_text)
        plots.append(trace)

    layout = go.Layout(
        title='Stock Returns Over Time',
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


def load_market_stocks(engine):
    metadata = MetaData()
    market_stocks = Table('market_stocks', metadata, autoload_with=engine, schema = 'stockplot')

    Session = sessionmaker(bind=engine)
    session = Session()

    markets = ['FTSE 100', 'DAX', 'S&P 500']
    jsons = {}

    for market in markets:
        sel = select(market_stocks).where(market_stocks.c.market == market)
        result = session.execute(sel).fetchall()

        data = {row[2]: row[1] for row in result}

        jsons[market] = data

    session.close()

    return jsons['FTSE 100'], jsons['DAX'], jsons['S&P 500']

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

def refresh_stocks(engine, ftse_100_stocks, dax_stocks, sp500_stocks):
    try:
        print("truncating stock_price_history")
        delete_qry = f"TRUNCATE TABLE stockplot.stock_price_history"
        # Delete the entries for the current day
        with engine.begin() as connection:
            connection.execute(text(delete_qry))
            connection.commit()

        # Download and insert the data in parallel
        with Pool() as pool:
            pool.map(download_and_insert, [ftse_100_stocks, dax_stocks, sp500_stocks])

        # Store the data in PostgreSQL
        return "Stocks refreshed"
    except Exception as e:
        return str(e)




def daily_refresh_stocks(engine, ftse_100_stocks, dax_stocks, sp500_stocks):
    try:
        # Create an empty DataFrame to store all the data
        all_data = pd.DataFrame()

        # Get the date one week ago
        one_week_ago = datetime.datetime.now() - datetime.timedelta(weeks=1)
        delete_qry = f"DELETE FROM stockplot.stock_price_history WHERE reported_date >= '{one_week_ago.date()}'"
        # Delete the entries for the current day
        with engine.begin() as connection:
            connection.execute(text(delete_qry))

        for symbol, company in ftse_100_stocks.items():
            # Download the stock data for the last week
            print(company)
            data = yf.download(symbol, start=one_week_ago)
            data = data.reset_index()

            # Add the stock symbol as a column
            data['stock_symbol'] = symbol
            data = data.rename(columns={'Date': 'reported_date', 'High': 'high', 'Low':'low', 'Open':'open','Close':'close', 'Adj Close':'adj_close', 'Volume':'volume'})

            # Concatenate the new data
            all_data = pd.concat([all_data, data])

        for symbol, company in dax_stocks.items():
            # Download the stock data for the last week
            print(company)
            data = yf.download(symbol, start=one_week_ago)
            data = data.reset_index()

            # Add the stock symbol as a column
            data['stock_symbol'] = symbol
            data = data.rename(
                columns={'Date': 'reported_date', 'High': 'high', 'Low': 'low', 'Open': 'open', 'Close': 'close',
                         'Adj Close': 'adj_close', 'Volume': 'volume'})

            # Concatenate the new data
            all_data = pd.concat([all_data, data])

        for symbol, company in sp500_stocks.items():
            # Download the stock data for the last week
            print(company)
            data = yf.download(symbol, start=one_week_ago)
            data = data.reset_index()

            # Add the stock symbol as a column
            data['stock_symbol'] = symbol
            data = data.rename(
                columns={'Date': 'reported_date', 'High': 'high', 'Low': 'low', 'Open': 'open', 'Close': 'close',
                         'Adj Close': 'adj_close', 'Volume': 'volume'})

            # Concatenate the new data
            all_data = pd.concat([all_data, data])

        # Store the data in PostgreSQL
        all_data.to_sql('stock_price_history', engine, schema='stockplot', if_exists='append', index=False, method='multi',chunksize=5000)
        return "Stocks refreshed"
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
        return f"Stock prices for {symbol} refreshed"
    except Exception as e:
        return str(e)
