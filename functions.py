from sqlalchemy import create_engine, text, Table, MetaData, select
from sqlalchemy.orm import sessionmaker
import plotly.graph_objs as go
import plotly.offline as pyo
import pandas as pd, os

def connect_db():
    username = os.getenv('USER')
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
            delete_query = text(f"DELETE FROM user_stocks WHERE email = '{email}'")
            connection.execute(delete_query)

def save_user_stocks(engine, user_stocks):
    # Create and populate a dataframe
    df = pd.DataFrame(user_stocks)
    # Store the user_stocks in the database
    df.to_sql('user_stocks', engine, if_exists='replace', index=False)

def load_all_user_stocks(engine):
    #fetch user's stocks
    query = "select * from user_stocks"

    # Create a connection and execute the query
    with engine.connect() as connection:
        result = connection.execute(text(query))

    # Fetch the resultset
    user_stocks_list = result.fetchall()

    return user_stocks_list

def load_stock_price_history(engine):
    #fetch the stock_price_history from the database
    query = "select * from stock_price_history"

    # Use pandas to execute the query and load the data into a DataFrame
    stock_price_history = pd.read_sql_query(query, engine)

    # Make sure 'reported_date' is a datetime object
    stock_price_history['reported_date'] = pd.to_datetime(stock_price_history['reported_date'])

    return stock_price_history

def load_stock_returns(stock, stock_price_history, time_periods):
    stock_returns = []
    for period, days in time_periods.items():
        # Filter the data for the specific stock
        stock_data = stock_price_history[stock_price_history['stock_symbol'] == stock]

        # Get the latest date for the stock
        max_date = stock_data['reported_date'].max()

        # Get the data for the latest date and the date 'days' ago
        stock_data = stock_data[(stock_data['reported_date'] == max_date) |
                                (stock_data['reported_date'] <= max_date - pd.Timedelta(days=days))].sort_values(
            'reported_date').tail(2)

        if len(stock_data) > 0:  # Check if data is available
            percentage_return = round(
                (stock_data['Close'].iloc[-1] - stock_data['Close'].iloc[0]) / stock_data['Close'].iloc[0] * 100, 2)
            stock_returns.append(percentage_return)
        else:
            stock_returns.append(None)
    return stock_returns

#Create the plot for all of the stocks in the stock portfolio
def load_plots(returns, time_periods, ftse_100_stocks, sp500_stocks, dax_stocks):
    plots = []
    for stock, ret in returns.items():
        hover_text = [
            f"{next((key for key, value in {**ftse_100_stocks, **sp500_stocks, **dax_stocks}.items() if value == stock), 'Unknown')}, {period}, {r}%"
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
    market_stocks = Table('market_stocks', metadata, autoload_with=engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    markets = ['FTSE 100', 'DAX', 'S&P 500']
    jsons = {}

    for market in markets:
        sel = select(market_stocks).where(market_stocks.c.market == market)
        result = session.execute(sel).fetchall()

        data = {row[1]: row[2] for row in result}

        jsons[market] = data

    session.close()

    return jsons['FTSE 100'], jsons['DAX'], jsons['S&P 500']
