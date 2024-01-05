import plotly.graph_objs as go
import plotly.offline as pyo
import os

from reports_functions import *
from refresh_functions import *
from load_stock_price_history import load_stock_price_history
from sqlalchemy import create_engine, text

def connect_db(user=None):
    if user is None:
        username = os.getenv('USER')
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

def load_stock_data(engine):
    time_periods = {f"{i}y": i for i in range(1, 26)}
    print("Loading user stocks")
    user_stocks = load_all_user_stocks(engine)
    print("Loading stock price history")
    stock_price_history = load_stock_price_history(engine)
    print("Loading cumulative and year on year returns")
    cumulative_returns = json.loads(load_returns(engine,'cumulative_returns','cumulative_return'))
    yoy_returns = json.loads(load_returns(engine,'yoy_returns','yoy_return'))
    print("Loading stock min & max daily changes")
    min_max_changes = load_min_max_changes(engine)
    print("Stocks, price history and precalculated_returns all loaded")
    return user_stocks, stock_price_history, cumulative_returns, yoy_returns, min_max_changes


#Create the plot for all of the stocks in the stock portfolio
def load_plots(engine, returns, ftse100_stocks, dax_stocks, sp500_stocks):
    plots = []
    time_periods = {f"{i}y": i for i in range(1, 26)}
    for stock, ret in returns.items():
        hover_text = [
    f"{stock}, {ftse100_stocks.get(stock) or dax_stocks.get(stock) or sp500_stocks.get(stock)}, {period}, {r}%"
    for period, r in zip(time_periods, ret)
        ]
        trace = go.Scatter(x=list(time_periods.keys()), y=ret, mode='lines+markers', name=stock, hoverinfo='text', text=hover_text)
        plots.append(trace)


    last_refresh_timestamp = fetch_last_refresh_timestamp(engine)

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





