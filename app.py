from flask import Flask, request, session, render_template, redirect
from sqlalchemy import create_engine, text, Table, MetaData, select
from sqlalchemy.orm import sessionmaker
from authlib.integrations.flask_client import OAuth
import os, uuid, json
import plotly.graph_objs as go
import plotly.offline as pyo
import pandas as pd
from pop_postgres_stock_data import refresh_stocks

def create_app():

    app = Flask(__name__)
    app.config.from_object('config')
    CONF_URL = 'https://accounts.google.com/.well-known/openid-configuration'

    # Initialize oauth with the app
    oauth.init_app(app)

    oauth.register(
        name='google',
        server_metadata_url=CONF_URL,
        client_kwargs={
            'scope': 'email openid profile'
        }
    )

    # Initialize the global engine variable
    global engine
    engine = connect_db()

    return app

def connect_db():
    username = os.getenv('USER')
    password = os.getenv('PASSWORD')
    engine = create_engine(
        f'postgresql://{username}:{password}@postgres-srvr.postgres.database.azure.com/data_product_metadata')
    return engine

def delete_user_stocks(email):
    # Establish a connection
    with engine.connect() as connection:
        # Start a new transaction
        with connection.begin():
            # Delete the existing rows for the given email address
            delete_query = text(f"DELETE FROM user_stocks WHERE email = '{email}'")
            connection.execute(delete_query)

def save_user_stocks():
    # Create and populate a dataframe
    df = pd.DataFrame(user_stocks)
    # Store the user_stocks in the database
    df.to_sql('user_stocks', engine, if_exists='replace', index=False)

def load_all_user_stocks():
    #fetch user's stocks
    query = "select * from user_stocks"

    # Create a connection and execute the query
    with engine.connect() as connection:
        result = connection.execute(text(query))

    # Fetch the resultset
    user_stocks_list = result.fetchall()

    return user_stocks_list

def load_stock_price_history():
    #fetch the stock_price_history from the database
    query = "select * from stock_price_history"

    # Use pandas to execute the query and load the data into a DataFrame
    stock_price_history = pd.read_sql_query(query, engine)

    # Make sure 'reported_date' is a datetime object
    stock_price_history['reported_date'] = pd.to_datetime(stock_price_history['reported_date'])

    return stock_price_history

def load_stock_returns(stock, stock_price_history):
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
def load_plots(returns):
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

def get_username():
    user = session.get('user','')
    if 'name' in user:
        username=user['name']+" <a href='/logout'>Logout</a>"
    else:
        username=f"<a href='/login'>Login</a>"
    return username

def get_email():
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

#load the market stocks from the database
engine = connect_db()
ftse_100_stocks, dax_stocks, sp500_stocks = load_market_stocks(engine)

oauth = OAuth()

app = create_app()
app.secret_key=os.getenv('SECRET_KEY')

time_periods = {"1y": 365, "2y": 365*2, "3y": 365*3, "4y":365*4, "5y": 365*5, "6y": 365*6, "7y":365*7, "8y":365*8, "9y":365*9, "10y": 365*10}

returns={}
user_stocks = load_all_user_stocks()
stock_price_history = load_stock_price_history()

@app.route('/')
def homepage():
    username = get_username()
    return render_template('index.html', user = username)

@app.route('/refresh')
def refresh():
    global stock_price_history
    engine = connect_db()
    retmsg = refresh_stocks(engine)
    stock_price_history = load_stock_price_history()
    return retmsg

@app.route('/blog')
def blog():
    username = get_username()
    return render_template('blog.html', user = username)

@app.route('/legal')
def legal():
    username = get_username()
    return render_template('legal.html', user = username)


@app.route('/plot',methods=['GET','POST'])
def get_stocks_returns():
    returns={}
    global user_stocks
    #Either use the selected stock market or default to sp500 if there isn't one
    selected_market = request.form.get('market', 'sp500')
    #For each selected_market choose default stocks
    if selected_market == 'sp500':
        default_stock = 'NVDA'
    elif selected_market == 'ftse100':
        default_stock = 'JD.L'
    elif selected_market == 'dax':
        default_stock = 'DHL.DE'
    #Either use the selected stock or the default stock
    selected_stock = request.form.get('stock', default_stock)
    if request.method == 'POST':
        email = get_email()
        action = request.form['action']
        if action == 'add':
            # Check if the selected stock already exists for the specified email
            if (email, selected_stock) not in user_stocks:
                # If it doesn't exist, append it to the list and the database
                user_stocks.append((email, selected_stock))
        elif action == 'remove':
            #Remove the selected stock from the portfolio
            email = get_email()
            # Check if the selected stock already exists for the specified email
            if (email, selected_stock) in user_stocks:
                # If it exists then remove it from user_stocks
                user_stocks.remove((email, selected_stock))
        elif action == 'save':
            #Save the stock portfolio to the database
            save_user_stocks()
        elif action == 'delete':
            #Delete all of the stocks from the portfolio belonging to the user's email
            delete_user_stocks(get_email())
            user_stocks = load_all_user_stocks()

    #Construct the plotly plot
    email = get_email()
    for stock in user_stocks:
        if stock[0]==email:
            returns[stock[1]] = load_stock_returns(stock[1], stock_price_history)
    plot_div = load_plots(returns)
    return render_template('stocks.html', user = get_username(), ftse_100_stocks=ftse_100_stocks, dax_stocks = dax_stocks, sp500_stocks = sp500_stocks, plot_div=plot_div, selected_market = selected_market, selected_stock = selected_stock, returns=returns)

@app.route('/login')
def login():
    redirect_uri = f"{request.host_url}authorize"
    state = str(uuid.uuid4())
    session['state'] = state
    session['referrer'] = request.referrer
    return oauth.google.authorize_redirect(redirect_uri, _external=True, state=state)

@app.route('/logout')
def logout():
    session['state'] = ''
    session['user']=''
    username = get_username()
    return redirect(request.referrer)

@app.route('/authorize')
def authorize():
    # Check that the state in the session matches the state parameter in the request
    state = session.get('state','')
    if request.args.get('state', '') != session.get('state', ''):
        return f"Error: state mismatch request:{request.args.get('state','')} session:{session.get('state','')}"
    token = oauth.google.authorize_access_token()
    session['user'] = token['userinfo']
    return redirect(session.get('referrer','/'))

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5050)
