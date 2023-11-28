from flask import Flask, request, session, render_template, redirect
from authlib.integrations.flask_client import OAuth
import os, uuid
import plotly.graph_objs as go
import plotly.offline as pyo
import pandas as pd

from stocks import stocks, ftse_100_stocks

from sqlalchemy import create_engine
from datetime import datetime, timedelta

def create_app():

    app = Flask(__name__)
    app.config.from_object('config')
    CONF_URL = 'https://accounts.google.com/.well-known/openid-configuration'

    # Initialize oauth with the app
    oauth.init_app(app)

    google = oauth.register(
        name='google',
        server_metadata_url=CONF_URL,
        client_kwargs={
            'scope': 'email openid profile'
        }
    )

    return app

#Load the 1 year, 2 year, 5 year and 10 year percentage increases for a specific stock
def load_stock_data(stock):
    # Create a connection to your PostgreSQL database
    username = os.getenv('USER')
    password = os.getenv('PASSWORD')
    engine = create_engine(f'postgresql://{username}:{password}@flora.db.elephantsql.com/{username}')

    stock_returns = []
    for period, days in time_periods.items():
        # Execute the SQL query and fetch the data
        query = f"""
        SELECT * FROM stock_price_history WHERE stock_symbol = '{stock}' 
        AND reported_date in (  select max(reported_date) FROM stock_price_history WHERE stock_symbol = '{stock}' 
        union
        select max(reported_date) - INTERVAL '{days} days' FROM stock_price_history WHERE stock_symbol = '{stock}')"""
        stock_data = pd.read_sql_query(query, engine)
        if len(stock_data) > 0:  # Check if data is available
            percentage_return = round(
                (stock_data['Close'].iloc[-1] - stock_data['Close'].iloc[0]) / stock_data['Close'].iloc[0] * 100, 2)
            stock_returns.append(percentage_return)
        else:
            stock_returns.append(None)
    engine.dispose()
    return stock_returns



#Create the plot for all of the stocks in the stock portfolio
def load_plots(returns):
    plots = []
    for stock, ret in returns.items():
        hover_text = [
            f"{next((key for key, value in ftse_100_stocks.items() if value == stock), 'Unknown')}, {period}, {r}%" for
            period, r in zip(time_periods, ret)]
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

oauth = OAuth()

app = create_app()
app.secret_key=os.getenv('SECRET_KEY')

all_stock_data = {}
time_periods = {"1y": 365, "2y": 365*2, "3y": 365*3, "4y":365*4, "5y": 365*5, "6y": 365*6, "7y":365*7, "8y":365*8, "9y":365*9, "10y": 365*10}

returns = {}

for stock in stocks:
    returns[stock] = load_stock_data(stock)

@app.route('/')
def homepage():
    username = get_username()
    return render_template('index.html', user = username)

@app.route('/blog')
def blog():
    username = get_username()
    return render_template('blog.html', user = username)

@app.route('/plot',methods=['GET','POST'])
def get_stocks_returns():
    if request.method == 'POST':
        action = request.form['action']
        selected_stock = request.form['stock']
        if action == 'add':
            # Add the selected stock to the portfolio
            if selected_stock in ftse_100_stocks.values() and selected_stock not in stocks:
                stocks.append(selected_stock)
                returns[selected_stock] = load_stock_data(selected_stock)
            elif selected_stock not in ftse_100_stocks.values():
                return "<script>alert('The selected stock is not in the FTSE 100 list. Please select a valid stock.')</script>"
            else:
                return "<script>alert('The selected stock is already in the list. Please select a different stock.')</script>"
        elif action == 'remove':
            #Remove the selected stock from the portfolio
            if selected_stock in stocks:
                stocks.remove(selected_stock)
                del returns[selected_stock]
            else:
                return "<script>alert('The selected stock is not in the current list. Please select a different stock to remove.')</script>"

    username = get_username()
    plot_div = load_plots(returns)
    return render_template('stocks.html', user = username, ftse_100_stocks=ftse_100_stocks, plot_div=plot_div)

@app.route('/login')
def login():
    redirect_uri = f"{request.host_url}authorize"
    state = str(uuid.uuid4())
    session['state'] = state
    return oauth.google.authorize_redirect(redirect_uri, _external=True, state=state)

@app.route('/logout')
def logout():
    session['state'] = ''
    session['user']=''
    username = get_username()
    return render_template('index.html', user=username)

@app.route('/authorize')
def authorize():
    # Check that the state in the session matches the state parameter in the request
    state = session.get('state','')
    if request.args.get('state', '') != session.get('state', ''):
        return f"Error: state mismatch request:{request.args.get('state','')} session:{session.get('state','')}"
    token = oauth.google.authorize_access_token()
    session['user'] = token['userinfo']
    return redirect('/')

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5050)
