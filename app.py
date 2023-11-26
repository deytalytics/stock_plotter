from flask import Flask, request, session, render_template, redirect
from authlib.integrations.flask_client import OAuth
import os, uuid
import plotly.graph_objs as go
import plotly.offline as pyo
import pandas as pd

from stocks import stocks, ftse_100_stocks

from sqlalchemy import create_engine, text
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
    password = os.getenv('PWD')
    engine = create_engine(f'postgresql://{username}:{password}@flora.db.elephantsql.com/{username}')
    print(engine, password)

    stock_returns = []
    for period, days in time_periods.items():
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        # Execute the SQL query and fetch the data
        query = f"SELECT * FROM stock_price_history WHERE stock_symbol = '{stock}' AND Reported_date >= '{start_date}' AND Reported_date <= '{end_date}'"
        stock_data = pd.read_sql_query(query, engine)
        if len(stock_data) > 0:  # Check if data is available
            percentage_return = round(
                (stock_data['Close'].iloc[-1] - stock_data['Open'].iloc[0]) / stock_data['Open'].iloc[0] * 100, 2)
            stock_returns.append(percentage_return)
        else:
            stock_return.append(None)
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


oauth = OAuth()

app = create_app()
app.secret_key=os.getenv('SECRET_KEY')

all_stock_data = {}
time_periods = {"1y": 365, "2y": 365*2, "5y": 365*5, "10y": 365*10}

returns = {}

for stock in stocks:
    returns[stock] = load_stock_data(stock)


@app.route('/',methods=['GET','POST'])
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

    user = session.get('user','')
    if 'name' in user:
        username=user['name']
    else:
        username = 'Not Found'
    print(username)
    plot_div = load_plots(returns)
    return render_template('stocks.html', user = username, ftse_100_stocks=ftse_100_stocks, plot_div=plot_div)

@app.route('/login')
def login():
    redirect_uri = f"{request.host_url}authorize"
    state = str(uuid.uuid4())
    session['state'] = state
    print(redirect_uri, state)
    return oauth.google.authorize_redirect(redirect_uri, _external=True, state=state)

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
