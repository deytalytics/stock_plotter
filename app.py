from flask import Flask, request, session, render_template, redirect
from authlib.integrations.flask_client import OAuth
import os, uuid
import plotly.graph_objs as go
import plotly.offline as pyo
import yfinance as yf

from stocks import stocks, ftse_100_stocks

oauth = OAuth()

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
            'scope': 'openid email profile'
        }
    )

    return app

app = create_app()
app.secret_key=os.getenv('SECRET_KEY')

def get_stock_data(ticker, period):
    stock = yf.Ticker(ticker)
    return stock.history(period=period)

@app.route('/',methods=['GET','POST'])
def get_stocks_returns():
    if request.method == 'POST':
        action = request.form['action']
        selected_stock = request.form['stock']
        if action == 'add':
            if selected_stock in ftse_100_stocks.values() and selected_stock not in stocks:
                stocks.append(selected_stock)
            elif selected_stock not in ftse_100_stocks.values():
                return "<script>alert('The selected stock is not in the FTSE 100 list. Please select a valid stock.')</script>"
            else:
                return "<script>alert('The selected stock is already in the list. Please select a different stock.')</script>"
        elif action == 'remove':
            if selected_stock in stocks:
                stocks.remove(selected_stock)
            else:
                return "<script>alert('The selected stock is not in the current list. Please select a different stock to remove.')</script>"
    time_periods = ["1y", "2y", "5y", "10y"]
    returns = {}
    for stock in stocks:
        stock_returns = []
        for period in time_periods:
            stock_data = get_stock_data(stock, period)
            if len(stock_data) > 0:  # Check if data is available
                percentage_return = round((stock_data['Close'].iloc[-1] - stock_data['Open'].iloc[0]) / stock_data['Open'].iloc[0] * 100, 2)
                stock_returns.append(percentage_return)
            else:
                stock_returns.append(None)
        returns[stock] = stock_returns
    plots = []
    for stock, ret in returns.items():
      hover_text = [f"{next((key for key, value in ftse_100_stocks.items() if value == stock), 'Unknown')}, {time_periods[i]}, {ret[i]}%" for i in range(len(time_periods))]
      trace = go.Scatter(x=time_periods, y=ret, mode='lines+markers', name=stock, hoverinfo='text', text=hover_text)
      plots.append(trace)

    layout = go.Layout(
        title='Stock Returns Over Time',
        xaxis=dict(title='Time Period'),
        yaxis=dict(title='%age Return')
    )
    fig = go.Figure(data=plots, layout=layout)
    plot_div = pyo.plot(fig, output_type='div')
    user = session.get('user','')
    return render_template('stocks.html', user = user['name'], ftse_100_stocks=ftse_100_stocks, plot_div=plot_div)

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
    print(request.host_url, state)
    if request.args.get('state', '') != session.get('state', ''):
        return f"Error: state mismatch request:{request.args.get('state','')} session:{session.get('state','')} username:{session.get('username','')}"
    else:
        print(f"request:{request.args.get('state','')} session:{session.get('state','')} username:{session.get('username','')}")
    token = oauth.google.authorize_access_token()
    session['user'] = token['userinfo']
    return redirect('/')

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8080)
