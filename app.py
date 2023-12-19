from flask import Flask, request, session, render_template, redirect
from authlib.integrations.flask_client import OAuth
import uuid
from functions import *

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

#load the market stocks from the database
engine = connect_db()
ftse_100_stocks, dax_stocks, sp500_stocks = load_market_stocks(engine)

oauth = OAuth()

app = create_app()
app.secret_key=os.getenv('SECRET_KEY')

time_periods = {f"{i}y": 365*i for i in range(1, 26)}

returns={}
print("Loading stocks, price history and precalculated_returns")
user_stocks = load_all_user_stocks(engine)
stock_price_history = load_stock_price_history(engine)
precalculated_returns = precalculate_returns(stock_price_history, time_periods)
print("Stocks, price history and precalculated_returns all loaded")
@app.route('/')
def homepage():
    username = get_username(session)
    return render_template('index.html', user = username)

@app.route('/chat')
def chat():
    username = get_username(session)
    return render_template('chat.html', user = username)

@app.route('/full_refresh')
def full_refresh():
    global stock_price_history, precalculated_returns
    engine = connect_db()
    ftse_100_stocks, dax_stocks, sp500_stocks = load_market_stocks(engine)
    retmsg = refresh_stocks(engine, ftse_100_stocks, dax_stocks, sp500_stocks)
    stock_price_history = load_stock_price_history(engine)
    precalculated_returns = precalculate_returns(stock_price_history, time_periods)
    return retmsg

@app.route('/daily_refresh')
def daily_refresh():
    global stock_price_history, precalculated_returns
    engine = connect_db()
    ftse_100_stocks, dax_stocks, sp500_stocks = load_market_stocks(engine)
    retmsg = daily_refresh_stocks(engine, ftse_100_stocks, dax_stocks, sp500_stocks)
    stock_price_history = load_stock_price_history(engine)
    precalculated_returns = precalculate_returns(stock_price_history, time_periods)
    return retmsg

@app.route('/blog')
def blog():
    username = get_username(session)
    return render_template('blog.html', user = username)

@app.route('/legal')
def legal():
    username = get_username(session)
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
        email = get_email(session)
        action = request.form['action']
        if action == 'add':
            # Check if the selected stock already exists for the specified email
            if (email, selected_stock) not in user_stocks:
                # If it doesn't exist, append it to the list and the database
                user_stocks.append((email, selected_stock))
        elif action == 'remove':
            #Remove the selected stock from the portfolio
            email = get_email(session)
            # Check if the selected stock already exists for the specified email
            if (email, selected_stock) in user_stocks:
                # If it exists then remove it from user_stocks
                user_stocks.remove((email, selected_stock))
        elif action == 'save':
            #Save the stock portfolio to the database
            save_user_stocks(engine, user_stocks)
        elif action == 'delete':
            #Delete all of the stocks from the portfolio belonging to the user's email
            delete_user_stocks(get_email(session), engine)
            user_stocks = load_all_user_stocks(engine)

    #Construct the plotly plot
    email = get_email(session)
    for stock in user_stocks:
        if stock[0]==email:
            returns[stock[1]] = precalculated_returns[stock[1]]
            plot_div = load_plots(returns, time_periods, ftse_100_stocks, sp500_stocks, dax_stocks)
    return render_template('stocks.html', user = get_username(session), ftse_100_stocks=ftse_100_stocks, dax_stocks = dax_stocks, sp500_stocks = sp500_stocks, plot_div=plot_div, selected_market = selected_market, selected_stock = selected_stock, returns=returns)

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
    username = get_username(session)
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
