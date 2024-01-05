from flask import Flask, request, session, render_template, redirect, make_response, jsonify
from authlib.integrations.flask_client import OAuth
import uuid
from functions import *
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import text, and_

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

    return app

#load the market stocks from the database
engine = connect_db()
market_stocks = load_market_stocks(engine)

user_stocks, stock_price_history, cumulative_returns, yoy_returns, min_max_changes = load_stock_data(engine)
stock_names = {}
for market in market_stocks:
    for stock_symbol, stock_info in market_stocks[market].items():
        stock_names[stock_symbol] = {'stock': stock_info['stock_name'], 'industry' : stock_info['industry_name']}

oauth = OAuth()
app = create_app()
app.secret_key=os.getenv('SECRET_KEY')

@app.route('/')
def homepage():
    username = get_username(session)
    return render_template('index.html', user = username)

@app.route('/chat')
def chat():
    username = get_username(session)
    return render_template('chat.html', user = username)

@app.route('/reports')
def reports():
    username = get_username(session)
    return render_template('reports.html', user = username)

@app.route('/sql')
def sql():
    username = get_username(session)
    email = get_email(session)
    query = text(f"SELECT query_name, sql FROM admin.user_queries WHERE email = '{email}'")
    with engine.connect() as connection:
        result = connection.execute(query)
    error = session.get('error', None)
    sql = session.get('sql', None)
    # Clear the session data
    session['error'] = None
    session['sql'] = None
    savedQueries = [row for row in result]

    return render_template('sql.html', user=username, error = error, sql=sql, savedQueries=savedQueries)

@app.route('/query', methods=['POST'])
def query():
    # Get the SQL query from the form
    sql = request.form['sql']
    username = get_username(session)
    try:
        engine = connect_db('stockplot')

        # Execute the SQL query
        df = pd.read_sql_query(sql, engine)

        # Convert the DataFrame to a list of dictionaries
        data = df.to_dict('records')

        # Export the result to CSV or HTML format
        export_format = request.form.get('export')
        if export_format == 'csv':
            response = make_response(df.to_csv(index=False))
            response.headers['Content-Disposition'] = 'attachment; filename=result.csv'
            response.headers['Content-Type'] = 'text/csv'
            return response
        elif export_format == 'html':
            keys_order = list(data[0].keys())
            return render_template('resultset.html', user=username, data=data, keys_order = keys_order, stock_names = stock_names)
    except ProgrammingError as e:
        error_message = str(e)
        return error_message

@app.route('/cumulative', methods=['GET'])
def cumulative():
    # Get the 'years' parameter from the request
    years = int(request.args.get('years'))
    username = get_username(session)
    try:
        # Initialize an empty list to hold the new dictionaries
        data = []

        # Iterate over the dictionary
        for market, stocks in market_stocks.items():
            for stock_symbol, returns in cumulative_returns.items():
                if stock_symbol in stocks:
                    # Fetch the x cumulative annual return
                    percentage_increase = returns[years-1]

                    # Construct the new dictionary and append it to the list
                    data.append({
                        'stock_symbol': stock_symbol,
                        'stock_name': stocks[stock_symbol]['stock_name'],
                        'industry_name': stocks[stock_symbol]['industry_name'],
                        'percentage_increase': percentage_increase
                    })

        # Filter out dictionaries where 'percentage_increase' is None
        data = [d for d in data if d['percentage_increase'] is not None]

        # Sort the data list by 'percentage_increase' in descending order
        data = sorted(data, key=lambda x: x['percentage_increase'], reverse=True)

        # Export the result to CSV or HTML format
        export_format = 'html'

        if export_format == 'csv':
            response = make_response(df.to_csv(index=False))
            response.headers['Content-Disposition'] = 'attachment; filename=result.csv'
            response.headers['Content-Type'] = 'text/csv'
            return response
        elif export_format == 'html':
            keys_order = list(data[0].keys())
            return render_template('resultset.html', report_title = f"{years} year performance (cumulative)", user=username, data=data, keys_order = keys_order, stock_names = stock_names)

    except ProgrammingError as e:
        error_message = str(e)
        return error_message

@app.route('/positiveyears', methods=['GET'])
def positiveyears():
    # Get the 'years' parameter from the request
    years = int(request.args.get('years'))
    username = get_username(session)
    try:
        # Initialize an empty list to hold the new dictionaries
        data = []

        # Iterate over the dictionary
        for market, stocks in market_stocks.items():
            for stock_symbol, returns in yoy_returns.items():
                if stock_symbol in stocks:
                    # Limit the years to check
                    returns = returns[:years]
                    positive_years = len([year for year in returns if year is not None and year > 0])

                    # Construct the new dictionary and append it to the list
                    data.append({
                        'stock_symbol': stock_symbol,
                        'stock_name': stocks[stock_symbol]['stock_name'],
                        'industry_name': stocks[stock_symbol]['industry_name'],
                        'positive_years': positive_years
                    })

        # Sort the data list by 'percentage_increase' in descending order
        data = sorted(data, key=lambda x: x['positive_years'], reverse=True)

        # Export the result to CSV or HTML format
        export_format = 'html'

        if export_format == 'csv':
            response = make_response(df.to_csv(index=False))
            response.headers['Content-Disposition'] = 'attachment; filename=result.csv'
            response.headers['Content-Type'] = 'text/csv'
            return response
        elif export_format == 'html':
            keys_order = list(data[0].keys())
            return render_template('resultset.html', report_title = f"Number of years of positive stock market gains in {years} years", user=username, data=data, keys_order = keys_order, stock_names = stock_names)

    except ProgrammingError as e:
        error_message = str(e)
        return error_message

@app.route('/max_min_changes', methods=['GET'])
def max_min_changes():
    username = get_username(session)
    try:
        df = min_max_changes
        # Export the result to CSV or HTML format
        export_format = 'html'

        if export_format == 'csv':
            response = make_response(df.to_csv(index=False))
            response.headers['Content-Disposition'] = 'attachment; filename=result.csv'
            response.headers['Content-Type'] = 'text/csv'
            return response
        elif export_format == 'html':
            keys_order = list(df.columns)
            return render_template('resultset.html', report_title = f"Max & Min daily changes per stock", user=username, data=df.to_dict('records'), keys_order = keys_order, stock_names = stock_names)

    except ProgrammingError as e:
        error_message = str(e)
        return error_message

@app.route('/save_query', methods=['POST'])
def save_query():
    data = request.get_json()
    query_name = data['name']
    sql = data['sql']
    email = get_email(session)

    with engine.connect() as connection:
        metadata = MetaData()
        queries = Table('user_queries', metadata, autoload_with=engine, schema='admin')

        # Check if the record exists
        sel = select(queries).where(and_(queries.c.email == email, queries.c.query_name == query_name))

        result = connection.execute(sel).fetchone()

        # If the record exists, update it
        if result:
            upd = queries.update().where(queries.c.email == email, queries.c.query_name == query_name).values(sql=sql)
            connection.execute(upd)
        # If the record does not exist, insert a new one
        else:
            ins = queries.insert().values(email=email, query_name=query_name, sql=sql)
            connection.execute(ins)

        connection.commit()

    return jsonify(success=True)

@app.route('/delete_query', methods=['POST'])
def delete_query():
    data = request.get_json()
    query_name = data['name']
    email = get_email(session)

    try:
        with engine.connect() as connection:
            del_stmt = text(f"DELETE FROM admin.user_queries WHERE email = '{email}' AND query_name = '{query_name}'")
            connection.execute(del_stmt)
            connection.commit()
            return jsonify(success=True)
    except Exception as e:
        return jsonify(success=False, error=str(e))

@app.route('/refresh_stock')
def stock_refresh():
    global stock_price_history, cumulative_returns, yoy_returns
    symbol = request.args.get('ticker', type = str)
    engine = connect_db()
    retmsg = refresh_stock(engine, symbol)
    user_stocks, stock_price_history, cumulative_returns, yoy_returns, min_max_changes = load_stock_data(engine)
    return retmsg

@app.route('/daily_refresh')
def daily_refresh():
    global stock_price_history, cumlative_returns, yoy_returns
    time_periods = {f"{i}y": i for i in range(1, 26)}
    engine = connect_db()
    print("loading stocks")
    market_stocks = load_market_stocks(engine)
    print("refreshing stock prices from yahoo finance")
    retmsg = daily_refresh_stocks(engine, market_stocks)
    print(retmsg)
    print("loading stock_price_history")
    stock_price_history = load_stock_price_history(engine)
    cumulative_returns, yoy_returns = precalculate_returns(market_stocks, stock_price_history, time_periods)
    print("saving cumulative returns")
    save_returns(engine, 'cumulative_returns', 'cumulative_return', cumulative_returns)
    print("saving year on year returns")
    save_returns(engine, 'yoy_returns', 'yoy_return', yoy_returns)
    print("saving min & max daily changes")
    save_min_max_changes(engine)
    return retmsg

@app.route('/refresh_returns')
def refresh_returns():
    global stock_price_history, cumulative_returns, yoy_returns
    engine = connect_db()
    user_stocks, stock_price_history, cumulative_returns, yoy_returns, min_max_changes = load_stock_data(engine)
    return "Stock price history refreshed from database and precalculated returns recalculated "

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
    user_cumulative_returns={}
    user_yoy_returns={}
    global user_stocks
    #Either use the selected stock market or default to sp500 if there isn't one
    selected_market = request.form.get('market', 'sp500')
    #For each selected_market choose default stocks
    if selected_market == 'sp500':
        default_stock = 'NVDA'
    elif selected_market == 'ftse100':
        default_stock = 'JD.L'
    elif selected_market == 'dax':
        default_stock = 'BEI.DE'
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
            user_cumulative_returns[stock[1]] = cumulative_returns[stock[1]]
            user_yoy_returns[stock[1]] = yoy_returns[stock[1]]

    # Initialize empty dictionaries for each market
    ftse100_stocks = {}
    dax_stocks = {}
    sp500_stocks = {}

    # Iterate over market_stocks
    for market, stocks in market_stocks.items():
        if market == 'FTSE 100':
            ftse100_stocks = stocks
        elif market == 'DAX':
            dax_stocks = stocks
        elif market == 'S&P 500':
            sp500_stocks = stocks

    #load the plot data
    plot_div = load_plots(engine, user_cumulative_returns, ftse100_stocks, dax_stocks, sp500_stocks)

    return render_template('stocks.html', user = get_username(session), ftse100_stocks=ftse100_stocks, dax_stocks=dax_stocks, sp500_stocks=sp500_stocks, plot_div=plot_div, selected_market = selected_market, selected_stock = selected_stock, yoy_returns = user_yoy_returns, cumulative_returns=user_cumulative_returns)

@app.route('/login')
def login():
    try:
        redirect_uri = f"{request.host_url}authorize"
        state = str(uuid.uuid4())
        session['state'] = state
        session['referrer'] = request.referrer
        return oauth.google.authorize_redirect(redirect_uri, _external=True, state=state)
    except Exception as e:
        return str(e)

@app.route('/logout')
def logout():
    session['state'] = ''
    session['user']=''
    username = get_username(session)
    return redirect(request.referrer)

@app.route('/authorize')
def authorize():
    try:
        # Check that the state in the session matches the state parameter in the request
        state = session.get('state','')
        if request.args.get('state', '') != session.get('state', ''):
            return f"Error: state mismatch request:{request.args.get('state','')} session:{session.get('state','')}"
        token = oauth.google.authorize_access_token()
        session['user'] = token['userinfo']
        return redirect(session.get('referrer','/'))
    except Exception as e:
        return str(e)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5050)
