from flask import Flask, request, session, render_template, redirect, make_response, jsonify, url_for
from authlib.integrations.flask_client import OAuth
import uuid, re
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
ftse_100_stocks, dax_stocks, sp500_stocks = load_market_stocks(engine)
user_stocks, stock_price_history, precalculated_returns = load_stock_data(engine)

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

        # Export the result to CSV or HTML format
        export_format = request.form.get('export')
        if export_format == 'csv':
            response = make_response(df.to_csv(index=False))
            response.headers['Content-Disposition'] = 'attachment; filename=result.csv'
            response.headers['Content-Type'] = 'text/csv'
            return response
        elif export_format == 'html':
            return render_template('resultset.html', user=username, table=df.to_html(classes='table table-bordered custom-table-striped', header="true", index=False))
    except ProgrammingError as e:
        error_message = str(e)
        error_message = error_message.split("(Background on this error at:")[0]

        # Remove the technical details from the error message
        error_message = re.sub(r'\(psycopg2\.errors\.\w+\)', '', error_message)

        session['error'] = error_message
        session['sql'] = sql
        return redirect(url_for('sql'))


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

@app.route('/full_refresh')
def full_refresh():
    global stock_price_history, precalculated_returns
    engine = connect_db()
    ftse_100_stocks, dax_stocks, sp500_stocks = load_market_stocks(engine)
    retmsg = refresh_stocks(engine, ftse_100_stocks, dax_stocks, sp500_stocks)
    if retmsg=="Stocks refreshed":
        user_stocks, stock_price_history, precalculated_returns = load_stock_data(engine)
    return retmsg

@app.route('/refresh_stock')
def stock_refresh():
    global stock_price_history, precalculated_returns
    symbol = request.args.get('ticker', type = str)
    engine = connect_db()
    retmsg = refresh_stock(engine, symbol)
    user_stocks, stock_price_history, precalculated_returns = load_stock_data(engine)
    return retmsg


@app.route('/daily_refresh')
def daily_refresh():
    global stock_price_history, precalculated_returns
    engine = connect_db()
    ftse_100_stocks, dax_stocks, sp500_stocks = load_market_stocks(engine)
    retmsg = daily_refresh_stocks(engine, ftse_100_stocks, dax_stocks, sp500_stocks)
    user_stocks, stock_price_history, precalculated_returns = load_stock_data(engine)
    return retmsg

@app.route('/refresh_returns')
def refresh_returns():
    global stock_price_history, precalculated_returns
    engine = connect_db()
    user_stocks, stock_price_history, precalculated_returns = load_stock_data(engine)
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
            returns[stock[1]] = precalculated_returns[stock[1]]
    plot_div = load_plots(returns, ftse_100_stocks, sp500_stocks, dax_stocks)
    return render_template('stocks.html', user = get_username(session), ftse_100_stocks=ftse_100_stocks, dax_stocks = dax_stocks, sp500_stocks = sp500_stocks, plot_div=plot_div, selected_market = selected_market, selected_stock = selected_stock, returns=returns)

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
        del request.session['state']
        return redirect(session.get('referrer','/'))
    except Exception as e:
        return str(e)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5050)
