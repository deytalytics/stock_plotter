from flask import Blueprint, request, session, render_template
from app import oauth
import plotly.graph_objs as go
import plotly.offline as pyo
import yfinance as yf

routes = Blueprint('routes', __name__)

ftse_100_stocks = {
    'Anglo American plc': 'AAL.L',
    'Associated British Foods plc': 'ABF.L',
    'Admiral Group plc': 'ADM.L',
    'Ashtead Group plc': 'AHT.L',
    'Antofagasta plc': 'ANTO.L',
    'Aviva plc': 'AV..L',
    'AstraZeneca plc': 'AZN.L',
    'BAE Systems plc': 'BA.L',
    'Barclays plc': 'BARC.L',
    'British American Tobacco plc': 'BATS.L',
    'Barratt Developments plc': 'BDEV.L',
    'BHP Group plc': 'BHP.L',
    'BP plc': 'BP.L',
    'Burberry Group plc': 'BRBY.L',
    'BlackRock World Mining Trust plc': 'BRWM.L',
    'BT Group plc': 'BT-A.L',
    'Coca-Cola HBC AG': 'CCH.L',
    'Carnival plc': 'CCL.L',
    'Croda International plc': 'CRDA.L',
    'Cineworld Group plc': 'CINE.L',
    'Compass Group plc': 'CPG.L',
    'CRH plc': 'CRH.L',
    'ConvaTec Group plc': 'CTEC.L',
    'DCC plc': 'DCC.L',
    'Diageo plc': 'DGE.L',
    'Daily Mail and General Trust plc': 'DMGT.L',
    'DS Smith plc': 'SMDS.L',
    'Evraz plc': 'EVR.L',
    'Experian plc': 'EXPN.L',
    'Ferguson plc': 'FERG.L',
    'Fresnillo plc': 'FRES.L',
    'GlaxoSmithKline plc': 'GSK.L',
    'Halma plc': 'HLMA.L',
    'Hargreaves Lansdown plc': 'HL.L',
    'HSBC Holdings plc': 'HSBA.L',
    'International Consolidated Airlines Group SA': 'IAG.L',
    'InterContinental Hotels Group plc': 'IHG.L',
    '3i Group plc': 'III.L',
    'Imperial Brands plc': 'IMB.L',
    'Informa plc': 'INF.L',
    'Intertek Group plc': 'ITRK.L',
    'ITV plc': 'ITV.L',
    'Just Eat Takeaway.com NV': 'JET.L',
    'JD Sports Fashion plc': 'JD.L',
    'Just Eat plc': 'JE.L',
    'Johnson Matthey plc': 'JMAT.L',
    'Kingfisher plc': 'KGF.L',
    'Land Securities Group plc': 'LAND.L',
    'Legal & General Group plc': 'LGEN.L',
    'Lloyds Banking Group plc': 'LLOY.L',
    'London Stock Exchange Group plc': 'LSEG.L',
    'M&G plc': 'MNG.L',
    'Melrose Industries plc': 'MRO.L',
    'Morrison (Wm) Supermarkets plc': 'MRW.L',
    'National Grid plc': 'NG..L',
    'Next plc': 'NXT.L',
    'Ocado Group plc': 'OCDO.L',
    'Pennon Group plc': 'PNN.L',
    'Flutter Entertainment plc': 'PPB.L',
    'Prudential plc': 'PRU.L',
    'Persimmon plc': 'PSN.L',
    'Pearson plc': 'PSON.L',
    'Reckitt Benckiser Group plc': 'RB.L',
    'Royal Dutch Shell plc Class A': 'RDSA.L',
    'Royal Dutch Shell plc Class B': 'RDSB.L',
    'RELX plc': 'REL.L',
    'Rentokil Initial plc': 'RTO.L',
    'Sainsbury (J) plc': 'SBRY.L',
    'Schroders plc': 'SDR.L',
    'Sage Group plc': 'SGE.L',
    'Segro plc': 'SGRO.L',
    'Scottish Mortgage Investment Trust plc': 'SMT.L',
    'Smiths Group plc': 'SMIN.L',
    'Smith & Nephew plc': 'SN..L',
    'Spirax-Sarco Engineering plc': 'SPX.L',
    'SSE plc': 'SSE.L',
    'Standard Chartered plc': 'STAN.L',
    'St. James''s Place plc': 'STJ.L',
    'Severn Trent plc': 'SVT.L',
    'Tesco plc': 'TSCO.L',
    'TUI AG': 'TUI.L',
    'Taylor Wimpey plc': 'TW..L',
    'Unilever plc': 'ULVR.L',
    'United Utilities Group PLC': 'UU.L',
    'Vodafone Group plc': 'VOD.L',
    'Weir Group PLC': 'WEIR.L',
    'Whitbread plc': 'WTB.L',
    'WPP plc': 'WPP.L'
}

stocks = ['AHT.L', 'JD.L', 'MRO.L', 'HLMA.L', 'LSEG.L']

def get_stock_data(ticker, period):
    stock = yf.Ticker(ticker)
    return stock.history(period=period)

@routes.route('/',methods=['GET','POST'])
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
    return render_template('stocks.html', ftse_100_stocks=ftse_100_stocks, plot_div=plot_div)

@routes.route('/login')
def login():
    redirect_uri = "http://127.0.0.1:5050/authorize"
    print("Session:", session.get('state', ''))
    print(redirect_uri)
    return oauth.google.authorize_redirect(redirect_uri, _external=True)

@routes.route('/authorize')
def authorize():
    # Check that the state in the session matches the state parameter in the request
    if request.args.get('state', '') != session.get('state', ''):
        return f"Error: state mismatch request:{request.args.get('state','')} session:{session.get('state','')} username:{session.get('username','')}"
    else:
        print(f"request:{request.args.get('state','')} session:{session.get('state','')} username:{session.get('username','')}")
    token = oauth.google.authorize_access_token()
    session['user'] = token['userinfo']
    return redirect('/')
