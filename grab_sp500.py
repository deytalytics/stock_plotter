import pandas as pd
from functions import connect_db
from sqlalchemy import text

# Load spreadsheet
xl = pd.ExcelFile('sp500_stocks.xlsx')

# Load a sheet into a DataFrame by its name
df = xl.parse('Sheet1')

# Create an array of dictionaries
sp500_stocks = df.to_dict('records')

# Convert array of dictionaries to have symbol as key and company as value
sp500_stocks = {d['Symbol']: d['Company'] for d in sp500_stocks}

# Connect to your SQLite database
engine = connect_db()
with engine.connect() as conn:

    # Delete existing entries for 'S&P 500'
    conn.execute(text("DELETE FROM stockplot.market_stocks WHERE market = 'S&P 500'"))

    # Insert new entries into the 'market_stocks' table
    for symbol, company in sp500_stocks.items():
        conn.execute(
            text("INSERT INTO stockplot.market_stocks (market, stock_name, stock_symbol) VALUES (:market, :company, :symbol)"),
            {'market': 'S&P 500', 'company': company, 'symbol': symbol}
        )

    # Commit the changes and close the connection
    conn.commit()
    conn.close()