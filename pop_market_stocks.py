import os
from sqlalchemy import create_engine, Table, MetaData, Column, String
from sqlalchemy.orm import sessionmaker
from stocks import ftse_100_stocks, dax_stocks, sp500_stocks

def connect_db():
    username = os.getenv('USER')
    password = os.getenv('PASSWORD')
    engine = create_engine(
        f'postgresql://{username}:{password}@postgres-srvr.postgres.database.azure.com/data_product_metadata')
    return engine

def create_table(engine):
    metadata = MetaData()

    market_stocks = Table('market_stocks', metadata,
        Column('market', String),
        Column('stock_name', String),
        Column('ticker', String)
    )

    metadata.create_all(engine, checkfirst=True)


def insert_data(engine, market, data):
    metadata = MetaData()
    market_stocks = Table('market_stocks', metadata, autoload_with=engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    for stock_name, ticker in data.items():
        print(stock_name, ticker)
        ins = market_stocks.insert().values(market=market, stock_name=stock_name, ticker=ticker)
        session.execute(ins)
    session.commit()

engine = connect_db()

create_table(engine)

# Insert data into the database
insert_data(engine, 'FTSE 100', ftse_100_stocks)
insert_data(engine, 'DAX', dax_stocks)
insert_data(engine, 'S&P 500', sp500_stocks)
