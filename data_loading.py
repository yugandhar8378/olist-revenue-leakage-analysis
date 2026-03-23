import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
connection_url = URL.create(drivername = "postgresql+psycopg2",
    username = "postgres",
    password = "Bhuvana15@30",
    host = "localhost" ,
    port= 5432,
    database = 'olist')
engine = create_engine(connection_url)
tables = {
    'orders': 'olist_orders_dataset.csv',
    'customers': 'olist_customers_dataset.csv',
    'order_items': 'olist_order_items_dataset.csv',
    'products': 'olist_products_dataset.csv',
    'payments': 'olist_order_payments_dataset.csv',
    'reviews': 'olist_order_reviews_dataset.csv',
    'sellers': 'olist_sellers_dataset.csv'
}

for table_name, file_name in tables.items():
    df = pd.read_csv(f'D:\\Python_Practice\\olist_dataset\\{file_name}')
    df.to_sql(table_name, engine, if_exists='replace', index= False)
