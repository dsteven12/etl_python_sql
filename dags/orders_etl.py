import pandas as pd
import sqlalchemy as db
import os
from dotenv import load_dotenv

load_dotenv()

DB_STRING=os.getenv('DB_STRING')
USERNAME=os.getenv('USERNAME')
PASSWORD=os.getenv('PASSWORD')
HOSTNAME=os.getenv('HOSTNAME')
PORT=os.getenv('PORT')
DB_NAME=os.getenv('DB_NAME')

orders_table = './data/orders/orders.parquet'
engine = db.create_engine(f'{DB_STRING}://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DB_NAME}')

def main():
    orders = pd.