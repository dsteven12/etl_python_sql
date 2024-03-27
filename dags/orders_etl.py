import pandas as pd
import sqlalchemy as db
import os
import glob
from dotenv import load_dotenv

load_dotenv()

DB_STRING=os.getenv('DB_STRING')
USERNAME=os.getenv('USERNAME')
PASSWORD=os.getenv('PASSWORD')
HOSTNAME=os.getenv('HOSTNAME')
PORT=os.getenv('PORT')
DB_NAME=os.getenv('DB_NAME')


orders_directory = './data/orders/'
engine = db.create_engine(f'{DB_STRING}://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DB_NAME}')

def process_file(file_path):
    orders = pd.read_excel(file_path)
    orders.to_sql(
        'orders',
        engine,
        if_exists='append',  # Changed to 'append' to add data to the table rather than replacing it
        index=False
    )

def main():
    for file_path in glob.glob(os.path.join(orders_directory, '*.parquet')):
        process_file(file_path)
    print("ETL Script Executed Successfully.")

if __name__ == "__main__":
    main()