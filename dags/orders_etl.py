import pandas as pd
import sqlalchemy as db
import os
import glob
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.DEBUG)

load_dotenv()

DB_STRING=os.getenv('DB_STRING')
USERNAME=os.getenv('USERNAME')
PASSWORD=os.getenv('PASSWORD')
HOSTNAME=os.getenv('HOSTNAME')
PORT=os.getenv('PORT')
DB_NAME=os.getenv('DB_NAME')

orders_directory = '/mnt/c/Users/d.lardizabal/Downloads/etl_python_sql/data/orders/'
engine = db.create_engine(f'{DB_STRING}://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DB_NAME}')

def process_file(file_path, if_exists_mode):
    try:
        orders = pd.read_parquet(file_path)
        orders.to_sql(
            'orders',
            engine,
            if_exists=if_exists_mode,
            index=False
        )
    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    files = glob.glob(os.path.join(orders_directory, '*.parquet'))  # Adjusted for Excel files; change to '*.parquet' if needed
    first_file = True
    
    for file_path in files:
        if first_file:
            process_file(file_path, 'replace')
            first_file = False  # After processing the first file, set this to False
        else:
            process_file(file_path, 'append')
    print("ETL Script Executed Successfully.")

if __name__ == "__main__":
    main()