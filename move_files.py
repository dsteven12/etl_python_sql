import shutil
import os
import glob

source_dir = '/mnt/c/Users/d.lardizabal/Downloads/etl_python_sql/data/new_orders'
destination_dir = '/mnt/c/Users/d.lardizabal/Downloads/etl_python_sql/data/orders'

def move_files():
    for file_path in glob.glob(os.path.join(source_dir, '*.parquet')):
        shutil.move(file_path, destination_dir)
    print("Files moved successfully.")

if __name__ == "__main__":
    move_files()
