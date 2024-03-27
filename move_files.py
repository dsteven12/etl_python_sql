import shutil
import os
import glob

source_directory = './data/new_orders/'
destination_directory = './data/orders/'

def move_files():
    for file_path in glob.glob(os.path.join(source_directory, '*.parquet')):
        shutil.move(file_path, destination_directory)
    print("Files moved successfully.")

if __name__ == "__main__":
    move_files()
