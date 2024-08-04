from utils.s3_utils import get_most_recent_file_s3, get_list_most_recent_files_s3, download_files_from_s3
import re
import os
import shutil


def extract_file_date_time(file_name: str) -> str:
    pattern = r'^.*?-(\d{4}-\d{2}-\d{2})-\d+\.json$'
    match = re.match(pattern, file_name)
    if match:
        return match.group(1)
    else:
        raise ValueError(f"File name {file_name} does not match the expected format.")


def prepare_local_directory(local_dir: str):
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    else:
        # Clean the directory by removing all files in it
        for filename in os.listdir(local_dir):
            file_path = os.path.join(local_dir, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)


def download_lazada_data():
    local_dir = './data/lazada/1'
    prepare_local_directory(local_dir)

    prefix = 'lazada/raw/product/'
    file_prefix = 'products'

    recent_file = get_most_recent_file_s3(prefix, file_prefix)
    recent_file_name = recent_file['Key']

    file_date_time = extract_file_date_time(recent_file_name)
    # file_date_time = '2024-06-12'

    recent_files = get_list_most_recent_files_s3(prefix, file_prefix, file_date_time)
    if recent_files:
        download_files_from_s3(recent_files, local_dir)
    else:
        print('No recent files found.')


def download_shopee_data():
    local_dir = './data/shopee/1'
    prepare_local_directory(local_dir)

    prefix = 'shopee/raw/'
    file_prefix = 'products'

    recent_file = get_most_recent_file_s3(prefix, file_prefix)
    recent_file_name = recent_file['Key']

    # file_date_time = extract_file_date_time(recent_file_name)
    file_date_time = '2024-06-26'

    recent_files = get_list_most_recent_files_s3(prefix, file_prefix, file_date_time)
    if recent_files:
        download_files_from_s3(recent_files, local_dir)
    else:
        print('No recent files found.')


def download_tiki_data():
    local_dir = './data/tiki'
    prepare_local_directory(local_dir)

    prefix = 'tiki/raw/product/'
    file_prefix = 'details'

    recent_file = get_most_recent_file_s3(prefix, file_prefix)
    recent_file_name = recent_file['Key']

    file_date_time = extract_file_date_time(recent_file_name)
    # file_date_time = '2024-07-07'

    recent_files = get_list_most_recent_files_s3(prefix, file_prefix, file_date_time)
    if recent_files:
        download_files_from_s3(recent_files, local_dir)
    else:
        print('No recent files found.')


def main():
    download_lazada_data()
    download_shopee_data()
    download_tiki_data()


if __name__ == "__main__":
    main()
