import os
import shutil

_TEMP_DOWNLOAD_PATH = r'C:\Users\truon\Desktop\FUNiX\Download_Shopee'


def cleaner():
    for root, dirs, files in os.walk(_TEMP_DOWNLOAD_PATH):
        for file in files:
            os.remove(os.path.join(root, file))
        for dir in dirs:
            shutil.rmtree(os.path.join(root, dir))

    print(f"{_TEMP_DOWNLOAD_PATH} has been cleaned. Ready to download")
