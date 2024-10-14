import os
from temp_download_cleaner import cleaner
import subprocess
from time import sleep

import pyautogui as cursor

_TEMP_DOWNLOAD_PATH = r'C:\Users\truon\Desktop\FUNiX\Download_Shopee'
_URLS_PATH = './urls'
_BATCH_SIZE = 9
_CLOSE = 0
_OPEN = 1


def open_and_close_tabs(number_of_tabs, action):
    if action == _OPEN:
        for _ in range(1, number_of_tabs):
            cursor.hotkey('ctrl', 't')
            sleep(0.3)
    else:
        for _ in range(0, number_of_tabs):
            cursor.hotkey('ctrl', 'w')
            sleep(0.3)


def downloader_processor(batch_urls, batch_id, batch_count):
    # switch the proxies after two batches
    # if batch_count % 5 == 0:
    #     cursor.moveTo(1850, 180)
    #     cursor.click()
    #     sleep(0.2)
    #     cursor.hotkey('alt', 'shift', 'o')
    #     sleep(3)
    #     cursor.moveTo(1850, 180)
    #     sleep(0.3)

    # download html
    for index, url in enumerate(batch_urls):
        tab_order = index + 1
        last_tab = len(batch_urls)
        cursor.hotkey('ctrl', f'{tab_order}')
        cursor.moveTo(350, 63)  # move to address bar
        cursor.click()
        sleep(0.3)
        if batch_count > 1:
            cursor.press('backspace')  # clear the old url
            sleep(0.2)
        cursor.write(url)
        sleep(0.5)
        cursor.press('enter')
        sleep(5)  # wait to load the page

        # scroll till the bottom of the page to make sure all js are loaded
        cursor.moveTo(1850, 180)  # a random point near scroll bar
        cursor.click()
        for i in range(1, 10):
            cursor.scroll(-255)
            sleep(1.5)
            if i == 9:
                sleep(2)

        # save page content
        cursor.hotkey('ctrl', 's')
        sleep(0.3)

        # name the downloading file
        cursor.press('backspace')
        sleep(0.3)
        cursor.write(str(batch_id) + '_' + str(tab_order))
        sleep(0.3)
        cursor.press('enter')
        sleep(5)

        if tab_order == last_tab:
            sleep(25)


def downloader():
    for root, dirs, files in os.walk(_URLS_PATH):
        print("Chose file to download (Enter 0 to exit): ")
        print("**Note: run cleaner for the latest data")
        for index, file in enumerate(files):
            print(f"{index + 1}. {file}")
        print("99. Run the file cleaner")
    choice = input("Please enter your choice: ")
    if int(choice.strip()) == 0:
        return
    elif int(choice.strip()) == 99:
        cleaner()
        return

    print(f"Start downloader with options -> {int(choice.strip())}")
    file_opened_name = ''
    for root, dirs, files in os.walk(_URLS_PATH):
        file_to_open = os.path.join(root, files[int(choice.strip()) - 1])
        with open(file_to_open, 'r') as f:
            file_opened_name = f.name
            urls_to_crawl = [url.strip() for url in f.readlines()]

    batch_count = 1  # fix this before running
    file_name = file_opened_name.split("\\")[1]
    name = str(file_name).split(".")[0]
    batch_id = f"{name}_{batch_count}"

    edge_execute_path = r'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe'
    profile_path = r'C:\Users\truon\AppData\Local\Microsoft\Edge\User Data'
    subprocess.Popen(
        [edge_execute_path,
         "--user-data-dir=" + os.path.normpath(profile_path),
         "--profile-directory=Profile 1"]
    )
    sleep(5)  # Loading Edge

    # open all batch tabs, except the first initial tab
    open_and_close_tabs(_BATCH_SIZE, _OPEN)

    for i in range(0, len(urls_to_crawl), _BATCH_SIZE):
        print(f"Start the batch: {batch_id}")
        batch_urls = urls_to_crawl[i: i + _BATCH_SIZE]
        downloader_processor(batch_urls, batch_id, batch_count)
        batch_count += 1
        batch_id = f"{name}_{batch_count}"
        print(f"Done with this batch: {batch_id}")

    print(f"Done the downloading process with file: {file_name}")

    # close edge
    open_and_close_tabs(_BATCH_SIZE, _CLOSE)


if __name__ == '__main__':
    downloader()
