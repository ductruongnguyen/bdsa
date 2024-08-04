import requests
from concurrent.futures import ThreadPoolExecutor


def check_url_status(url):
    try:
        response = requests.head(url, allow_redirects=True, timeout=10)
        return url, response.status_code
    except requests.RequestException as e:
        return url, str(e)


def main():
    input_file = '../projects/lazada/data/urls_to_crawl.txt'
    output_file = 'urls_with_status.csv'

    with open(input_file, 'r') as f:
        urls = [line.strip() for line in f]

    with ThreadPoolExecutor(max_workers=30) as executor:
        results = list(executor.map(check_url_status, urls))

    with open(output_file, 'w') as f:
        f.write("URL,Status\n")
        for url, status in results:
            f.write(f"{url},{status}\n")


if __name__ == "__main__":
    main()
