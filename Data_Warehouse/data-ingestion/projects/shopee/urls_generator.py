import os


def generate_urls(url_raw_list):
    urls = []
    for raw_url in url_raw_list:
        raw_url = raw_url.strip()  # Remove newline character
        url, max_page = raw_url.split(",")
        max_page = int(max_page)  # Convert max_page to int
        for i in range(int(max_page)):
            url_new = url.replace("page=0", f"page={i}")
            urls.append(url_new)
    return urls


def save_urls_to_file(urls, filename):
    with open(filename, "w") as f:
        for url in urls:
            f.write(url + "\n")


def main():
    url_base = "./urls/"
    for root, dirs, files in os.walk("./raws"):
        for file in files:
            print(f"Generating urls for {file}")
            with open(os.path.join(root, file), "r") as f:
                lines = [line.strip() for line in f.readlines()]
                print(f"{file} has {len(lines)} original urls")
                urls = generate_urls(lines)
            print(f"{file} has {len(urls)} urls generated")
            file_name = file.split(".")[0]
            extension = file.split(".")[1]
            saved_name = f"{url_base}{file_name}_urls.{extension}"
            print(f"Saving urls to {saved_name}")
            save_urls_to_file(urls, saved_name)
            print(f"Saved urls to {saved_name}")
            print(f"Done processing wih: {file}")
            print("=============================")


if __name__ == "__main__":
    main()
