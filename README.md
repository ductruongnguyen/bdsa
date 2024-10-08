# The Business Decision Supporting Application for E-Commerce Sellers
This application is a complete data pipeline that crawls data from three well-known e-commerce platforms in Vietnam and transforms them into an interactive dashboard to support sellers in making decisions.
## Data Ingestion Installation Guide
### Install required libraries
#### Install AWS CLIv2
See the original AWS CLIv2 installation guide at:  [AWS CLIv2 Guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

Open `Terminal` to config credentials and region for AWS S3
```
aws configure
```
#### Install required Python libs via pip
```
cd .\Data_Warehouse\data-ingestion\
python -m venv venv
venv\Scripts\activate

pip install -r requirements.txt
```

### Run the `Scrapy` crawlers by command line
```
cd .\Data_Warehouse\data-ingestion\projects\tiki
scrapy crawl categories_crawler
scrapy crawl prd_list_crawler
scrapy crawl prd_detail_crawler

cd .\Data_Warehouse\data-ingestion\projects\lazada
scrapy crawl urls_crawler
scrapy crawl products_crawler
```

## Data Ingestion Installation Guide
### Install required Python libs via pip
```
cd .\Data_Warehouse\data-transformation\
python -m venv venv
venv\Scripts\activate

pip install -r requirements.txt
```

### Run script one by one follow the order below
The Loop section means that these scripts could be run multiple times.
```
# 1_data_downloader.py
# 2_general_initial_load.py
# 3_tiki_initial_load.py
# 4_lazada_initial_load.py
# 5_shopee_initial_load.py

-- Loop
# 6_tiki_incremental_load.py
# 7_lazada_incremental_load.py
# 8_shopee_incremental_load.py
```

To run the Python scripts, use this command
```
python data_downloader.py # replace by the script willing to run
```

## Web Server Installation Guide
Build the Spring Boot project using Gradle
```
cd .\Web_Server\web_server
.\gradlew build
```
Run the Application on 8080 port by default
```
.\gradlew bootRun
```