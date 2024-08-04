import re

from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, LongType, StringType


def extract_lazada_discount(discount_str):
    if discount_str:
        return float(discount_str.replace('% Off', '').strip()) / 100
    return None


def extract_sold_number(s):
    if not s:
        return 0

    # Regex to find the number part
    match = re.search(r'(\d+[.,]?\d*)[kK]?', s)
    if not match:
        return 0
    num_str = match.group(1)

    if 'k' in s.lower():
        # Handle case with 'k' or 'K'
        if '.' in num_str or ',' in num_str:
            num_str = num_str.replace('.', '').replace(',', '')
            num = int(num_str) * 100
        else:
            num = int(num_str) * 1000
    else:
        # No 'k' or 'K'
        num = int(num_str)

    return num


def extract_shopee_location(location_str: str):
    if location_str:
        return location_str.replace("|", "").replace("TP.", "").replace(" - ", " ").strip()
    return None


extract_shopee_location_udf = udf(extract_shopee_location, StringType())
extract_sold_number_udf = udf(extract_sold_number, LongType())
extract_lazada_discount_udf = udf(extract_lazada_discount, FloatType())
