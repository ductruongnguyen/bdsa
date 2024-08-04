DB_CREDENTIALS = {
    'username': 'postgres',
    'password': '1',
    'host': 'localhost',
    'port': '5432',
    'dbname': 'postgres',
    'schema': 'ecommerce',
    'connectTimeout': '10',
    'socketTimeout': '300000',
    'driver': 'org.postgresql.Driver'
}

SPARK_CONFIG = {
    'total_cores': 12,
    'workers': 1,
    'executors_per_worker': 3,
    'cores_per_executor': 4,
    'memory_per_executor': '8g',
    'driver_memory': '16g',
    'shuffle_partitions': '200'
}

PLATFORM = {
    'shopee': 10,
    'lazada': 11,
    'tiki': 12
}

READ_MODE = {
    'single': 'single',
    'multi': 'multi'
}
