# singleton_spark.py
from pyspark.sql import SparkSession
from utils.config import SPARK_CONFIG


class SparkSessionSingleton:
    _instance = None

    @staticmethod
    def get_instance():
        if SparkSessionSingleton._instance is None:
            SparkSessionSingleton._instance = SparkSession.builder \
                .appName("Read JSON from S3") \
                .master(f"local[{SPARK_CONFIG['total_cores']}]") \
                .config("spark.executor.instances", str(SPARK_CONFIG['workers'] * SPARK_CONFIG['executors_per_worker'])) \
                .config("spark.executor.cores", str(SPARK_CONFIG['cores_per_executor'])) \
                .config("spark.executor.memory", SPARK_CONFIG['memory_per_executor']) \
                .config("spark.driver.memory", SPARK_CONFIG['driver_memory']) \
                .config("spark.sql.shuffle.partitions", SPARK_CONFIG['shuffle_partitions']) \
                .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.kryoserializer.buffer.max", "512m") \
                .getOrCreate()
        return SparkSessionSingleton._instance
