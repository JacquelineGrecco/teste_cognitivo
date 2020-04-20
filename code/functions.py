from pyspark.sql import SparkSession
from pyspark.sql.types import *
import json

# Function to configurate spark according the spark documentation
def SparkConfig() -> object:
    spark = SparkSession \
        .builder \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer", "512k") \
        .config("spark.kryoserializer.buffer.max", "512m") \
        .config("spark.scripts_sql.parquet.filterPushdown", "true") \
        .config("spark.scripts_sql.parquet.mergeSchema", "false") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.speculation", "false") \
        .config("spark.network.timeout", "10000000") \
        .config("spark.executor.heartbeatInterval", "10000000") \
        .config("spark.executor.memory", "6g") \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.pyspark.memory", "6g") \
        .master("local[10]") \
        .getOrCreate()
    return spark

# Function to map and create a StructType based on a json file. 
def CustomSchema(filename):
    try:
        with open(filename,'r') as f:
            data = json.loads(f.read())
            mapping = {"string": StringType, "integer": IntegerType, "timestamp": TimestampType}
            schema = StructType([StructField(key, mapping.get(data[key])(), True) for key in data])  
        return schema 
    except ValueError as e:
        print('invalid json: %s' % e)
    return None