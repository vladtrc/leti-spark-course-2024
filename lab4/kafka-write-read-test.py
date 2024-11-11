from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, IntegerType
from pyspark.shell import spark

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("PySparkKafkaJSONExample") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.apache.spark:spark-avro_2.12:3.1.2") \
    .getOrCreate()

# Set log level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Define schema and sample data
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

data = [("Alice", 30, "New York"),
        ("Bob", 35, "San Francisco"),
        ("Charlie", 25, "Chicago")]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Convert data to JSON format
df_json = df.select(to_json(struct([col(c) for c in df.columns])).alias("value"))

# Kafka configurations
kafka_bootstrap_servers = 'localhost:9092'
topic_name = 'test-topic'

# Write DataFrame to Kafka
df_json.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", topic_name) \
    .save()

spark.stop()
