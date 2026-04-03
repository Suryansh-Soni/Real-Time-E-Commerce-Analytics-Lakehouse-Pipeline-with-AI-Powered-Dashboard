from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType



spark = SparkSession.builder \
    .appName("KafkaStreaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
            "io.delta:delta-spark_2.13:4.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Schema
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

# Read Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce_orders") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON
df_clean = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Write to Delta
query = df_clean.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "C:/delta/checkpoint") \
    .start("C:/delta/orders")

query.awaitTermination()