from pyspark.sql import SparkSession
import pyspark

print("PySpark version:", pyspark.__version__)

spark = SparkSession.builder.appName("VersionCheck").getOrCreate()
print("Spark session started successfully!")
spark.stop()