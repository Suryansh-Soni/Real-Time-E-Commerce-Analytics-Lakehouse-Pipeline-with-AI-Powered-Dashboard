from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, window, to_timestamp
import os

# ==============================
# 🚀 Spark Session with Delta
# ==============================
spark = SparkSession.builder \
    .appName("GoldLayerAnalytics") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ==============================
# 📁 Create CSV Folder
# ==============================
base_csv_path = "C:/delta/gold_csv/"
os.makedirs(base_csv_path, exist_ok=True)

# ==============================
# 📥 Read Delta (Silver Layer)
# ==============================
df = spark.read.format("delta").load("C:/delta/orders")

# ==============================
# 🛠 Fix Timestamp (CORRECT)
# ==============================
df = df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss.SSSSSS")
)

df = df.dropna(subset=["timestamp"])

# ==============================
# 💰 Add Revenue Column
# ==============================
df = df.withColumn("revenue", col("price") * col("quantity"))

# ==============================
# 🔧 Helper Function for CSV
# ==============================
def write_csv(dataframe, path):
    dataframe.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(path)

# ==============================
# 🔥 1. Total Revenue
# ==============================
total_revenue = df.select(
    sum("revenue").alias("total_revenue")
)

total_revenue.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("C:/delta/gold/total_revenue")

write_csv(total_revenue, base_csv_path + "total_revenue")

# ==============================
# 🔥 2. Revenue by Product
# ==============================
product_sales = df.groupBy("product").agg(
    sum("revenue").alias("total_sales"),
    count("*").alias("total_orders")
)

product_sales.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("C:/delta/gold/product_sales")

write_csv(product_sales, base_csv_path + "product_sales")

# ==============================
# 🔥 3. Top Products
# ==============================
top_products = product_sales.orderBy(col("total_sales").desc())

top_products.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("C:/delta/gold/top_products")

write_csv(top_products, base_csv_path + "top_products")

# ==============================
# 🔥 4. Orders Per Minute (FIXED)
# ==============================
orders_per_minute = df.groupBy(
    window(col("timestamp"), "1 minute")
).agg(
    count("*").alias("order_count")
).select(
    col("window.start").alias("start_time"),
    col("window.end").alias("end_time"),
    col("order_count")
)

orders_per_minute.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("C:/delta/gold/orders_per_minute")

write_csv(orders_per_minute, base_csv_path + "orders_per_minute")

# ==============================
# 🔥 5. Revenue Trend (FIXED)
# ==============================
revenue_trend = df.groupBy(
    window(col("timestamp"), "1 minute")
).agg(
    sum("revenue").alias("revenue")
).select(
    col("window.start").alias("start_time"),
    col("window.end").alias("end_time"),
    col("revenue")
)

revenue_trend.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("C:/delta/gold/revenue_trend")

write_csv(revenue_trend, base_csv_path + "revenue_trend")

# ==============================
# 🔥 6. Avg Order Value
# ==============================
avg_order_value = df.select(
    avg("revenue").alias("avg_order_value")
)

avg_order_value.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("C:/delta/gold/avg_order_value")

write_csv(avg_order_value, base_csv_path + "avg_order_value")

# ==============================
# 🔥 7. KPI TABLE
# ==============================
kpi = df.agg(
    sum("revenue").alias("total_revenue"),
    count("*").alias("total_orders"),
    avg("revenue").alias("avg_order_value")
)

kpi.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("C:/delta/gold/kpi")

write_csv(kpi, base_csv_path + "kpi")

# ==============================
print("✅ Gold Layer Created Successfully!")