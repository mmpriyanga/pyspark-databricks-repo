# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# Start Spark session
spark = SparkSession.builder.appName("SalesDF").getOrCreate()

# Define schema
schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("sale_date", StringType(), True),
    StructField("amount", IntegerType(), True)
])

# Create data
data = [
    (1, "Mumbai", "2024-01-10", 5000),
    (2, "Delhi", "2024-01-15", 7000),
    (3, "Bangalore", "2024-01-20", 10000),
    (4, "Chennai", "2024-02-05", 3000),
    (5, "Mumbai", "2024-02-08", 9000)
]

# Convert to DataFrame
df = spark.createDataFrame(data, schema)

# Show result
df.show()


# COMMAND ----------

from pyspark.sql.functions import  *
df.withColumn('sale_date', to_date('sale_date'))
df.display()
df.createOrReplaceTempView('mytable')

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(amount), city from mytable group by city order by max(amount) desc limit 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(amount) from mytable

# COMMAND ----------


