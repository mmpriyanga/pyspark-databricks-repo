# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("SimpleDataFrame").getOrCreate()

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("Emp", StringType(), True),
    StructField("loc", StringType(), True)
])

# Create data
data = [
    (1, "pri", "can"),
    (2, "kum", None)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show DataFrame
df.show()


# COMMAND ----------

# Create temporary view
df.createOrReplaceTempView("emp_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select emp,coalesce(loc, lag(loc) over (order by id) )from emp_table

# COMMAND ----------


