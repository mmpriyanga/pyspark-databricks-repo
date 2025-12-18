# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder.appName("StringFunctions").getOrCreate()

# Sample data
data = [
    ("  John Doe  ", "john.doe@email.com", "123-456-7890", "Hello World"),
    ("Jane Smith", "jane_smith@test.org", "987-654-3210", "pyspark is great"),
    ("Bob Johnson", "bob123@company.net", "555-123-4567", "Data Engineering")
]
columns = ["name", "email", "phone", "message"]
df = spark.createDataFrame(data, columns)

# COMMAND ----------

from pyspark.sql.functions import *
df.select(
    col("name"),
    upper(col("name")).alias("upper_name"),           # Convert to uppercase
    lower(col("name")).alias("lower_name"),           # Convert to lowercase  
    initcap(col("name")).alias("initcap_name")        # Title case (first letter of each word)
).show(truncate=False)


# COMMAND ----------

df.show()

# COMMAND ----------


