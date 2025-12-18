# Databricks notebook source
# File location and type
file_location = "/FileStore/tables/BigMart_Sales-2.csv"
file_type = "csv"

# COMMAND ----------

df = spark.read.format('csv').option('header',True).option('infer_schema',True).load(file_location)

# COMMAND ----------


df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.filter(df.Item_Type == 'Dairy')

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col, when
df = df.withColumn('new_col', 
                   when(df.Item_Fat_Content == 'Low Fat', 'LF')
                   .otherwise(df.Item_Fat_Content))

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, BooleanType
from datetime import date, timedelta

spark = SparkSession.builder.appName("UberRides").getOrCreate()

# Define schema
schema = StructType([
    StructField("ride_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("ride_date", DateType(), True),
    StructField("cancelled", BooleanType(), True)
])

# --- Build data ---
data = []

# 50 rows for Mani (ride_id 1–50)
for i in range(50):
    data.append((i+1, 201, "Mani", "Chennai", date(2025, 4, 1) + timedelta(days=i), False))

# 2 rows for Ravi
data.append((51, 202, "Ravi", "Chennai", date(2025, 4, 1), False))
data.append((52, 202, "Ravi", "Chennai", date(2025, 4, 2), True))

# 1 row for Arun
data.append((100, 203, "Arun", "Coimbatore", date(2025, 4, 1), False))

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

df.show(10, False)     # show first 10
print("Total rows:", df.count())


# COMMAND ----------

df.createOrReplaceTempView("emp_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_name, count(customer_id) cnt from emp_table  where cancelled = 'false' and ride_date > add_months(current_date(), -1) group by customer_id,customer_name having cnt > 20
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DecimalType
from datetime import date
from decimal import Decimal   # ✅ import Decimal

spark = SparkSession.builder.appName("ZomatoOrders").getOrCreate()

# Define schema
schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("amount", DecimalType(10,2), True)   # ✅ precise decimal type
])

# Data (converted floats → Decimal)
data = [
    (1, 301, "Karthik", date(2025, 4, 4), Decimal("350.00")),
    (2, 301, "Karthik", date(2025, 4, 11), Decimal("420.00")),
    (3, 301, "Karthik", date(2025, 4, 18), Decimal("250.00")),
    (4, 301, "Karthik", date(2025, 4, 25), Decimal("500.00")),
    (20, 302, "Suresh", date(2025, 4, 4), Decimal("300.00")),
    (21, 302, "Suresh", date(2025, 4, 18), Decimal("450.00")),
    (40, 303, "Divya", date(2025, 4, 5), Decimal("600.00"))
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

df.show()
df.printSchema()


# COMMAND ----------

df.createOrReplaceTempView("emp_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte_friday as (select * from emp_table where dayofweek(order_date) = 6 and order_date > add_months(current_date(), -7))
# MAGIC select * from cte_friday

# COMMAND ----------

# MAGIC %sql
# MAGIC with last_3_months as (
# MAGIC  SELECT explode(
# MAGIC            sequence(add_months(current_date(), -3), current_date(), interval 1 day)
# MAGIC          ) AS d)
# MAGIC select count(dayofweek(d)) from last_3_months where dayofweek(d) = 6
# MAGIC select customer_name, count(order_date) cnt where dayofweek(order_date) = 6 having %sql
# MAGIC select customer_name, count(order_date) cnt from emp_data group by  dayofweek(order_date)  having cnt >= 13

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import date

# Create Spark session
spark = SparkSession.builder.appName("NetflixWatchHistory").getOrCreate()

# Define schema
schema = StructType([
    StructField("watch_id", IntegerType(), False),
    StructField("user_id", IntegerType(), True),
    StructField("user_name", StringType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("movie_name", StringType(), True),
    StructField("watch_date", DateType(), True)
])

# Sample data (converted dates to Python date objects)
data = [
    (1, 501, "Saravanan", 2001, "Movie A", date(2025, 6, 2)),
    (2, 501, "Saravanan", 2002, "Movie B", date(2025, 6, 3)),
    (3, 501, "Saravanan", 2003, "Movie C", date(2025, 6, 4)),
    (4, 501, "Saravanan", 2004, "Movie D", date(2025, 6, 5)),
    (5, 501, "Saravanan", 2005, "Movie E", date(2025, 6, 6)),
    (6, 502, "Divya", 2001, "Movie A", date(2025, 6, 2)),
    (7, 502, "Divya", 2002, "Movie B", date(2025, 6, 2))
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show sample
df.show()
df.printSchema()


# COMMAND ----------

df.createOrReplaceTempView("netf")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, BooleanType
from datetime import date

# Create Spark session
spark = SparkSession.builder.appName("SwiggyOrders").getOrCreate()

# Define schema
schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("cancelled", BooleanType(), True)
])

# Data (converted to Python date + boolean)
data = [
    (1, 601, "Meena", date(2025, 3, 2), True),
    (2, 601, "Meena", date(2025, 3, 5), True),
    (3, 601, "Meena", date(2025, 3, 10), True),
    (4, 601, "Meena", date(2025, 3, 15), True),
    (5, 602, "Ravi",  date(2025, 4, 3), True),
    (6, 602, "Ravi",  date(2025, 4, 10), False),
    (7, 602, "Ravi",  date(2025, 4, 18), True)
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show results
df.show()
df.printSchema()


# COMMAND ----------

df.createOrReplaceTempView("swig")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select cancelled, count(customer_id) cnt,customer_id,month(order_date) from swig  where order_date > add_months(curdate(), -6) group by month(order_date),customer_id, cancelled having  cnt >= 3 

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import date

# Create Spark session
spark = SparkSession.builder.appName("MetaLikes").getOrCreate()

# Define schema
schema = StructType([
    StructField("like_id", IntegerType(), False),
    StructField("user_id", IntegerType(), True),
    StructField("user_name", StringType(), True),
    StructField("post_id", IntegerType(), True),
    StructField("like_date", DateType(), True)
])

# Sample data (converted to Python date objects)
data = [
    (1, 701, "Kavin", 9001, date(2025, 3, 5)),
    (2, 701, "Kavin", 9002, date(2025, 3, 6)),
    # Suppose 48 more likes in March (simulate if needed)
    (51, 701, "Kavin", 9051, date(2025, 4, 2)),
    # Suppose 50 more likes in April, May, June (simulate if needed)
    (200, 702, "Divya", 9101, date(2025, 3, 10)),
    (201, 702, "Divya", 9102, date(2025, 3, 12))
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show data
df.show()
df.printSchema()


# COMMAND ----------

df.createOrReplaceTempView("meta")

# COMMAND ----------

# MAGIC %sql
# MAGIC select user_id, count(user_id) cnt from meta where like_date > add_months(like_date, -4) group by user_id, month(like_date) having cnt > 50 

# COMMAND ----------

import pandas as pd

# Creating the sample data for swiggy_late_orders
data = [
    (1, 901, 'Aravind', '2025-06-01', '23:15:00'),
    (2, 901, 'Aravind', '2025-06-02', '23:45:00'),
    (3, 901, 'Aravind', '2025-06-03', '23:05:00'),
    (4, 901, 'Aravind', '2025-06-04', '23:25:00'),
    (5, 901, 'Aravind', '2025-06-05', '23:35:00'),
    (6, 901, 'Aravind', '2025-06-06', '23:55:00'),
    (7, 901, 'Aravind', '2025-06-07', '23:20:00'),
    (8, 901, 'Aravind', '2025-06-08', '23:40:00'),
    (9, 901, 'Aravind', '2025-06-09', '23:10:00'),
    (10, 901, 'Aravind', '2025-06-10', '23:50:00'),
    (11, 902, 'Selvi', '2025-06-01', '22:55:00'),
    (12, 902, 'Selvi', '2025-06-02', '23:05:00'),
    (13, 902, 'Selvi', '2025-06-03', '23:25:00')
]

# Create DataFrame
df = pd.DataFrame(data, columns=["order_id", "customer_id", "customer_name", "order_date", "order_time"])




