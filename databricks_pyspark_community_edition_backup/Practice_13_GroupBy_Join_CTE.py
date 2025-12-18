# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Start Spark session
spark = SparkSession.builder.appName("UserPlays").getOrCreate()

# Sample data
data = [
    ("U001", "Artist A", "S001", 180),
    ("U001", "Artist B", "S002", 200),
    ("U001", "Artist A", "S003", 150),
    ("U001", "Artist C", "S004", 300),
    ("U001", "Artist B", "S005", 50),
    ("U002", "Artist D", "S006", 240),
    ("U002", "Artist A", "S007", 100)
]

# Schema
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("artist_name", StringType(), True),
    StructField("song_id", StringType(), True),
    StructField("played_duration", IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show DataFrame
df.show()


# COMMAND ----------

df.createOrReplaceTempView('spotify')

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
df_agg = df.groupBy('user_id','artist_name').agg((sum('played_duration')).alias('Total'))


# COMMAND ----------

df_agg.withColumn('Rank', rank().over(Window.partitionBy('user_id').orderBy(col('Total').desc()))).display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Start Spark session
spark = SparkSession.builder.appName("UserPlays").getOrCreate()

# Sample data
data = [
    ("U001", "Artist A", "S001", 180),
    ("U001", "Artist B", "S002", 200),
    ("U001", "Artist A", "S003", 150),
    ("U001", "Artist C", "S004", 300),
    ("U001", "Artist B", "S005", 50),
    ("U002", "Artist D", "S006", 240),
    ("U002", "Artist A", "S007", 100)
]

# Schema
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("artist_name", StringType(), True),
    StructField("song_id", StringType(), True),
    StructField("played_duration", IntegerType(), True)
])

# Create DataFrame
plays_df = spark.createDataFrame(data, schema=schema)

# Show DataFrame
plays_df.show()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create Spark session
spark = SparkSession.builder.appName("CustomerTable").getOrCreate()

# Sample data
customer_data = [
    ("C001", "Priya", "Chennai", 28),
    ("C002", "Rahul", "Mumbai", 35),
    ("C003", "Ayesha", "Bangalore", 24),
    ("C004", "John", "Hyderabad", 30)
]

# Schema
customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Create DataFrame
customer_df = spark.createDataFrame(customer_data, schema=customer_schema)

# Show DataFrame
customer_df.show()


# COMMAND ----------




# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import datetime

# Create Spark session
spark = SparkSession.builder.getOrCreate()

# Customer data
customers_data = [
    (1, 'John'), (2, 'Jane'), (3, 'Bob'), (4, 'Alice'), (5, 'Mike')
]

# Orders data
orders_data = [
    (1, 1, '2020-01-01'), (2, 2, '2020-01-02'), (3, 3, '2020-01-03'),
    (4, 4, '2020-01-04'), (5, 5, '2020-01-05'), (6, 1, '2020-02-01'),
    (7, 2, '2020-02-02'), (8, 3, '2020-02-03'), (9, 4, '2020-02-04'),
    (10, 5, '2020-02-05'), (11, 1, '2020-03-01'), (12, 2, '2020-03-02'),
    (13, 3, '2020-03-03'), (14, 4, '2020-03-04'), (15, 5, '2020-03-05'),
    (361, 1, '2020-12-01'), (362, 2, '2020-12-02'), (363, 3, '2020-12-03'),
    (364, 4, '2020-12-04'), (365, 5, '2020-12-05')
]

# Define schemas
customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True)
])

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", DateType(), True)
])

# Create DataFrames
customers_df = spark.createDataFrame(customers_data, customers_schema)
orders_df = spark.createDataFrame(
    [(oid, cid, datetime.strptime(date, "%Y-%m-%d")) for oid, cid, date in orders_data],
    orders_schema
)

# Show DataFrames
customers_df.show()
orders_df.show()


# COMMAND ----------

orders_df.createOrReplaceTempView("plays")
customers_df.createOrReplaceTempView("customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from plays;
# MAGIC select * from customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer.name, plays.customer_id, count(plays.order_id), month(plays.order_date) from customer join plays on customer.customer_id = plays.customer_id group by plays.customer_id, customer.name, month(plays.order_date) 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     c.name AS customer_name,
# MAGIC     p.customer_id,
# MAGIC     COUNT(p.order_id) AS total_orders,
# MAGIC     MONTH(p.order_date) AS month
# MAGIC   FROM customer c
# MAGIC   JOIN plays p ON c.customer_id = p.customer_id
# MAGIC
# MAGIC   GROUP BY c.name, p.customer_id, MONTH(p.order_date)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# Initialize Spark Session
spark = SparkSession.builder.appName("TransactionTable").getOrCreate()

# Sample Data
data = [
    (123, 101, 10.00, "Deposit"),
    (124, 101, 20.00, "Deposit"),
    (125, 101, 5.00, "Withdrawal"),
    (126, 201, 20.00, "Deposit"),
    (128, 201, 10.00, "Withdrawal")
]

# Define Schema
schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("account_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_type", StringType(), True)
])

# Create DataFrame
transaction_df = spark.createDataFrame(data, schema)

# Register as SQL view
transaction_df.createOrReplaceTempView("transactions")

# Show data
transaction_df.show()


# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (select 
# MAGIC sum(case when transaction_type = 'Deposit' then amount
# MAGIC end) as deposits,
# MAGIC sum(case when transaction_type = 'Withdrawal' then (-1*amount)
# MAGIC end) as withdrawal,
# MAGIC deposits + withdrawal as AccountBalance,
# MAGIC account_id from transactions group by account_id)
# MAGIC
# MAGIC select account_id, AccountBalance from cte

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import datetime

# Start Spark session
spark = SparkSession.builder.appName("TransactionsDF").getOrCreate()

# Sample data
data = [
    (1, 1000, "01/25/2021", 50),
    (2, 1000, "03/02/2021", 150),
    (3, 2000, "03/04/2021", 300),
    (4, 3000, "04/15/2021", 100),
    (5, 2000, "04/18/2021", 200),
    (6, 3000, "05/05/2021", 100),
    (7, 4000, "05/10/2021", 500)
]

# Define schema
schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("transaction_date", StringType(), True),  # We'll convert to DateType after
    StructField("amount", IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Convert transaction_date to DateType
from pyspark.sql.functions import to_date
df = df.withColumn("transaction_date", to_date("transaction_date", "MM/dd/yyyy"))

# View the DataFrame
df.show()


# COMMAND ----------

df.createOrReplaceTempView('trans')

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (select user_id,avg(amount) as AvgSpend from trans group by user_id)
# MAGIC select *, dense_rank() over (order by AvgSpend) as Rank from cte

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

# Start Spark session
spark = SparkSession.builder.appName("FraudulentTransactions").getOrCreate()

# Transactions data
transactions_data = [
    (101, 123, "2022-07-08 00:00:00", "Sent", 750),
    (102, 265, "2022-07-10 00:00:00", "Received", 6000),
    (103, 265, "2022-07-18 00:00:00", "Sent", 1500),
    (104, 362, "2022-07-26 00:00:00", "Received", 6000),
    (105, 981, "2022-07-05 00:00:00", "Sent", 3000)
]

transactions_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", IntegerType(), True)
])

transactions_df = spark.createDataFrame(transactions_data, schema=transactions_schema)

# Users data
users_data = [
    (123, "Jessica", False),
    (265, "Daniel", True),
    (362, "Michael", False),
    (981, "Sophia", False)
]

users_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("username", StringType(), True),
    StructField("is_fraudulent", BooleanType(), True)
])

users_df = spark.createDataFrame(users_data, schema=users_schema)

# Show the DataFrames
transactions_df.show()
users_df.show()


# COMMAND ----------

# MAGIC %sql
# MAGIC select users.user_id, users.username from trans  join users on users.user_id = trans.user_id where amount > 1000 and users.is_fraudulent = 'false'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from users

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create Spark session
spark = SparkSession.builder.appName("AdClicksAndSetup").getOrCreate()

# ad_clicks data
ad_clicks_data = [
    (1, 200, "2022-09-01 10:14:00", 4001),
    (2, 534, "2022-09-01 11:30:00", 4003),
    (3, 120, "2022-09-02 14:43:00", 4001),
    (4, 534, "2022-09-03 16:15:00", 4002),
    (5, 287, "2022-09-04 17:20:00", 4001)
]

ad_clicks_schema = StructType([
    StructField("click_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("click_time", StringType(), True),
    StructField("ad_id", IntegerType(), True)
])

ad_clicks_df = spark.createDataFrame(ad_clicks_data, schema=ad_clicks_schema)

# account_setup data
account_setup_data = [
    (1, 200, "2022-09-01 10:30:00"),
    (2, 287, "2022-09-04 17:40:00"),
    (3, 534, "2022-09-01 11:45:00")
]

account_setup_schema = StructType([
    StructField("setup_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("setup_time", StringType(), True)
])

account_setup_df = spark.createDataFrame(account_setup_data, schema=account_setup_schema)

# Show the DataFrames
ad_clicks_df.show()
account_setup_df.show()


# COMMAND ----------

ad_clicks_df.createOrReplaceTempView('click')

# COMMAND ----------

account_setup_df.createOrReplaceTempView('setup')

# COMMAND ----------


