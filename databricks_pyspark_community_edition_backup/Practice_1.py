# Databricks notebook source
data = [
    ("O001", "U001", [{"item_id": "I001", "category": "electronics", "price": 12000},
                      {"item_id": "I002", "category": "books", "price": 400}]),
    ("O002", "U002", [{"item_id": "I003", "category": "clothing", "price": 2500}]),
    ("O003", "U001", [{"item_id": "I004", "category": "electronics", "price": 9000}])
]


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("items", ArrayType(
        StructType([
            StructField("item_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", IntegerType(), True)
        ])
    ), True)
])

df = spark.createDataFrame(data, schema=schema)
df.show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import col, explode

df_filtered = df.withColumn('items', explode(col('items'))) \
    .filter((col('items.category') == 'electronics') & (col('items.price') > 10000))

df_filtered.show(truncate=False)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

# Start Spark session
spark = SparkSession.builder.appName("EmployeeData").getOrCreate()

# --- Table 1: entry_details ---
entry_data = [
    (1000, "login", "2023-06-16 01:00:15.34"),
    (1000, "login", "2023-06-16 02:00:15.34"),
    (1000, "logout", "2023-06-16 03:00:15.34"),
    (1000, "logout", "2023-06-16 12:00:15.34"),
    (1001, "login", "2023-06-16 01:00:15.34"),
    (1001, "login", "2023-06-16 02:00:15.34"),
    (1001, "logout", "2023-06-16 03:00:15.34"),
    (1001, "logout", "2023-06-16 12:00:15.34")
]

entry_schema = StructType([
    StructField("employeeid", IntegerType(), True),
    StructField("entry_details", StringType(), True),
    StructField("timestamp_details", StringType(), True)
])

df_entry = spark.createDataFrame(entry_data, schema=entry_schema)
df_entry.show(truncate=False)

# --- Table 2: employee_details ---
employee_data = [
    (1001, 9999, False),
    (1001, 1111, False),
    (1001, 2222, True),
    (1002, 3333, False)
]

employee_schema = StructType([
    StructField("employeeid", IntegerType(), True),
    StructField("phone_number", IntegerType(), True),
    StructField("isdefault", BooleanType(), True)
])

df_employee = spark.createDataFrame(employee_data, schema=employee_schema)
df_employee.show()


# COMMAND ----------

df_employee.createOrReplaceTempView('emp')

# COMMAND ----------

df_entry.createOrReplaceTempView('ent')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ent

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     emp.employeeid,
# MAGIC     MAX(CASE WHEN emp.isdefault = 'true' THEN emp.phone_number END) AS employee_default_phone_number,
# MAGIC
# MAGIC     COUNT(ent.entry_details) AS totalentry,
# MAGIC
# MAGIC     COUNT(CASE WHEN ent.entry_details = 'login' THEN 1 END) AS totallogin,
# MAGIC     COUNT(CASE WHEN ent.entry_details = 'logout' THEN 1 END) AS totallogout,
# MAGIC
# MAGIC     MAX(CASE WHEN ent.entry_details = 'login' THEN ent.timestamp_details END) AS latestlogin,
# MAGIC     MAX(CASE WHEN ent.entry_details = 'logout' THEN ent.timestamp_details END) AS latestlogout
# MAGIC
# MAGIC FROM emp
# MAGIC LEFT JOIN ent
# MAGIC   ON emp.employeeid = ent.employeeid
# MAGIC
# MAGIC GROUP BY emp.employeeid;
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import date

spark = SparkSession.builder.getOrCreate()

# Customers Data
customers_data = [
    (1, 'Gowtham'),
    (2, 'Sneha'),
    (3, 'Arjun')
]

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True)
])

df_customers = spark.createDataFrame(customers_data, schema=customers_schema)
df_customers.show()

# Orders Data
orders_data = [
    (101, 1, '2024-04-05', 'Shirts'),
    (102, 1, '2024-04-15', 'Shoes'),
    (103, 1, '2024-04-22', 'Watches'),
    (104, 2, '2024-04-10', 'Shirts'),
    (105, 2, '2024-05-11', 'Shirts'),
    (106, 3, '2024-04-03', 'Shoes'),
    (107, 3, '2024-04-14', 'Shoes'),
    (108, 3, '2024-04-29', 'Shoes')
]

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("item", StringType(), True)
])

df_orders = spark.createDataFrame(orders_data, schema=orders_schema)
df_orders.show()


# COMMAND ----------



# COMMAND ----------

df_orders.createOrReplaceTempView('orders')

# COMMAND ----------

df_customers.createOrReplaceTempView('customers')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customers.customer_id, customers.customer_name AS item_count
# MAGIC FROM orders
# MAGIC JOIN customers ON orders.customer_id = customers.customer_id
# MAGIC GROUP BY customers.customer_id,customers.customer_name HAVING count(DISTINCT item) >= 3
# MAGIC

# COMMAND ----------


