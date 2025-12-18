# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("interview").getOrCreate()

data = [
    (1, "John", "Engineering", 95000),
    (2, "Sarah", "Engineering", 120000),
    (3, "Mike", "Engineering", 110000),
    (4, "Lisa", "HR", 75000),
    (5, "Tom", "HR", 85000),
    (6, "Emma", "HR", 80000),
    (7, "David", "Sales", 90000),
    (8, "Anna", "Sales", 105000),
    (9, "James", "Sales", 95000)
]

schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True)
])

df_employees = spark.createDataFrame(data, schema)
df_employees.show()

# COMMAND ----------

#Find the top 2 highest-paid employees in each department along with their salary rank and the salary difference from the highest-paid employee in their department.

from pyspark.sql import Window
from pyspark.sql.functions import *
window_spec = Window.partitionBy(col('department')).orderBy(col('salary').desc())
df_employees = df_employees.withColumn('dense_rank', dense_rank().over(window_spec)).filter(col('dense_rank')<=2)
df_employees = df_employees.withColumn('max_dept_sal', max('salary').over(window_spec))
df_employees = df_employees.withColumn('salary_diff', when(col('max_dept_sal') - col('salary') == 0, 'This is the maximum salary').otherwise(col('max_dept_sal') - col('salary')))


# COMMAND ----------

df_employees.show(truncate=False)

# COMMAND ----------

from datetime import datetime

customers_data = [
    (1, "Alice", "alice@email.com"),
    (2, "Bob", "bob@email.com"),
    (3, "Charlie", "charlie@email.com"),
    (1, "Alice", "alice@email.com"),  # duplicate
    (4, "Diana", "diana@email.com")
]

orders_data = [
    (101, 1, "2024-01-15", 500),
    (102, 1, "2024-02-10", 750),
    (103, 2, "2024-01-20", 300),
    (104, 2, "2024-03-05", 400),
    (105, 3, "2024-01-25", 600),
    (106, 3, "2024-02-28", 550),
    (107, 3, "2024-03-15", 700),
    (108, 4, "2024-02-10", 200)
]

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True)
])

order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("amount", IntegerType(), True)
])

df_customers = spark.createDataFrame(customers_data, customer_schema)
df_orders = spark.createDataFrame(orders_data, order_schema)

df_customers.show()
df_orders.show()

# COMMAND ----------

#Find customers who placed orders in consecutive months, and for each such customer, show their total order value for those consecutive months. Remove any duplicate customer records based on email before processing.
df_customers = df_customers.dropDuplicates()
df_joined = df_customers.alias('c').join(df_orders.alias('o'), df_orders.customer_id ==df_customers.customer_id, how = 'inner' )
window_spec = Window.partitionBy('c.customer_id').orderBy('order_date')
df_joined = df_joined.withColumn('Prev_Month', lag('order_date').over(window_spec))
df_joined = df_joined.withColumn('month_diff', month(col('order_date'))- month(col('Prev_Month')))
# ✅ CORRECT - Note: isNull() not isnull()
df_filtered = df_joined.filter((col('month_diff') == 1) | (col('month_diff').isNull()))
df_final = df_filtered.groupBy(col('c.customer_id')).agg(sum('amount'))
display(df_filtered)


# COMMAND ----------

display(df_joined)

# COMMAND ----------

sales_data = [
    ("Electronics", "Laptop", "North", 1200),
    ("Electronics", "Laptop", "South", 1500),
    ("Electronics", "Phone", "North", 800),
    ("Electronics", "Phone", "South", 900),
    ("Electronics", "Tablet", "North", 600),
    ("Clothing", "Shirt", "North", 50),
    ("Clothing", "Shirt", "South", 60),
    ("Clothing", "Pants", "North", 80),
    ("Clothing", "Pants", "South", 90),
    ("Clothing", "Jacket", "North", 150)
]

sales_schema = StructType([
    StructField("category", StringType(), True),
    StructField("product", StringType(), True),
    StructField("region", StringType(), True),
    StructField("sales", IntegerType(), True)
])

df_sales = spark.createDataFrame(sales_data, sales_schema)
df_sales.show()

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col, sum as spark_sum

# Window spec with orderBy (required for rowsBetween)
window_spec = Window.partitionBy('category', 'region') \
    .orderBy('product') \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_sales = df_sales.withColumn('running_total', spark_sum('sales').over(window_spec))
window_spec = Window.partitionBy('category', 'region')
df_sales = df_sales.withColumn('total_sales', sum('sales').over(window_spec))
df_sales = df_sales.withColumn('percetage_contribution', round((col('sales')/col('total_sales')*100),2))

# COMMAND ----------

df_sales.show()

# COMMAND ----------

quarterly_data = [
    ("Product_A", 2023, 1000, 1200, 1100, 1300),
    ("Product_A", 2024, 1100, 1400, 1250, 1500),
    ("Product_B", 2023, 800, 900, 850, 950),
    ("Product_B", 2024, 900, 1000, 950, 1100),
    ("Product_C", 2023, 500, 600, 550, 650),
    ("Product_C", 2024, 600, 750, 700, 800)
]

quarterly_schema = StructType([
    StructField("product", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("Q1", IntegerType(), True),
    StructField("Q2", IntegerType(), True),
    StructField("Q3", IntegerType(), True),
    StructField("Q4", IntegerType(), True)
])

df_quarterly = spark.createDataFrame(quarterly_data, quarterly_schema)
df_quarterly.show()

# COMMAND ----------

df_quarterly.groupBy('product').pivot('year').agg(avg('Q1')).show()

# COMMAND ----------

hierarchy_data = [
    (1, "CEO", None),
    (2, "VP_Eng", 1),
    (3, "VP_Sales", 1),
    (4, "Eng_Manager", 2),
    (5, "Sales_Manager", 3),
    (6, "Engineer_1", 4),
    (7, "Engineer_2", 4),
    (8, "Sales_Rep_1", 5),
    (9, "Sales_Rep_2", 5),
    (10, "Senior_Eng", 2),
    (11, "Junior_Eng", 10)
]

hierarchy_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("emp_name", StringType(), True),
    StructField("manager_id", IntegerType(), True)
])

df_hierarchy = spark.createDataFrame(hierarchy_data, hierarchy_schema)
df_hierarchy.show()

# COMMAND ----------

from datetime import datetime, timedelta

session_data = [
    (1, "2024-03-15 09:00:00"),
    (1, "2024-03-15 09:15:00"),
    (1, "2024-03-15 09:20:00"),
    (1, "2024-03-15 10:00:00"),  # 40 min gap - new session
    (1, "2024-03-15 10:10:00"),
    (2, "2024-03-15 10:00:00"),
    (2, "2024-03-15 10:05:00"),
    (2, "2024-03-15 10:45:00"),  # 40 min gap - new session
    (2, "2024-03-15 11:00:00"),
    (3, "2024-03-15 11:00:00"),
    (3, "2024-03-15 11:10:00"),
    (3, "2024-03-15 11:15:00")
]

session_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("action_timestamp", StringType(), True)
])

df_sessions = spark.createDataFrame(session_data, session_schema)
df_sessions.show()

# COMMAND ----------

window_spec = Window.partitionBy('user_id').orderBy('action_timestamp')
df_sessions = df_sessions.withColumn('action_timestamp', to_timestamp('action_timestamp', "yyyy-MM-dd HH:mm:ss"))
df_sessions = df_sessions.withColumn('Prev_Session', lag('action_timestamp').over(window_spec))
df_sessions = df_sessions.withColumn('Time_Diff',(unix_timestamp(col('action_timestamp'))-unix_timestamp(col('prev_session')))/60)
df_sessions = df_sessions.withColumn('Flag', when(col('Time_Diff') > 30, 'Session Timeout').otherwise('Ok'))
df_sessions.show()


# COMMAND ----------

# Find customers who have purchased products from at least 3 different categories within a 30-day window. For these customers, calculate their total spending, average order value, and the date range of their purchases.

purchase_data = [
    (1, "2024-01-05", "Electronics", 500),
    (1, "2024-01-10", "Clothing", 200),
    (1, "2024-01-15", "Home", 300),
    (1, "2024-01-20", "Sports", 150),
    (2, "2024-01-05", "Electronics", 600),
    (2, "2024-01-08", "Electronics", 400),
    (2, "2024-02-10", "Clothing", 250),
    (3, "2024-01-05", "Electronics", 700),
    (3, "2024-01-12", "Clothing", 300),
    (3, "2024-01-18", "Home", 400),
    (3, "2024-01-25", "Sports", 200),
    (3, "2024-01-30", "Books", 100),
    (4, "2024-01-05", "Electronics", 800),
    (4, "2024-01-07", "Electronics", 900)
]

purchase_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("purchase_date", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amount", IntegerType(), True)
])

df_purchases = spark.createDataFrame(purchase_data, purchase_schema)
df_purchases.show()

# COMMAND ----------

df_purchases_grouped = df_purchases.groupBy('customer_id').agg(countDistinct('category').alias('count_of_distinct'))
df_joined = df_purchases.alias('dp').join(df_purchases_grouped.alias('ddp'),df_purchases.customer_id == df_purchases_grouped.customer_id, 'inner' )
df_filtered = df_joined.filter(col('count_of_distinct')>=3)
df_filtered.show()
window_spec = Window.partitionBy('dp.customer_id')
with_total_spending = df_filtered.withColumn('total_spending', sum('amount').over(window_spec))
with_avg_spending = with_total_spending.withColumn('avg_spending', avg('amount').over(window_spec))

# COMMAND ----------

with_avg_spending.show()

# COMMAND ----------

quarterly_data = [
    ("Product_A", 2023, 1000, 1200, 1100, 1300),
    ("Product_A", 2024, 1100, 1400, 1250, 1500),
    ("Product_B", 2023, 800, 900, 850, 950),
    ("Product_B", 2024, 900, 1000, 950, 1100),
    ("Product_C", 2023, 500, 600, 550, 650),
    ("Product_C", 2024, 600, 750, 700, 800)
]

quarterly_schema = StructType([
    StructField("product", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("Q1", IntegerType(), True),
    StructField("Q2", IntegerType(), True),
    StructField("Q3", IntegerType(), True),
    StructField("Q4", IntegerType(), True)
])

df_quarterly = spark.createDataFrame(quarterly_data, quarterly_schema)
df_quarterly.show()

# COMMAND ----------

df_quarterly_q1 = df_quarterly.select('product', 'year', col('Q1').alias('sales')).withColumn('quarter', lit('Q1'))
df_quarterly_q2 = df_quarterly.select('product', 'year', col('Q2').alias('sales')).withColumn('quarter', lit('Q2'))
df_quarterly_q3 = df_quarterly.select('product', 'year', col('Q3').alias('sales')).withColumn('quarter', lit('Q3'))
df_quarterly_q4 = df_quarterly.select('product', 'year', col('Q4').alias('sales')).withColumn('quarter', lit('Q4'))
df_union = df_quarterly_q1.union(df_quarterly_q2).union(df_quarterly_q3).union(df_quarterly_q4)

# COMMAND ----------

df_quarterly.show()

# COMMAND ----------

df_union.show()

# COMMAND ----------


