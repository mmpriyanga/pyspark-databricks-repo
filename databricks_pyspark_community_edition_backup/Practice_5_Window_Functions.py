# Databricks notebook source
emp_data = [
    (1, "Alice", "Engineering", 95000),
    (2, "Bob", "Engineering", 88000),
    (3, "Charlie", "Engineering", 105000),
    (4, "Diana", "Sales", 75000),
    (5, "Eve", "Sales", 82000),
    (6, "Frank", "Sales", 70000),
    (7, "Grace", "HR", 65000),
    (8, "Henry", "HR", 68000)
]

df_emp = spark.createDataFrame(emp_data, ["emp_id", "name", "department", "salary"])


# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import *
window_spec = Window.partitionBy(col("department")).orderBy(col("salary").desc())
df = df_emp.withColumn("rank", row_number().over(window_spec))
df_final = df.filter(col("rank") <= 2)
df_final.show()

# COMMAND ----------

dup_data = [
    (1, "Alice", "2024-01-01", 5000),
    (1, "Alice", "2024-03-01", 5500),  # Latest
    (1, "Alice", "2024-02-01", 5200),
    (2, "Bob", "2024-01-15", 4500),
    (2, "Bob", "2024-03-15", 5000),    # Latest
    (3, "Charlie", "2024-01-01", 6000)
]

df_dup = spark.createDataFrame(dup_data, ["id", "name", "date", "salary"])

# COMMAND ----------

win_spec = Window.partitionBy("id").orderBy(col("date").desc())
df_dedup = df_dup.withColumn("row_num", row_number().over(win_spec)).filter(col("row_num")==1).drop(col("row_num"))
df_dedup.show()

# COMMAND ----------

sales_data = [
    ("North", "2024-01-01", 1000),
    ("North", "2024-01-02", 1500),
    ("North", "2024-01-03", 1200),
    ("South", "2024-01-01", 800),
    ("South", "2024-01-02", 900),
    ("South", "2024-01-03", 1100),
]

df_sales = spark.createDataFrame(sales_data, ["region", "date", "amount"])

print("\nINPUT DATA:")
df_sales.show()

# COMMAND ----------

win_spec = Window.partitionBy("region").orderBy("date")
df_fin = df_sales.withColumn("cum_col", sum("amount").over(win_spec))
df_fin.show()
