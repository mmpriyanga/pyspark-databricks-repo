# Databricks notebook source
data=[(1,1,1)]

# COMMAND ----------

d = [(1,1,1)]

# COMMAND ----------

df1= spark.createDataFrame([(1,),(1,),(1,)],["id"])
df2 = spark.createDataFrame([(1,),(1,),(1,)],["id"])

# COMMAND ----------

df1.alias("df1df").join(df2, df1["id"] == df2["id"], "inner").show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "64")
spark.conf.set("spark.sql.adaptive.enabled", "true")  # AQE on

# ---- Fact: sales/events (think 100M rows in prod; here a tiny sample)
fact_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("cust_id", StringType(), False),
    StructField("event_date", StringType(), False),        # partition column (date, not ts)
    StructField("event_ts", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("amt", DoubleType(), True),
    StructField("ingestion_ts", StringType(), False),
    StructField("op_type", StringType(), False),         # I/U/D from CDC
    StructField("ingestion_id", StringType(), False)     # run id / batch id
])

fact = spark.createDataFrame([
    ("e1","c1","2025-09-01","2025-09-01 09:05:01","PURCHASE",120.0,"2025-09-01 09:10:00","I","run_001"),
    ("e2","c2","2025-09-01","2025-09-01 09:06:11","BROWSE",  0.0,"2025-09-01 09:10:00","I","run_001"),
    ("e3","c1","2025-09-02","2025-09-02 10:01:00","PURCHASE",80.0,"2025-09-02 10:05:00","I","run_002"),
    ("e4","c3","2025-09-02","2025-09-02 11:00:00","PURCHASE",50.0,"2025-09-02 11:05:00","I","run_002"),
    ("e5","c1","2025-09-02","2025-09-02 12:00:00","REFUND", -20.0,"2025-09-02 12:05:00","U","run_002"),
    (None,"c1","2025-09-02","2025-09-02 12:00:00","REFUND", -20.0,"2025-09-02 12:05:00","U","run_002")
], schema=fact_schema)\
.withColumn("event_date", F.to_date("event_date"))\
.withColumn("event_ts", F.to_timestamp("event_ts"))\
.withColumn("ingestion_ts", F.to_timestamp("ingestion_ts"))

# ---- Dimension: customers (small enough to broadcast)
dim_customers = spark.createDataFrame([
    ("c1","IN","active","seg_A"),
    ("c2","US","active","seg_B"),
    ("c3","IN","inactive","seg_C")
], ["id","country","status","segment"])

# ---- CDC updates to apply via MERGE (Delta/Iceberg)
fact_updates = spark.createDataFrame([
    ("e3","c1","2025-09-02","2025-09-02 10:01:00","PURCHASE",85.0,"2025-09-02 10:30:00","U","run_003"),  # amt changed
    ("e6","c2","2025-09-03","2025-09-03 09:00:00","PURCHASE",60.0,"2025-09-03 09:05:00","I","run_003"),
], fact_schema)\
.withColumn("event_date", F.to_date("event_date"))\
.withColumn("event_ts", F.to_timestamp("event_ts"))\
.withColumn("ingestion_ts", F.to_timestamp("ingestion_ts"))


# COMMAND ----------

fact.withColumn("is_deleted", F.when(F.col("op_type")=="D", F.lit(True)).otherwise(F.lit(False))).show()

# COMMAND ----------

fact.show()

# COMMAND ----------

fact=fact.dropna("any")

# COMMAND ----------

display(fact)

# COMMAND ----------

fact=fact.dropna("all")

# COMMAND ----------

display(fact)

# COMMAND ----------


