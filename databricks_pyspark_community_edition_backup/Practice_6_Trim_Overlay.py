# Databricks notebook source
from pyspark.sql import functions as F, Window

data = [
    ("A", "alice", 100), ("A", "aaron", 120), ("A", "amy", 120),
    ("B", "bob", 90),    ("B", "bella", 110), ("B", "ben", 105)
]
df = spark.createDataFrame(data, ["dept", "emp", "salary"])

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Window
w = Window.partitionBy('dept').orderBy(col('salary').desc())
df = df.withColumn('rank', dense_rank().over(w))
df.filter(col('rank')<= 2).show()


# COMMAND ----------

from pyspark.sql import functions as F, Window

sales = [
    (1, "2025-01-01", 100),
    (1, "2025-01-03", 200),
    (1, "2025-01-07", 300),
    (2, "2025-01-02", 50),
    (2, "2025-01-08", 80),
]
df = spark.createDataFrame(sales, ["prod_id","sale_date","revenue"]) \
    .withColumn("sale_date", F.to_date("sale_date"))

# COMMAND ----------

w = Window.partitionBy('prod_id').rowsBetween(-7, Window.currentRow)

# COMMAND ----------

df.withColumn('rolling_avg', sum('revenue').over(w)).show()

# COMMAND ----------

from pyspark.sql import functions as F, Window

clicks = [
    ("u1", "2025-09-10 10:00:00", "home"),
    ("u1", "2025-09-10 10:10:00", "prod"),
    ("u1", "2025-09-10 11:00:01", "cart"),
    ("u2", "2025-09-10 09:00:00", "home"),
    ("u2", "2025-09-10 09:35:00", "prod"),
]
df = spark.createDataFrame(clicks, ["user_id","event_ts","page"]) \
    .withColumn("event_ts", F.to_timestamp("event_ts"))

# COMMAND ----------

W = Window.partitionBy('user_id').orderBy('event_ts')
df = df.withColumn('prev_time', lag('event_ts').over(W))
df = df.withColumn('time_diff', col('event_ts').cast('long')-col('prev_time').cast('long'))
df = df.withColumn('time_diff', col('time_diff')/60)
df = df.withColumn('time_diff', round('time_diff',2))
df = df.withColumn('new_session', when(col('new_session').isNull(),0).otherwise('1'))
df.show()
df = df.withColumn('new_session', (col('time_diff')>30).cast('int'))
df = df.withColumn('new_session', when(col('new_session').isNull(),0).otherwise('1'))
df.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lpad

spark = SparkSession.builder.getOrCreate()

data = [("1",), ("23",), ("4561111",)]
df = spark.createDataFrame(data, ["num"])

df.withColumn("padded", lpad("num", 5, "0")).show()


# COMMAND ----------

ids = spark.createDataFrame([("7",), ("23",), ("456",)], ["id"])
ids.select(
    F.lpad("id", 5, "0").alias("id_lpad"),
    F.rpad("id", 5, "_").alias("id_rpad")
).show()


# COMMAND ----------

t = spark.createDataFrame([("  abc  ",)], ["raw"])
t.select(F.trim("raw").alias("trim"),
         F.ltrim("raw").alias("ltrim"),
         F.rtrim("raw").alias("rtrim")).show(truncate=False)


# COMMAND ----------

s = spark.createDataFrame([("ABCD1234",)], ["code"])
s.select(
    F.substring("code", 1, 4).alias("prefix"),
    F.length("code").alias("len")
).show()


# COMMAND ----------

o = spark.createDataFrame([("XXXXXXXX",)], ["s"])
o.select(F.overlay("s", "ABCD", 3, 4).alias("patched")).show()
# Replaces 4 chars starting at pos 3 with "ABCD" → "XXABCDXX"


# COMMAND ----------


