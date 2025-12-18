# Databricks notebook source
from pyspark.sql import functions as F, types as T

click_schema = T.StructType([
    T.StructField("user_id", T.StringType(), False),
    T.StructField("event_ts", T.StringType(), False),
    T.StructField("page", T.StringType(), False),
    T.StructField("referrer", T.StringType(), True),
])

raw_click_df = spark.createDataFrame([
    ("u1", "2025-09-10 10:00:00", "home", None),
    ("u1", "2025-09-10 10:05:00", "prod", "home"),
    ("u1", "2025-09-10 11:00:01", "cart", "prod"),
    ("u2", "2025-09-10 09:00:00", "home", None),
    ("u2", "2025-09-10 09:10:00", "search", "home"),
    ("u2", "2025-09-10 09:45:00", "prod", "search"),
], schema=click_schema)

# Explicit conversion
click_df = raw_click_df.withColumn("event_ts", F.to_timestamp("event_ts"))


# COMMAND ----------

display(click_df)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import lag
w = Window.partitionBy("user_id").orderBy("event_ts")
click_df = click_df.withColumn("lag", lag("event_ts").over(w) ).
                    

# COMMAND ----------

from pyspark.sql.functions import col
click_df = click_df.withColumn("gap", (col("event_ts").cast("long")-col("lag").cast("long"))/60)

# COMMAND ----------

display(click_df)

# COMMAND ----------

click_df.printSchema()

# COMMAND ----------


