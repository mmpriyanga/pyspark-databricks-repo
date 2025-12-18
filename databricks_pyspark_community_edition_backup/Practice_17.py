# Databricks notebook source
spark.conf.set("spark.sql.adaptive.enabled","false")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.get("spark.sql.adaptive.enabled")

# COMMAND ----------

df=spark.read.format('csv').option('inferschema',True).option('header',True).load('dbfs:/FileStore/tables/BigMart_Sales-1.csv')

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

spark.conf.set('spark.sql.files.maxPartitionBytes', 134217728)

# COMMAND ----------

df = df.repartition(10)

# COMMAND ----------

df.withColumn('PartitionID', spark_partition_id()).display()

# COMMAND ----------

df.write.format('parquet').mode('append').option("path","dbfs:/FileStore/tables").save()

# COMMAND ----------

df.display()

# COMMAND ----------

df.filter(col('Outlet_Location_Type') == 'Tier 1').display()

# COMMAND ----------

df.write.format('parquet')\
    .partitionBy('Outlet_Location_Type')\
    .mode('append')\
    .option("path","dbfs:/FileStore/tables/Opt").save()

# COMMAND ----------

df.display()

# COMMAND ----------

df.filter(col('Outlet_Location_Type') == 'Tier 1').display()

# COMMAND ----------

# Big DataFrame
df_transactions = spark.createDataFrame([
    (1, "US", 100),
    (2, "IN", 200),
    (3, "UK", 150),
    (4, "US", 80),
], ["id", "country_code", "amount"])

# Small DataFrame
df_countries = spark.createDataFrame([
    ("US", "United States"),
    ("IN", "India"),
    ("UK", "United Kingdom"),
], ["country_code", "country_name"])


# COMMAND ----------

from pyspark.sql.functions import *
df_transactions.join(broadcast(df_countries), df_countries['country_code'] == df_transactions ['country_code'], 'inner').display()

# COMMAND ----------


