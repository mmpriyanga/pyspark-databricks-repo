# Databricks notebook source
# File location and type
file_location = "/FileStore/tables/BigMart_Sales-2.csv"
file_type = "csv"

# COMMAND ----------

df = spark.read.format('csv').option('header', True).option('inferschema', True).load(file_location)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

df_w = df.select(col("Item_Weight").alias("iw"))
df_w.show()


# COMMAND ----------

display(df_w)

# COMMAND ----------

df.filter(col('Item_Fat_Content') == 'Regular').show()

# COMMAND ----------

display(df.filter((col('Item_Fat_Content') == 'Regular') & (col('Outlet_Location_Type').isin('Tier 1','Tier 2'))))

# COMMAND ----------

from pyspark.sql.functions import *
display(df.withColumn('new_reg', regexp_replace(col('Item_Fat_Content'), 'Regular','Reg')).withColumn('new_reg', regexp_replace(col('Item_Fat_Content'), 'Low Fat','LF')))

# COMMAND ----------

df.sort([col('Item_Outlet_Sales'), col('Outlet_Type')], ascending = [1,0]).show()

# COMMAND ----------

df_split = df.withColumn('Outlet_Type', split('Outlet_Type',' '))

# COMMAND ----------

df_exp = df_split.withColumn('Outlet_Type', explode('Outlet_Type'))

# COMMAND ----------

display(df_exp)

# COMMAND ----------

df.withColumn('outlet_type_bool', array_contains('Outlet_Type','Type3'))

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(avg('Item_MRP')).show()

# COMMAND ----------


