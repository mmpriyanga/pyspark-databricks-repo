# Databricks notebook source
df = spark.createDataFrame(
    [('{"name":"Alice","age":30,"phones":["+91-111","+91-222"]}',),('{"name":"Pri","age":23,"phones":["+91-666","+91-555"]}',)],
    ["json_str"]
)

# COMMAND ----------

df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.types import *
schema = StructType([
    StructField('name',StringType(),True),
    StructField('age',IntegerType(),True),
    StructField('phones',ArrayType(StringType()),True)
])

# COMMAND ----------

df = ([(1,),(2,)])              # ✅ data is fine (list of tuples)
schema = "id string"            # ❌ should be "id STRING"
df = spark.createDataFrame(df,schema)  # ❌ should be createDataFrame (capital F)


# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import *
df_parsed = df.withColumn('json', from_json(col('json_str'),schema)) 

# COMMAND ----------

df_parsed.show(truncate=False)

# COMMAND ----------

df_parsed.select(col('json.name').alias('name')).show()

# COMMAND ----------

df_json =spark.createDataFrame(
 [('{"name":"Priyanga","age":25,"city":"Chennai","hobbies":["Tv","Movies"]}',),
  ('{"name":"Mukil","age":4,"city":"Pondy","hobbies":["Series","Mutta Pidithal"]}',)], ['json_str'])

schema = StructType([
      StructField('name',StringType(),True),
      StructField('age',IntegerType(),True),
      StructField('city',StringType(),True),
      StructField('hobbies',ArrayType(StringType()),True)
  ])

df_parsed = df_json.withColumn('parsed_json', from_json('json_str',schema))
df_parsed.show(truncate=False)


# COMMAND ----------

df_json.show()

# COMMAND ----------

final_json = df_parsed.select('parsed_json.name','parsed_json.age', 'parsed_json.city', col('parsed_json.hobbies')[0].alias('primary_hobby'),col('parsed_json.hobbies')[1].alias('secondary_hobby') )

# COMMAND ----------

final_json.show()

# COMMAND ----------

from pyspark.sql.functions import explode
df_explode = df_parsed.select('parsed_json.name', explode('parsed_json.hobbies').alias('hobby'))


# COMMAND ----------

df_phoneNumber = spark.createDataFrame([('9442745702',),('8754956262',)], ['phone'])

# COMMAND ----------

df_phoneNumber.show()

# COMMAND ----------

df_phoneNumber.withColumn('masked_phone_number', 
                          regexp_replace(col('phone'),r"(\d{4})$", "****")).show()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import SparkSession

# Spark session (skip if you already have 'spark')
spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("payment_mode", StringType(), True),
    StructField("txn_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("txn_ts", StringType(), True),  # ISO timestamp string for realism
])

data = [
    ("u1", "UPI",  "t101", 120.0, "2025-08-01T10:01:00"),
    ("u1", "UPI",  "t102", 250.0, "2025-08-01T11:05:00"),
    ("u1", "UPI",  "t103", 250.0, "2025-08-01T11:06:00"),  # tie with t102
    ("u1", "CARD", "t104", 180.0, "2025-08-02T09:10:00"),
    ("u1", "CARD", "t105", 350.0, "2025-08-02T10:15:00"),
    ("u2", "UPI",  "t201", 500.0, "2025-08-03T08:30:00"),
    ("u2", "UPI",  "t202", 320.0, "2025-08-03T12:45:00"),
    ("u2", "CASH", "t203",  90.0, "2025-08-03T13:00:00"),
    ("u3", "CARD", "t301", 700.0, "2025-08-04T14:20:00"),
    ("u3", "CARD", "t302", 700.0, "2025-08-04T14:21:00"),  # tie
    ("u3", "CARD", "t303", 150.0, "2025-08-04T15:40:00"),
    ("u3", "UPI",  "t304",  60.0, "2025-08-04T16:00:00"),
]

df = spark.createDataFrame(data, schema)
df.show(truncate=False)


# COMMAND ----------

df_grouped = df.groupBy('user_id','payment_mode').agg(sum('amount').alias('total_amount'))

# COMMAND ----------

df_grouped.show()

# COMMAND ----------

from pyspark.sql.window import *
w = Window.partitionBy('user_id','payment_mode').orderBy(col('amount').desc())

# COMMAND ----------

df4 = df.withColumn("rn", row_number().over(w)).withColumn('rnk',rank().over(w)).withColumn('dr',dense_rank().over(w))

# COMMAND ----------

df4.show()

# COMMAND ----------

df4.filter(col('rn')==1).drop('rn').show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime as dt

spark = SparkSession.builder.getOrCreate()

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_date", DateType(), False),
    StructField("customer_id", StringType(), False),
])

items_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("sku", StringType(), False),
    StructField("qty", IntegerType(), False),
    StructField("unit_price", IntegerType(), False),  # keep ints for simplicity
])

returns_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("sku", StringType(), False),
    StructField("return_date", DateType(), False),
])

catalogue_schema = StructType([
    StructField("sku", StringType(), False),
    StructField("category", StringType(), False),
])

orders = spark.createDataFrame([
    ("o1", dt.date(2025, 8, 20), "c1"),
    ("o2", dt.date(2025, 8, 20), "c2"),
    ("o3", dt.date(2025, 8, 21), "c1"),
    ("o4", dt.date(2025, 8, 22), "c3"),
], orders_schema)

items = spark.createDataFrame([
    ("o1", "A101", 2, 100),
    ("o1", "B202", 1, 250),
    ("o2", "A101", 1, 100),
    ("o2", "C303", 3, 80),
    ("o3", "B202", 2, 250),
    ("o4", "D404", 1, 500),
], items_schema)

returns = spark.createDataFrame([
    ("o2", "C303", dt.date(2025, 8, 23)),  # returned entirely
    ("o3", "B202", dt.date(2025, 8, 24)),  # returned entirely
], returns_schema)

catalogue = spark.createDataFrame([
    ("A101", "Accessories"),
    ("B202", "Accessories"),
    ("C303", "Kitchen"),
    ("E505", "Gadgets"),  # never sold
], catalogue_schema)

orders.createOrReplaceTempView("orders")
items.createOrReplaceTempView("order_items")
returns.createOrReplaceTempView("returns")
catalogue.createOrReplaceTempView("catalogue")


# COMMAND ----------

orders.show()

# COMMAND ----------

items.show()

# COMMAND ----------

from pyspark.sql.functions import col
df_up = items.withColumn('total_sale', col('qty') * col('unit_price') )
df_tot_with_ret = df_up.join(returns, (df_up['sku'] == returns['sku']) & (df_up['order_id'] == returns['order_id']), 'anti' )

# COMMAND ----------

df_tot_with_ret.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("PySparkPracticeDatasets").getOrCreate()

# COMMAND ----------

q1_schema = StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True)
])
q1_df = spark.createDataFrame(
    [("Priya","Maran"), ("Mukil",None), (None,"K"), ("", "Singh")],
    schema=q1_schema
)

# COMMAND ----------

from pyspark.sql.functions import concat_ws

q1_df = q1_df.withColumn(
    "full_name",
    concat_ws(" ", col("first_name"), col("last_name"))
)

# COMMAND ----------

q1_df.show()

# COMMAND ----------

#From df(id, ts_string='2024-08-31 12:34:56'), parse to timestamp and extract date, hour, weekofyear.

q2_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("ts_string", StringType(), False)
])
q2_df = spark.createDataFrame(
    [(1,"2024-08-31 12:34:56"), (2,"2025-01-01 00:00:00"), (3,"2025-09-01 23:59:59")],
    schema=q2_schema
)


# COMMAND ----------

from pyspark.sql.functions import *
q2_df = q2_df.withColumn('ts_ts', to_timestamp('ts_string', 'yyyy-MM-dd HH:mm:ss'))
q2_df = q2_df.withColumn('date', to_date('ts_ts')).withColumn('hour', hour('ts_ts')).withColumn('week_of_year', weekofyear('ts_ts'))

# COMMAND ----------

q2_df.show()

# COMMAND ----------

q3_schema = StructType([
    StructField("txn_id", IntegerType(), False),
    StructField("amount", DoubleType(), False)
])
q3_df = spark.createDataFrame(
    [(1, 999.99), (2, 1000.0), (3, 2500.5), (4, 5000.0), (5, 5000.01)],
    schema=q3_schema
)

# COMMAND ----------

q3_df = q3_df.filter((col('amount')>=1000) & (col('amount')<=5000))
q3_df.show()

# COMMAND ----------

q4_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("product_id", StringType(), False)
])
q4_df = spark.createDataFrame(
    [("u1","p1"), ("u1","p1"), ("u1","p2"), ("u2","p2"), ("u3","p3")],
    schema=q4_schema
)

# COMMAND ----------

q4_df=q4_df.distinct().count()

# COMMAND ----------

q4_df

# COMMAND ----------

a = 10

# COMMAND ----------


