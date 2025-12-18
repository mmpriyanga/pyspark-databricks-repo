# Databricks notebook source
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
data = [('John',28,'IT'), ('Alice', 30,'Finance'), ('Priya',25,'HR')]
df = spark.createDataFrame(data,['Name','Age','Dept'])
#df.show()
df.write.format('delta').mode('overwrite').save('/tmp/delta_demo')
df_read = spark.read.format('delta').load('/tmp/delta_demo')
#df_read.show()
deltaTable = DeltaTable.forPath(spark,'/tmp/delta_demo')



# COMMAND ----------

deltaTable.update(condition="Name ='John'",set={"age":"29"})

# COMMAND ----------

#df_update_read = spark.read.format('delta').load('/tmp/delta_demo')
deltaTable.toDF().show()

# COMMAND ----------

updates = spark.createDataFrame(
    [("Jane", 28, "IT"), ("Alice", 31, "Marketing")],
    ["name", "age", "dept"]
)
deltaTable.alias("t").merge(
    updates.alias("u"),
    "t.name = u.name"
).whenMatchedUpdate(set={"t.age": "u.age","t.dept":"u.dept"}) \
 .whenNotMatchedInsert(values={"name": "u.name", "age": "u.age", "dept": "u.dept"}) \
 .execute()

print("After Update/Delete/Merge:")
deltaTable.toDF().show()

# COMMAND ----------

deltaTable.toDF().show()

# COMMAND ----------

deltaTable.history().show(truncate=False)

# COMMAND ----------

df_old = spark.read.format("delta").option("versionAsOf", 0).load('/tmp/delta_demo')
print("Old Version (v0):")
df_old.show()

# COMMAND ----------

updates_v2 = [()]
