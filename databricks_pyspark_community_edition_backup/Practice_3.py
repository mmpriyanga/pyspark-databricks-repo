# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Find Missing Numbers").getOrCreate()

# Sample data
data = [(1,), (2,), (4,), (5,), (7,), (8,), (10,)]
df_numbers = spark.createDataFrame(data, ["Number"])

# Generating a complete sequence DataFrame
full_range = spark.range(1, 11).toDF("Number")

# Finding missing numbers
missing_numbers = full_range.join(df_numbers, "Number", "left_anti")
missing_numbers.show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

full_range.show()

# COMMAND ----------

data = [('1','2023-12-01',100), ('1','2023-12-02',150),
        ('2','2023-12-01',200), ('2','2023-12-02',250)]

# COMMAND ----------

schema = ['id','date','sales']
df = spark.createDataFrame(data,schema)

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import  *
from pyspark.sql.types import *


# COMMAND ----------

df = df.withColumn('date', col('date').cast(DateType()))

# COMMAND ----------

df.show()

# COMMAND ----------

df = df.orderBy('id','date', ascending = [1,0]).dropDuplicates(subset = ['id'])

# COMMAND ----------

df.display()

# COMMAND ----------

data= [('1', 'Mukil'),
       ('2', 'Pri'),
       ('3','Santhi'),
       ('4','Pri')]
schema = ('id','name')
df = spark.createDataFrame(data, schema)
df.display()
df = df.dropDuplicates (subset=['name']).display()

# COMMAND ----------

df.orderBy('id', ascending = 0).display()

# COMMAND ----------

df

# COMMAND ----------

data =[('user1',5), ('user2',8),
       ('user3',2),('user4',10),('user2',3)]

# COMMAND ----------

schema = ('user_id','sessions')

# COMMAND ----------

df = spark.createDataFrame(data, schema)

# COMMAND ----------

df.display()

# COMMAND ----------

df.groupBy('user_id').agg(sum('sessions').alias('TotalSessions')).orderBy('TotalSessions', ascending = 0).display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

data = [('1','2023-12-01',100), ('1','2023-12-02',150),
        ('2','2023-12-01',200), ('2','2023-12-02',250)]

# COMMAND ----------

schema = ('id','date','sales')

# COMMAND ----------

df = spark.createDataFrame(data,schema)

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import  *
from pyspark.sql.types import *
df = df.withColumn('date', col('date').cast(DateType()))

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df.withColumn('rank', dense_rank().over(Window.partitionBy('id').orderBy (col('date').desc()))).filter(col('rank') ==1 ).display() 

# COMMAND ----------

data = [('cust1', "2025-04-27", 300),
        ('cust2', "2025-01-27", 300),
        ('cust3', "2025-02-27", 300),
        ('cust3', "2025-04-27", 300)]

# COMMAND ----------

schema = ('userid', 'date_purchase', 'sales')
df = spark.createDataFrame(data,schema)

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('date_purchase', col('date_purchase').cast(DateType()))

# COMMAND ----------

from pyspark.sql.functions import max as  spark_max, col, current_date, datediff
latest_date = df.groupBy('userid').agg(max(col('date_purchase')).alias('lastpurchase'))
df = latest_date.withColumn('datediff', datediff(current_date(),'lastpurchase')).filter(col('datediff') > 30)

df.display()




# COMMAND ----------

df.display()

# COMMAND ----------

data = [("customer1", "The product is great"), ("customer2", "Great product, fast delivery"), ("customer3", "Not bad, could be better")]
schema = ('custid','review')
df = spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.withColumn('review',explode('review'))

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import split, explode, col

# Step 1: Create DataFrame
data = [("customer1", "The product is great"),
        ("customer2", "Great product, fast delivery"),
        ("customer3", "Not bad, could be better")]

schema = ('custid', 'review')
df = spark.createDataFrame(data, schema)

# Step 2: Split reviews into arrays of words
df_words = df.withColumn("word", explode(split(col("review"), "\\s+")))




# COMMAND ----------

df_words.display()

# COMMAND ----------

from pyspark.sql.functions import split, explode, col

# Step 1: Create DataFrame
data = [("customer1", "The product is great"),
        ("customer2", "Great product, fast delivery"),
        ("customer3", "Not bad, could be better")]

schema = ('custid', 'review')
df = spark.createDataFrame(data, schema)

# Step 2: Split reviews into arrays of words
df_words = df.withColumn("word", explode(split(col("review"),' ')))

# Step 3: Optional - normalize case, remove punctuation (for cleaner counts)
from pyspark.sql.functions import lower, regexp_replace

df_words = df_words.withColumn("word", lower(regexp_replace("word", "[^a-zA-Z]", "")))

# Step 4: Word count
word_count = df_words.groupBy("word").count().orderBy("count", ascending=False)

word_count.display()


# COMMAND ----------

data = [('1','2023-12-01',100), ('1','2023-12-02',150),
        ('2','2023-12-01',200), ('2','2023-12-02',250)]
schema = ('prod_id','date_prod','sale')

# COMMAND ----------

df = spark.createDataFrame(data,schema)

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df.withColumn('cum_val', sum('sale').over(Window.partitionBy('prod_id').orderBy('date_prod'))).display()

# COMMAND ----------

df = df.withColumn('date_prod', to_date('date_prod'))

# COMMAND ----------

df.display()

# COMMAND ----------

data = [("john",25), ("Jane",30), ("john",25), ("Alice",22)]

# COMMAND ----------

schema = ('Name', "Age")
df = spark.createDataFrame(data,schema)

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('rownum', row_number().over(Window.partitionBy('Name').orderBy('Age'))).filter(col('rownum') == 1).display()

# COMMAND ----------

df.dropDuplicates(subset = ['Name','Age']).display()

# COMMAND ----------

data = [('1','2023-12-01',50), ('1','2023-12-02',60),
        ('2','2023-12-01',45), ('2','2023-12-02',75),
        ('2','2023-11-02',90),('1','2023-11-02',80),
        ('1','2023-11-02',75),('2','2023-6-02',75)]
schema = ('user_id','date','activity')
df = spark.createDataFrame(data, schema)

# COMMAND ----------

df.withColumn('Avg_time', avg('activity').over(Window.partitionBy('user_id'))).display()

# COMMAND ----------

df.groupBy('user_id',month('date') ).agg(avg('activity')).display()

# COMMAND ----------

df = df.withColumn('date', to_date('date'))

# COMMAND ----------

df.withColumn('rownum', sum('sales').over(Window.partitionBy('prod_id', month('date'))))

# COMMAND ----------

data = [('1','2023-12-01',1100), ('1','2023-12-02',200),
        ('2','2023-12-01',150), ('2','2023-12-02',250)]
schema = ('prod_id','date_prod','sale')
df = spark.createDataFrame(data,schema)
df = df.withColumn('date_prod', to_date('date_prod'))
df = df.withColumn('tot_amt', sum('sale').over(Window.partitionBy('prod_id', month('date_prod'))))
df = df.withColumn('rownum', row_number().over(Window.partitionBy(month('date_prod')).orderBy(col('tot_amt').desc()))).display()

# COMMAND ----------

data = [("Alice", "HR"), ("Bob", "Finance"), ("Charlie", "HR"), ("David", "Engineering"), ("Eve", "Finance")]
columns = ["employee_name", "department"]

df = spark.createDataFrame(data, columns)
df.display()


# COMMAND ----------

from pyspark.sql.functions import count
df.groupBy('department').agg(count('employee_name')).display()

# COMMAND ----------

data = [("product1", 100), ("product2", 300), ("product3", 50)]
columns = ["product_id", "sales"]

df = spark.createDataFrame(data, columns)
df.display()

from pyspark.sql.functions import col, when

df = df.withColumn('flag_price', when(col('sales') > 100, 'expensive').otherwise('inexpensive'))
df.show()

# COMMAND ----------

data = [("product1", 100), ("product2", 200), ("product3", 300)]
columns = ["product_id", "sales"]

df = spark.createDataFrame(data, columns)
df.display()


# COMMAND ----------

from pyspark.sql.functions import *
df = df.withColumn('Timestamp', current_timestamp())

# COMMAND ----------

df.display()

# COMMAND ----------

df.createOrReplaceTempView('tempview')

# COMMAND ----------

spark.sql("select * from tempview").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from tempview

# COMMAND ----------

df.createOrReplaceGlobalTempView('global_temp')


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from global_temp.global_temp

# COMMAND ----------

data = [("product1", {"price": 100, "quantity": 2}),
        ("product2", {"price": 200, "quantity": 3})]

columns = ["product_id", "product_info"]

df = spark.createDataFrame(data, columns)

df.display()


# COMMAND ----------

df.createOrReplaceGlobalTempView('nest')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select product_info.price as price, product_info.quantity as quan from global_temp.nest

# COMMAND ----------

df.write.format("parquet").mode("append").partitionBy("category").save("/Users")


# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WriteParquetExample").getOrCreate()

data = [
    ("Apple", "Fruit", 100),
    ("Carrot", "Vegetable", 60),
    ("Banana", "Fruit", 80),
    ("Broccoli", "Vegetable", 90)
]

columns = ["item", "category", "price"]

df = spark.createDataFrame(data, columns)


# COMMAND ----------

df.write.format('parquet').option("compression","snappy")

# COMMAND ----------

data = [
    ("Country A", 5000000, 300000001),
    ("Country B", 20000000, 1200000000),
    ("Country C", 1000000, 50000000),
    ("Country D", 50000000, 4000000000),
    ("Country E", 10000000, 600000000)
]

# Create DataFrame
columns = ["country", "population", "GDP"]
df = spark.createDataFrame(data, columns)


# COMMAND ----------

df =  df.withColumn('GDP/Capita', round(col('GDP')/col('population')))

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

df.withColumn('gdp', col('gdp').cast(IntegerType()) )

# COMMAND ----------

df.display()

# COMMAND ----------

df.select((min('GDP/Capita'))).display()

# COMMAND ----------

df.orderBy(col('GDP/Capita').desc()).display()

# COMMAND ----------

# Sample dataset
data = [
    (2023, 1, 50000),
    (2023, 2, 60000),
    (2023, 3, 55000),
    (2023, 4, 70000),
    (2023, 5, 65000),
    (2023, 6, 60000),
    (2023, 7, 75000),
    (2023, 8, 85000),
    (2023, 9, 70000),
    (2023, 10, 90000),
    (2023, 11, 95000),
    (2023, 12, 100000)
]
schema = ('year', 'monnth','sale')

# COMMAND ----------

df = spark.createDataFrame(data, schema)

# COMMAND ----------

df.display()

# COMMAND ----------

df.orderBy(col('sale').desc()).limit(3).display()

# COMMAND ----------

df.withColumn('yearMonth', concat_ws('-',col('year'),col('monnth'))).select('yearMonth','sale').orderBy(col('sale').desc()).limit(3).display()

# COMMAND ----------

df.withColumn('yearMonth', concat_ws('-',col('year'),col('monnth')))

# COMMAND ----------

data = [("Electronics", "Laptop"), 
        ("Electronics", "Smartphone"), 
        ("Furniture", "Chair"), 
        ("Furniture", "Table")]

columns = ["category", "product"]

df = spark.createDataFrame(data, columns)

df.display()


# COMMAND ----------

from pyspark.sql.functions import *
df = df.groupBy('category').agg(collect_list('product')).alias('product')
df.display()

# COMMAND ----------

data = [
    (101, "P001"),
    (101, "P002"),
    (102, "P001"),
    (102, "P001"),
    (101, "P001")
]

columns = ["customer_id", "product_id"]

df = spark.createDataFrame(data, columns)

df.display()


# COMMAND ----------



df.groupBy("customer_id") \
  .agg(collect_set("product_id").alias("unique_products")) \
  .display()


# COMMAND ----------

data = [
    ("John", "Doe", "john.doe@example.com"),
    ("Jane", "Smith", None)
]

columns = ["first_name", "last_name", "email"]

df = spark.createDataFrame(data, columns)

df.display()


# COMMAND ----------

from pyspark.sql.functions import *
df = df.withColumn('FullName', when(col('email').isNotNull(), concat('first_name','last_name')).otherwise(None))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, StringType

# Sample data
data = [
    (1, ["prod1", "prod2", "prod3"]),
    (2, ["prod4"]),
    (3, ["prod5", "prod6"])
]

# Define schema
myschema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("product_ids", ArrayType(StringType()), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema=myschema)

df.display()


# COMMAND ----------

df.withColumn('numitems', size(col('product_ids'))).display()

# COMMAND ----------

data = [
    ("911234567890",),
    ("811234567890",),
    ("912345678901",)
]

schema = ["phone_number"]

df = spark.createDataFrame(data, schema)
df.display()


# COMMAND ----------

df.withColumn('flag', (when(col('phone_number').) =='91','Yes').otherwise('No'))

# COMMAND ----------

df.select(col('phone_number')[0:2]).display()

# COMMAND ----------

df1 = [('32A','abc'),('456','bcd')]

df2 = [('32A','Ram')]

schema1 = ('store_id','drug')

schema2 = ('store_id','patient')

df_drug = spark.createDataFrame(df1,schema1)

df_patient = spark.createDataFrame(df2,schema2)

df_join = df_drug.join(df_patient, on = 'store_id',how ='left').show()

# COMMAND ----------

from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("Join Example").getOrCreate()

# Employee data
data1 = [
    ('1', 'gaur', 'd01'),
    ('2', 'kit', 'd02'),
    ('3', 'sam', 'd03'),
    ('4', 'tim', 'd03'),
    ('5', 'aman', 'd05'),
    ('6', 'nad', 'd06')
]
schema1 = 'emp_id STRING, emp_name STRING, dept_id STRING'
df1 = spark.createDataFrame(data1, schema=schema1)

# Department data
data2 = [
    ('d01', 'HR'),
    ('d02', 'Marketing'),
    ('d03', 'Accounts'),
    ('d04', 'IT'),
    ('d05', 'Finance')
]
schema2 = 'dept_id STRING, dept_name STRING'
df2 = spark.createDataFrame(data2, schema=schema2)
df1.display()
df2.display()

# COMMAND ----------

df1.join(df2, on = 'dept_id', how = 'inner').display()

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], 'inner').display()

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], 'anti').display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

spark = SparkSession.builder.appName("InterviewPrep").getOrCreate()

# EMP TABLE
emp_data = [(1, "John", "Delhi"),
            (2, "Priya", "Bangalore"),
            (3, "Ahmed", "Mumbai")]

emp_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("address", StringType(), True)
])

emp_df = spark.createDataFrame(data=emp_data, schema=emp_schema)

# SAL TABLE
from datetime import date

sal_data = [(1, date(2025,5,1), 50000),
            (2, date(2025,4,1), 48000),
            (1, date(2025,4,1), 50000),
            (3, date(2025,5,2), 55000)]

sal_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("dep_date", DateType(), True),
    StructField("dep_amount", IntegerType(), True)
])

sal_df = spark.createDataFrame(data=sal_data, schema=sal_schema)

emp_df.show()
sal_df.show()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT emp_id) 
# MAGIC FROM sal_df 
# MAGIC WHERE MONTH(dep_date) = MONTH(CURRENT_DATE) 
# MAGIC   AND YEAR(dep_date) = YEAR(CURRENT_DATE);

# COMMAND ----------


emp_df.createTempView('emp')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT emp.emp_id 
# MAGIC FROM emp 
# MAGIC LEFT JOIN sal_df
# MAGIC   ON emp.emp_id = sal_df.emp_id 
# MAGIC   AND MONTH(sal_df.dep_date) = MONTH(CURRENT_DATE)
# MAGIC   AND YEAR(sal_df.dep_date) = YEAR(CURRENT_DATE)
# MAGIC WHERE sal_df.emp_id IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT emp_id, dep_date, dep_amount,
# MAGIC        ROW_NUMBER() OVER (PARTITION BY emp_id ORDER BY dep_date DESC) AS rn
# MAGIC FROM sal_df
# MAGIC QUALIFY rn = 1;
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import date

# Create Spark session
spark = SparkSession.builder.appName("SQLInterviewPractice").getOrCreate()

# EMP table
emp_data = [
    (1, "John", "Delhi"),
    (2, "Priya", "Bangalore"),
    (3, "Ahmed", "Mumbai")
]

emp_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("address", StringType(), True)
])

emp_df = spark.createDataFrame(emp_data, emp_schema)
emp_df.createOrReplaceTempView("emp")

# SAL table
sal_data = [
    (1, date(2025, 5, 1), 50000),
    (2, date(2025, 4, 1), 48000),
    (1, date(2025, 4, 1), 50000),
    (3, date(2025, 5, 2), 55000)
]

sal_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("dep_date", DateType(), True),
    StructField("dep_amount", IntegerType(), True)
])

sal_df = spark.createDataFrame(sal_data, sal_schema)
sal_df.createOrReplaceTempView("sal")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH cte AS (
# MAGIC   SELECT
# MAGIC          emp_id, dep_amount,ROW_NUMBER() OVER (PARTITION BY emp_id ORDER BY dep_date) AS rownum
# MAGIC   FROM sal
# MAGIC )  
# MAGIC SELECT * from cte WHERE rownum = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select emp_id, count(emp_id) as cnt from sal where year(dep_date) = year(current_date()) group by emp_id having cnt > 1

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Start Spark session
spark = SparkSession.builder.appName("Company Data").getOrCreate()

# Employees DataFrame
employees_data = [
    (1, "John", "Doe", 100),
    (2, "Jane", "Smith", 200),
    (3, "Jim", "Brown", 300)
]
employees_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("department_id", IntegerType(), True)
])
employees_df = spark.createDataFrame(employees_data, schema=employees_schema)

# Departments DataFrame
departments_data = [
    (100, "Engineering"),
    (200, "Marketing"),
    (300, "Human Resources")
]
departments_schema = StructType([
    StructField("department_id", IntegerType(), True),
    StructField("department_name", StringType(), True)
])
departments_df = spark.createDataFrame(departments_data, schema=departments_schema)

# Projects DataFrame
projects_data = [
    (50, "System Design", 100),
    (60, "Brand Campaign", 200)
]
projects_schema = StructType([
    StructField("project_id", IntegerType(), True),
    StructField("project_name", StringType(), True),
    StructField("department_id", IntegerType(), True)
])
projects_df = spark.createDataFrame(projects_data, schema=projects_schema)

# Show the data
employees_df.show()
departments_df.show()
projects_df.show()



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select employees_df.employee_id from employees_df join 

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

# Start or retrieve Spark session
spark = SparkSession.builder.appName("Employee Projects").getOrCreate()

# Define employee_projects DataFrame
employee_projects_data = [
    (1, 50),
    (2, 60),
    (1, 60),
    (3, 50)
]
employee_projects_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("project_id", IntegerType(), True)
])
employee_projects_df = spark.createDataFrame(employee_projects_data, schema=employee_projects_schema)

# Display the employee_projects DataFrame
employee_projects_df.show()


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select emp_df.first_name, emp_df.last_name, count(emp_prj_df.employee_id) from emp_prj_df join emp_df on emp_df.employee_id = emp_prj_df.employee_id group by emp_df.first_name, emp_df.last_name,emp_df.employee_id

# COMMAND ----------

employee_projects_df.createOrReplaceTempView('emp_prj_df')
employees_df.createOrReplaceTempView('emp_df')

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("NullCount").getOrCreate()

data = [
    ("Alice", 25, "F"),
    ("Bob", None, "M"),
    (None, 30, None),
    ("David", None, "M"),
    ("Eve", 45, None)
]

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True)
])

df = spark.createDataFrame(data, schema)
df.show()


# COMMAND ----------

from pyspark.sql.functions import col, sum, when

df_count = df.select([
    sum(when(col('name').isNull(), 1).otherwise(0)),
    sum(when(col('age').isNull(), 1).otherwise(0))])
df_count.show()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("TopPaidEmployees").getOrCreate()

data = [
    (1, "Amit", "IT", 90000),
    (2, "Neha", "HR", 50000),
    (3, "Raj", "IT", 85000),
    (4, "Priya", "HR", 60000),
    (5, "Suresh", "Finance", 75000),
    (6, "Anjali", "Finance", 80000),
    (7, "Vikas", "IT", 92000),
    (8, "Rohan", "HR", 58000),
    (9, "Meera", "Finance", 82000)
]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)
df.show()


# COMMAND ----------

from pyspark.sql.window import Window
df1 = df.withColumn('Rank_Salary', rank().over(Window.partitionBy('department').orderBy(col('salary').desc()))).filter((col('Rank_Salary') == 1) | (col('Rank_Salary') == 2)  )
df1.display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("CityStateDF").getOrCreate()

data = [
    (101, "Mumbai", "Maharashtra"),
    (102, "Delhi", "Delhi"),
    (103, "Bangalore", "Karnataka"),
    (101, "Mumbai", "Maharashtra"),
    (104, "Pune", "Maharashtra")
]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True)
])

df = spark.createDataFrame(data, schema)
df.show()


# COMMAND ----------

df.dropDuplicates(['id','city','state']).display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("NullValuesDF").getOrCreate()

data = [
    (1, "Alice", 25),
    (2, None, 30),
    (3, "Charlie", None),
    (4, None, None),
    (5, "Eve", 45)
]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)
df.show()


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, lag

window_spec = Window.orderBy("id")

df_with_filled_name = df.withColumn(
    "name_filled",
    when(col("name").isNull(), lag("name").over(window_spec)).otherwise(col("name"))
)

df_with_filled_name.show()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import to_date

spark = SparkSession.builder.appName("MovingAverage").getOrCreate()

data = [
    (1, "2023-01-01", 1000),
    (1, "2023-02-01", 1500),
    (1, "2023-03-01", 1200),
    (1, "2023-04-01", 1700),
    (1, "2023-05-01", 1600),
    (1, "2023-06-01", 1800),
]

schema = StructType([
    StructField("store_id", IntegerType(), True),
    StructField("date", StringType(), True),
    StructField("sales", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)
df = df.withColumn("date", to_date("date", "yyyy-MM-dd"))
df.show()


# COMMAND ----------

df.withColumn('moving_avg', avg('sales').over(Window.orderBy('date').rowsBetween(-2,0))).display()

# COMMAND ----------

colors = ['red', 'green', 'blue']

for index, color in enumerate(colors):
    print(index, color)


# COMMAND ----------

colors = ['red', 'green', 'blue']

for index, color in enumerate(colors):
    print(index, color)


# COMMAND ----------

#find top 3 movie based on the rating

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Initialize Spark Session
#spark = SparkSession.builder.appName("TopMovies").getOrCreate()

# Sample DataFrames
data_movies = [(1, "Movie A"), (2, "Movie B"), (3, "Movie C"), (4, "Movie D"), (5, "Movie E")]

data_ratings = [(1, 101, 4.5), (1, 102, 4.0), (2, 103, 5.0), 
                (2, 104, 3.5), (3, 105, 4.0), (3, 106, 4.0), 
                (4, 107, 3.0), (5, 108, 2.5), (5, 109, 3.0)]

columns_movies = ["MovieID", "MovieName"]
columns_ratings = ["MovieID", "UserID", "Rating"]

# Creating DataFrames
df_movies = spark.createDataFrame(data_movies, columns_movies)
df_ratings = spark.createDataFrame(data_ratings, columns_ratings)

# COMMAND ----------

df_movies.display()
df_ratings.display()

# COMMAND ----------

(df_movies.join(df_ratings, on = 'MovieID', how='inner'))
.groupBy('MovieName')
.agg(avg('Rating').alias('avg_rate')).(col(col(orderBy('avg_rate')).desc())).display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, count, rank
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data
data = [
    (8, 4,  "2021-08-24 22:46:07", "Houston"),
    (4, 8,  "2021-08-24 22:57:13", "Houston"),
    (5, 1,  "2021-08-11 21:28:44", "Houston"),
    (8, 3,  "2021-08-17 22:04:15", "Houston"),
    (11, 3, "2021-08-17 13:07:00", "New York"),
    (8, 11, "2021-08-17 14:22:22", "New York"),
]

# Create DataFrame
columns = ["caller_id", "recipient_id", "call_time", "city"]
df = spark.createDataFrame(data, columns)

# Convert call_time to timestamp
df = df.withColumn("call_time", col("call_time").cast("timestamp"))


# COMMAND ----------

df_view = df.createOrReplaceTempView('myview')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from myview

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select city, hour(call_time), count(hour(call_time) ), RANK() over (partition by city order by count(hour(call_time)) desc) as rn from myview  group by hour(call_time), city qualify rn = 1 

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("EmployeesDF").getOrCreate()

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# Sample data
data = [
    (1, 'John', 'Sales', 60000),
    (2, 'Jane', 'Sales', 55000),
    (3, 'Bob', 'Marketing', 65000),
    (4, 'Sue', 'Marketing', 70000),
    (5, 'Mike', 'IT', 80000),
    (6, 'Lisa', 'IT', 75000)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Display DataFrame
df.show()


# COMMAND ----------

df.createOrReplaceTempView('mysql')


# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select id, name, department, salary from 
# MAGIC (select *,row_number() over (partition by department order by salary desc) rn from mysql) as ranked
# MAGIC where rn <= 3

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("CallLogs").getOrCreate()

# Define schema
schema = StructType([
    StructField("caller_id", IntegerType(), True),
    StructField("recipient_id", IntegerType(), True),
    StructField("call_time", TimestampType(), True),
    StructField("city", StringType(), True)
])

# Sample data
data = [
    (8, 4, datetime.strptime("2021-08-24 22:46:07", "%Y-%m-%d %H:%M:%S"), "Houston"),
    (4, 8, datetime.strptime("2021-08-24 22:57:13", "%Y-%m-%d %H:%M:%S"), "Houston"),
    (5, 1, datetime.strptime("2021-08-11 21:28:44", "%Y-%m-%d %H:%M:%S"), "Houston"),
    (8, 3, datetime.strptime("2021-08-17 22:04:15", "%Y-%m-%d %H:%M:%S"), "Houston"),
    (11, 3, datetime.strptime("2021-08-17 13:07:00", "%Y-%m-%d %H:%M:%S"), "New York"),
    (8, 11, datetime.strptime("2021-08-17 14:22:22", "%Y-%m-%d %H:%M:%S"), "New York")
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Display DataFrame
df.show(truncate=False)


# COMMAND ----------

df.createOrReplaceTempView('myint')

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, DateType
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("TransactionsDF").getOrCreate()

# Define schema
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_date", DateType(), True)
])

# Sample data
data = [
    (1, 100.00, datetime.strptime("2021-05-01", "%Y-%m-%d")),
    (2, 50.00, datetime.strptime("2021-05-10", "%Y-%m-%d")),
    (3, 75.00, datetime.strptime("2021-06-05", "%Y-%m-%d")),
    (1, 200.00, datetime.strptime("2021-07-01", "%Y-%m-%d")),
    (4, 150.00, datetime.strptime("2021-08-02", "%Y-%m-%d")),
    (3, 300.00, datetime.strptime("2021-09-10", "%Y-%m-%d")),
    (5, 500.00, datetime.strptime("2021-09-15", "%Y-%m-%d")),
    (1, 400.00, datetime.strptime("2021-09-20", "%Y-%m-%d")),
    (6, 250.00, datetime.strptime("2021-10-01", "%Y-%m-%d"))
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()


# COMMAND ----------

df.createOrReplaceTempView('view')

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select add_months(curdate(), -6)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import datetime

spark = SparkSession.builder.appName("CustomerOrders").getOrCreate()

# Schema for customers
customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# Data for customers
customers_data = [
    (1, 'John'),
    (2, 'Jane'),
    (3, 'Bob'),
    (4, 'Alice'),
    (5, 'Mike')
]

# Create DataFrame
df_customers = spark.createDataFrame(customers_data, schema=customer_schema)
df_customers.show()


# COMMAND ----------

# Schema for orders
orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", DateType(), True)
])

# Sample data (expand as needed)
orders_data = [
    (1, 1, datetime.strptime('2020-01-01', '%Y-%m-%d')),
    (2, 2, datetime.strptime('2020-01-02', '%Y-%m-%d')),
    (3, 3, datetime.strptime('2020-01-03', '%Y-%m-%d')),
    (4, 4, datetime.strptime('2020-01-04', '%Y-%m-%d')),
    (5, 5, datetime.strptime('2020-01-05', '%Y-%m-%d')),
    (6, 1, datetime.strptime('2020-02-01', '%Y-%m-%d')),
    (7, 2, datetime.strptime('2020-02-02', '%Y-%m-%d')),
    (8, 3, datetime.strptime('2020-02-03', '%Y-%m-%d')),
    (9, 4, datetime.strptime('2020-02-04', '%Y-%m-%d')),
    (10, 5, datetime.strptime('2020-02-05', '%Y-%m-%d')),
    (11, 1, datetime.strptime('2020-03-01', '%Y-%m-%d')),
    (12, 2, datetime.strptime('2020-03-02', '%Y-%m-%d')),
    (13, 3, datetime.strptime('2020-03-03', '%Y-%m-%d')),
    (14, 4, datetime.strptime('2020-03-04', '%Y-%m-%d')),
    (15, 5, datetime.strptime('2020-03-05', '%Y-%m-%d')),
    # ... you can add more entries similarly
    (361, 1, datetime.strptime('2020-12-01', '%Y-%m-%d')),
    (362, 2, datetime.strptime('2020-12-02', '%Y-%m-%d')),
    (363, 3, datetime.strptime('2020-12-03', '%Y-%m-%d')),
    (364, 4, datetime.strptime('2020-12-04', '%Y-%m-%d')),
    (365, 5, datetime.strptime('2020-12-05', '%Y-%m-%d'))
]

# Create DataFrame
df_orders = spark.createDataFrame(orders_data, schema=orders_schema)
df_orders.show()


# COMMAND ----------

df_customers.createOrReplaceTempView('cust')
df_orders.createOrReplaceTempView('orders')

# COMMAND ----------

# MAGIC %sql
# MAGIC select month(orders.order_date), name, count(orders.order_id) from cust join orders on cust.customer_id = orders.customer_id where order_date <= date_add(current_date(), -12) group by orders.customer_id, name, month(orders.order_date) 

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from datetime import datetime

# Start Spark session
spark = SparkSession.builder.appName("ProductSales").getOrCreate()

# Define schema for products
products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True)
])

# Insert product data
products_data = [
    (1, 'Product A', 'Electronics'),
    (2, 'Product B', 'Clothing'),
    (3, 'Product C', 'Home Goods'),
    (4, 'Product D', 'Beauty')
]

# Create DataFrame
df_products = spark.createDataFrame(products_data, schema=products_schema)
df_products.show()


# COMMAND ----------

# Define schema for sales
sales_schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("sale_date", DateType(), True)
])

# Insert sales data
sales_data = [
    (1, 1, 100.00, datetime.strptime('2020-10-01', '%Y-%m-%d')),
    (2, 2, 50.00, datetime.strptime('2020-10-02', '%Y-%m-%d')),
    (3, 3, 75.00, datetime.strptime('2020-10-03', '%Y-%m-%d')),
    (4, 1, 150.00, datetime.strptime('2020-10-04', '%Y-%m-%d')),
    (5, 4, 25.00, datetime.strptime('2020-10-05', '%Y-%m-%d')),
    (6, 1, 200.00, datetime.strptime('2020-10-06', '%Y-%m-%d')),
    (7, 3, 100.00, datetime.strptime('2020-10-07', '%Y-%m-%d')),
    (8, 2, 75.00, datetime.strptime('2020-10-08', '%Y-%m-%d')),
    (9, 4, 50.00, datetime.strptime('2020-10-09', '%Y-%m-%d')),
    (10, 2, 125.00, datetime.strptime('2020-10-10', '%Y-%m-%d')),
    (11, 3, 150.00, datetime.strptime('2020-10-11', '%Y-%m-%d')),
    (12, 1, 75.00, datetime.strptime('2020-10-12', '%Y-%m-%d')),
    (13, 2, 100.00, datetime.strptime('2020-10-13', '%Y-%m-%d')),
    (14, 4, 200.00, datetime.strptime('2020-10-14', '%Y-%m-%d')),
    (15, 3, 50.00, datetime.strptime('2020-10-15', '%Y-%m-%d')),
    (16, 1, 125.00, datetime.strptime('2020-10-16', '%Y-%m-%d')),
    (17, 2, 150.00, datetime.strptime('2020-10-17', '%Y-%m-%d')),
    (18, 3, 75.00, datetime.strptime('2020-10-18', '%Y-%m-%d')),
    (19, 4, 100.00, datetime.strptime('2020-10-19', '%Y-%m-%d')),
    (20, 1, 50.00, datetime.strptime('2020-10-20', '%Y-%m-%d'))
]

# Create DataFrame
df_sales = spark.createDataFrame(sales_data, schema=sales_schema)
df_sales.show()


# COMMAND ----------

df_products.createOrReplaceTempView('prd')
df_sales.createOrReplaceTempView('sales')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   prd.product_name, 
# MAGIC   sales.product_id, 
# MAGIC   SUM(sales.amount) AS s
# MAGIC FROM sales 
# MAGIC JOIN prd ON sales.product_id = prd.product_id
# MAGIC WHERE sale_date >= date_add(current_date(),-7 )
# MAGIC GROUP BY sales.product_id, prd.product_name
# MAGIC ORDER BY s DESC
# MAGIC LIMIT 3;
# MAGIC
# MAGIC

# COMMAND ----------


