# Databricks notebook source
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("EmployeeDFs").getOrCreate()

# Data for Employees table
employees_data = [
    (1, "Alice"),
    (7, "Bob"),
    (11, "Meir"),
    (90, "Winston"),
    (3, "Jonathan")
]

# Data for EmployeeUNI table
employee_uni_data = [
    (3, 1),
    (11, 2),
    (90, 3)
]

# Creating DataFrames
df_employees = spark.createDataFrame(employees_data, ["id", "name"])
df_employee_uni = spark.createDataFrame(employee_uni_data, ["id", "unique_id"])

# Register as temporary views
df_employees.createOrReplaceTempView("Employees")
df_employee_uni.createOrReplaceTempView("EmployeeUNI")


# COMMAND ----------

df_employees.join(df_employee_uni, on='id', how = 'left').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Employees left join  EmployeeUNI  on EmployeeUNI.id = Employees.id

# COMMAND ----------

sales_data = [
    # Sample data
    (1, 101, 2022, 2, 300),
    (2, 102, 2023, 5, 450),
    (3, 101, 2023, 1, 150),
    (4, 103, 2022, 3, 200)
]

df_sales = spark.createDataFrame(sales_data, ["sale_id", "product_id", "year", "quantity", "price"])
df_sales.createOrReplaceTempView("Sales")

product_data = [
    # Sample data
    (101, "Keyboard"),
    (102, "Mouse"),
    (103, "Monitor")
]

df_product = spark.createDataFrame(product_data, ["product_id", "product_name"])
df_product.createOrReplaceTempView("Product")


# COMMAND ----------

# MAGIC %sql
# MAGIC select product_name, year, price from Sales join Product on Sales.product_id = Product.product_id

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("VisitsTransactions").getOrCreate()

# Visits table data
visits_data = [
    (1, 23),
    (2, 9),
    (4, 30),
    (5, 54),
    (6, 96),
    (7, 54),
    (8, 54)
]

# Transactions table data
transactions_data = [
    (2, 5, 310),
    (3, 5, 300),
    (9, 5, 200),
    (12, 1, 910),
    (13, 2, 970)
]

# Creating DataFrames
df_visits = spark.createDataFrame(visits_data, ["visit_id", "customer_id"])
df_transactions = spark.createDataFrame(transactions_data, ["transaction_id", "visit_id", "amount"])

# Register as temporary views
df_visits.createOrReplaceTempView("Visits")
df_transactions.createOrReplaceTempView("Transactions")


# COMMAND ----------

# MAGIC %sql
# MAGIC select Visits.customer_id , count(Visits.visit_id) from Visits left join  Transactions on Visits.visit_id = Transactions.visit_id where amount is null group by Visits.customer_id

# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import date

# Initialize Spark session
spark = SparkSession.builder.appName("WeatherDF").getOrCreate()

# Data from the image
weather_data = [
    (1, date(2015, 1, 1), 10),
    (2, date(2015, 1, 2), 25),
    (3, date(2015, 1, 3), 20),
    (4, date(2015, 1, 4), 30)
]

# Define schema
columns = ["id", "recordDate", "temperature"]

# Create DataFrame
df_weather = spark.createDataFrame(weather_data, columns)

# Register as temporary view
df_weather.createOrReplaceTempView("Weather")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Weather

# COMMAND ----------

# MAGIC %sql
# MAGIC  with cte as (select id, (temperature - lag(temperature) over (order by recordDate)) as lag from  Weather)
# MAGIC  select id from cte where lag > 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select W1.id from Weather w1 join Weather w2 on DateDiff(w1.recordDate,w2.recordDate) = 1 where (w1.temperature - w2.temperature) > 0

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("MachineProcessActivity").getOrCreate()

# Data from the image
activity_data = [
    (0, 0, "start", 0.712),
    (0, 0, "end",   1.520),
    (0, 1, "start", 3.140),
    (0, 1, "end",   4.120),
    (1, 0, "start", 0.550),
    (1, 0, "end",   1.550),
    (1, 1, "start", 0.430),
    (1, 1, "end",   1.420),
    (2, 0, "start", 4.100),
    (2, 0, "end",   4.512),
    (2, 1, "start", 2.500),
    (2, 1, "end",   5.000)
]

# Define schema
columns = ["machine_id", "process_id", "activity_type", "timestamp"]

# Create DataFrame
df_activity = spark.createDataFrame(activity_data, columns)

# Register as temporary view
df_activity.createOrReplaceTempView("MachineActivity")


# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (select a.machine_id, a.process_id, (b.timestamp - a.timestamp) as p from MachineActivity a join MachineActivity b on 
# MAGIC a.machine_id = b.machine_id AND 
# MAGIC a.process_id = b.process_id and
# MAGIC a.activity_type = 'start' and
# MAGIC b.activity_type = 'end')
# MAGIC
# MAGIC select machine_id, avg(p) from cte group by machine_id

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.appName("EmployeeBonusDF").getOrCreate()

# Sample data for Employee table
employee_data = [
    # (empId, name, supervisor, salary)
    (1, "Alice", 3, 60000),
    (2, "Bob", 3, 50000),
    (3, "Carol", None, 100000),
    (4, "David", 3, 40000)
]

employee_columns = ["empId", "name", "supervisor", "salary"]

df_employee = spark.createDataFrame(employee_data, employee_columns)
df_employee.createOrReplaceTempView("Employee")

# Sample data for Bonus table
bonus_data = [
    # (empId, bonus)
    (1, 5000),
    (2, 3000)
]

bonus_columns = ["empId", "bonus"]

df_bonus = spark.createDataFrame(bonus_data, bonus_columns)
df_bonus.createOrReplaceTempView("Bonus")


# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("EmployeeBonusDF").getOrCreate()

# Employee table data
employee_data = [
    (3, "Brad", None, 4000),
    (1, "John", 3, 1000),
    (2, "Dan", 3, 2000),
    (4, "Thomas", 3, 4000)
]

employee_columns = ["empId", "name", "supervisor", "salary"]

df_employee = spark.createDataFrame(employee_data, employee_columns)
df_employee.createOrReplaceTempView("Employee")

# Bonus table data
bonus_data = [
    (2, 500),
    (4, 2000)
]

bonus_columns = ["empId", "bonus"]

df_bonus = spark.createDataFrame(bonus_data, bonus_columns)
df_bonus.createOrReplaceTempView("Bonus")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Employee left join Bonus on Employee.empId = Bonus.empId where bonus < 1000

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("StudentSubjects").getOrCreate()

# Students table
students_data = [
    (1, "Alice"),
    (2, "Bob"),
    (13, "John"),
    (6, "Alex")
]

students_columns = ["student_id", "student_name"]
df_students = spark.createDataFrame(students_data, students_columns)
df_students.createOrReplaceTempView("Students")

# Subjects table
subjects_data = [
    ("Math",),
    ("Physics",),
    ("Programming",)
]

subjects_columns = ["subject_name"]
df_subjects = spark.createDataFrame(subjects_data, subjects_columns)
df_subjects.createOrReplaceTempView("Subjects")

# Enrollment or student-subject table (from second image)
enrollment_data = [
    (1, "Math"),
    (1, "Physics"),
    (1, "Programming"),
    (2, "Programming"),
    (1, "Physics"),
    (1, "Math"),
    (13, "Math"),
    (13, "Programming"),
    (13, "Physics"),
    (2, "Math"),
    (1, "Math")
]

enrollment_columns = ["student_id", "subject_name"]
df_enrollments = spark.createDataFrame(enrollment_data, enrollment_columns)
df_enrollments.createOrReplaceTempView("Enrollments")


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create Spark session
spark = SparkSession.builder.appName("EmployeeManager").getOrCreate()

# Define schema
schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("employee_name", StringType(), True),
    StructField("manager_id", IntegerType(), True)
])

# Data
data = [
    (1, "Alice", 3),
    (2, "Bob", 3),
    (3, "Charlie", 4),
    (4, "David", 5),
    (5, "Eve", 5),
    (6, "Frank", 5),
    (7, "Grace", 5),
    (8, "Hannah", 5),
    (9, "Ivan", 5),
    (10, "Jack", 5),
    (11, "Karen", 4),
    (12, "Leo", 4),
    (13, "Mike", 5),
    (14, "Nina", 5),
    (15, "Olivia", 5)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)
df.createOrReplaceTempView('Emp')

# Show the DataFrame
df.show()


# COMMAND ----------

# MAGIC %sql
# MAGIC select m.employee_name, count(m.employee_id) from Emp e join emp m on e.manager_id = m.employee_id group by  m.employee_id, m.employee_name

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

# Start SparkSession
spark = SparkSession.builder.appName("LoginConfirmations").getOrCreate()

# Logins Table
logins_data = [
    (3, "2020-03-21 10:16:13"),
    (2, "2021-01-04 13:57:59"),
    (7, "2020-07-29 23:09:44"),
    (6, "2020-12-09 10:39:37")
]

logins_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("time_stamp", StringType(), True)  # you can use TimestampType if parsing needed
])

df_logins = spark.createDataFrame(logins_data, schema=logins_schema)
df_logins.createOrReplaceTempView("logins")

# Confirmations Table
confirmations_data = [
    (3, "2021-01-05 03:30:46", "timeout"),
    (7, "2021-07-14 14:00:00", "timeout"),
    (7, "2021-06-12 11:57:29", "confirmed"),
    (7, "2021-06-13 12:58:20", "confirmed"),
    (7, "2021-06-14 13:59:27", "confirmed"),
    (7, "2021-01-02 01:00:00", "confirmed"),
    (1, "2021-02-01 23:59:59", "confirmed"),
    (2, "2021-02-28 23:59:59", "timeout")
]

confirmations_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("time_stamp", StringType(), True),
    StructField("action", StringType(), True)
])

df_confirmations = spark.createDataFrame(confirmations_data, schema=confirmations_schema)
df_confirmations.createOrReplaceTempView("confirmations")


# COMMAND ----------



# COMMAND ----------


