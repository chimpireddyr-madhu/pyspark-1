.#Find Employees Without Department (Uber)
SELECT e.*
FROM Employee e
LEFT JOIN Department d ON e.department_id = d.department_id
WHERE d.department_id IS NULL;

-----------------------

SELECT e.*
FROM Employee e
LEFT JOIN Department d ON e.department_id = d.department_id
WHERE d.department_id IS NULL;  

# pysparkTo execute this specific unmatched left join in PySpark, you can choose between two primary methods: the Native DataFrame API (which is highly recommended for speed) or Spark SQL (which mirrors your exact SQL query string).Method 1: PySpark DataFrame API (Highly Optimized)Using a left_anti join type is the most efficient big data pattern. It instructs Spark to only retain rows from the left table (Employee) that have no match in the right table (Department). This eliminates the need to allocate memory for right-side columns or run a slow post-join filter().pythonfrom pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("FindEmployeesWithoutDepartment") \
    .getOrCreate()

# 2. Mocking the 'Employee' DataFrame
employee_data = [
    (101, "Dev Lead 1", "DEP_01"),
    (102, "Dev 1",      "DEP_02"),
    (103, "Dev 5",      "DEP_01"),
    (104, "Dev 11",     None),      # Null department ID
    (105, "Intern 1",   "DEP_99")   # Invalid department ID (does not exist in Department table)
]
df_employee = spark.createDataFrame(employee_data, schema=["employee_id", "employee_name", "department_id"])

# 3. Mocking the 'Department' DataFrame
department_data = [
    ("DEP_01", "On-Prem Core Pod"),
    ("DEP_02", "AWS Cloud Pod")
]
df_department = spark.createDataFrame(department_data, schema=["department_id", "department_name"])

# 4. Execute Left Anti-Join (The most performant approach)
df_orphans = df_employee.join(
    df_department,
    on="department_id",
    how="left_anti"
)

# 5. Output the results
print("Orphaned Employees Detected:")
df_orphans.show()
Use code with caution.Method 2: PySpark SQL (Direct Translation)If your pipeline relies on passing direct SQL strings, register both DataFrames as temporary views. You can then execute your exact native SQL structure using the spark.sql() engine.python# Register DataFrames as local temporary cluster views
df_employee.createOrReplaceTempView("Employee")
df_department.createOrReplaceTempView("Department")

# Execute using your exact relational logic pattern
df_orphans_sql = spark.sql("""
    SELECT e.*
    FROM Employee e
    LEFT JOIN Department d ON e.department_id = d.department_id
    WHERE d.department_id IS NULL
""")

print("Orphaned Employees Detected via Spark SQL:")
df_orphans_sql.show()
Use code with caution.Distributed Cluster Output SummaryBoth computational paths will process your distributed partitions and isolate the exact matching data anomalies:textOrphaned Employees Detected:
+-------------+-----------+-------------+

|department_id|employee_id|employee_name|
+-------------+-----------+-------------+

|         null|        104|       Dev 11|
|       DEP_99|        105|     Intern 1|
+-------------+-----------+-------------+