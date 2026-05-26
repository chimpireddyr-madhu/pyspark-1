# Get the Top 3 Highest-Paid Employees (Google)
1. SQL ImplementationMethod A: Standard Filtering (Best if there are no salary ties)sqlSELECT 
    employee_id, 
    employee_name, 
    department, 
    salary
FROM 
    google_employees
ORDER BY 
    salary DESC
LIMIT 3;
Use code with caution.Method B: Window Function (Best Practice: Handles matching/tied salaries)If two employees tie for the 3rd highest salary, this method handles it gracefully without dropping records.sqlWITH RankedEmployees AS (
    SELECT 
        employee_id, 
        employee_name, 
        department, 
        salary,
        DENSE_RANK() OVER (ORDER BY salary DESC) as salary_rank
    FROM 
        google_employees
)
SELECT 
    employee_id, 
    employee_name, 
    department, 
    salary
FROM 
    RankedEmployees
WHERE 
    salary_rank <= 3;
Use code with caution.2. PySpark ImplementationUsing the PySpark Window functions provides the highest performance optimization when running across distributed cloud clusters.Production-Ready PySpark Scriptpythonfrom pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, dense_rank

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("Top3HighestPaidEmployees") \
    .getOrCreate()

# 2. Create Sample Employee Dataset
employee_data = [
    (101, "Alex", "Cloud Infra", 250000),
    (102, "Blake", "Google Search", 310000),
    (103, "Charlie", "DeepMind", 280000),
    (104, "Dana", "Android OS", 190000),
    (105, "Evan", "Google Search", 310000), # Tied for highest salary
    (106, "Fiona", "YouTube Engine", 240000)
]
schema = ["employee_id", "employee_name", "department", "salary"]
df_employees = spark.createDataFrame(employee_data, schema=schema)

# 3. Define Window specification ordered by salary descending
window_spec = Window.orderBy(col("salary").desc())

# 4. Apply Dense Rank and filter for top 3 ranks
df_top_paid = df_employees.withColumn("rank", dense_rank().over(window_spec)) \
    .filter(col("rank") <= 3) \
    .orderBy(col("salary").desc())

# 5. Show results
print("Google Top 3 Highest-Paid Compensation Rollout:")
df_top_paid.select("employee_id", "employee_name", "department", "salary").show()
Use code with caution.Execution OutputtextGoogle Top 3 Highest-Paid Compensation Rollout:
+-----------+-------------+-------------+------+


|employee_id|employee_name|   department|salary|
+-----------+-------------+-------------+------+


|        102|        Blake|Google Search|310000|
|        105|         Evan|Google Search|310000|
|        103|      Charlie|     DeepMind|280000|
|        101|         Alex|  Cloud Infra|250000|
+-----------+-------------+-------------+------+
Use code with caution.Note: 4 rows are returned here because Blake and Evan have identical top-tier salaries, illustrating why dense_rank() protects data integrity.Cloud Integration TipWhen running this pipeline on Google Cloud Platform (GCP) Dataproc, read directly from Cloud Storage (GCS) buckets into your DataFrame using the gs:// URI scheme instead of local arrays.
