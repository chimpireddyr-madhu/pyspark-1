df1.join (broadcast(df2),"id")
df.write.particianby ("state").parquet("/particain")

df_csv = spark.read.option("header",true).csv ("/path/data.csv

df_json =

df_json = spark.read.json("/path/data.json")

#Read_parque
df_parquet = spark.read.parquet("/path/data.parquet")

#write parquet
df_json.write.mode("/overwrite"),.parquet("/output/parquet")


df_json.write.option("header",true).("/output/csv")


park  pyspark.sql.types import structurtype,structfeild,stringtype,Intigertype
schema = structfied("id",intigerType(),true),

df =spark.read.schema(schema).csv ("/path/data.csv")

df.filter(df["age"]>25).show()
df.filter(df["age"] <25 ).show ()



df = df.withColumnRenamed ("dob","date_of_birth")
df = df.withColumnRenamed ("dob","date_of_birth")
()*
df.groupby ("dept").age(count(*).alies (total),sum ("salary").alies("total")
avg ("salary").alieas("avd_salary")

df1.join()df2,on="id",how="id"inner")

df1.join()
df =  

-----------------------




python# 1. Set the directory path where checkpoint data will be stored
spark.sparkContext.setCheckpointDir("hdfs:///tmp/spark_checkpoint_dir/")

# 2. Perform operations on your DataFrame
df = spark.read.csv("data.csv", header=True)
processed_df = df.filter(df["age"] > 21).withColumn("status", lit("active"))

# 3. Checkpoint the DataFrame to break the lineage and save state
checkpointed_df = processed_df.checkpoint(eager=True)


------------------------

PySpark Cheat SheetThis quick reference covers the most common PySpark operations for DataFrames, columns, and data manipulation.Initialize Sessionpythonfrom pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("QuickStart") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
Use code with caution.Read & Write Datapython# Read
df = spark.read.csv("path.csv", header=True, inferSchema=True)
df = spark.read.json("path.json")
df = spark.read.parquet("path.parquet")

# Write
df.write.mode("overwrite").csv("path.csv")
df.write.mode("append").parquet("path.parquet")
Use code with caution.Inspect Datapythondf.show(5)          # View top 5 rows
df.printSchema()    # View schema structure
df.columns          # List column names
df.count()          # Count total rows
Use code with caution.Column Operationspythonfrom pyspark.sql.functions import col, lit, when

# Select and Rename
df.select("col1", "col2")
df.withColumnRenamed("old_name", "new_name")

# Add or Modify Column
df.withColumn("new_col", col("col1") * 10)
df.withColumn("constant_col", lit(5))

# Conditional Logic (If/Else)
df.withColumn("type", when(col("age") >= 18, "Adult").otherwise("Minor"))

# Drop Column
df.drop("col1", "col2")
Use code with caution.Filter & Sortpython# Filter
df.filter(col("age") > 21)
df.filter((col("age") > 21) & (col("status") == "Active"))

# Sort
df.orderBy(col("age").desc())
df.sort("status", ascending=False)
Use code with caution.Grouping & Aggregationpythonfrom pyspark.sql.functions import sum, avg, max, count

df.groupBy("department").agg(
    sum("salary").alias("total_salary"),
    avg("age").alias("avg_age"),
    count("id").alias("employee_count")
)
Use code with caution.Joinspython# Inner, left, right, outer, semi, anti
df1.join(df2, df1["id"] == df2["id"], "inner")
df1.join(df2, "id", "left") # If column names match exactly
Use code with caution.Performance & Memorypythondf.cache()                      # Cache in memory
df.persist()                    # Persist with custom storage level
df.unpersist()                  # Remove from memory/disk
df.checkpoint()                 # Break lineage and save to storage
df.coalesce(1)                  # Decrease partitions without shuffle
df.repartition(10, "col1")      # Increase/decrease partitions with shuffle
Use code with caution.If you want to tailor this cheat sheet, let me know:Do you want to add Advanced Window Functions or User Defined Functions (UDFs)?Are you focusing on a specific file format like Delta Lake or Iceberg?Should we include syntax for PySpark SQL queries (spark.sql(...))?