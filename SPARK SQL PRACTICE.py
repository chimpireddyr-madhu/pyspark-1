order_id customer_name  country    category  product    quantity   unit_price    order_date   status
1         Alice           USA     Electronics Laptop       2         1200.0       2024-01-15    Delivered
2          bob             india     clothing  shirt       5        	80.0         2024-02-10    cancelled
3         carol           UK     Electronics  phone       1         800.0         2024-03-05    Delivered


==================================1st Question==========================================================
[1]Find the total number of orders placed from each country. Sort the result by order count in descending order.

Expected columns in output: country, order_count

df2 = df.groupby(df.country).agg(sqlfunc.sum(df.order_id).alias("order_count"))
df2.sort(col("order_count").desc()).show()


--------------------------------2nd Question----------------------------------------------------------------

[2]Find the total revenue generated per category. Revenue = quantity × unit_price. Only show categories 
where total revenue is greater than 10,000. Sort by total revenue descending

df2 = df.groupBy(df.category) \
        .agg(sqlfunc.sum(df.quantity * df.unit_price).alias("total_revenue")) \
        .filter(col("total_revenue") > 10000)

df2.sort(col("total_revenue").desc()).show()

------------------------------3rd Question-----------------------------------------------------------------
Find the top 3 customers (by total amount spent) from each country. Amount spent = quantity × unit_price.
Only consider orders with status = 'Delivered'.

Expected columns in output: country, customer_name, total_spent, rank

from pyspark.sql.window import Window
import pyspark.sql.functions as sqlfunc

# Step 1 — Filter delivered orders
df_delivered = df.filter(df.status == "Delivered")

# Step 2 — Aggregate total spent per customer per country
df_agg = df_delivered.groupBy("country", "customer_name") \
                     .agg(sqlfunc.sum(df.quantity * df.unit_price).alias("total_spent"))

# Step 3 — Define window and add rank
windowSpec = Window.partitionBy("country").orderBy(col("total_spent").desc())
df_ranked = df_agg.withColumn("rank", sqlfunc.dense_rank().over(windowSpec))

# Step 4 — Filter top 3
df_ranked.filter(col("rank") <= 3).show()
