SELECT 
    customer_id,
    customer_name,
    COUNT(order_id) AS total_orders_placed
FROM 
    meta_orders
WHERE 
    order_status = 'DELIVERED' -- Filters out failed, cart-abandoned, or cancelled orders
GROUP BY 
    customer_id,
    customer_name
ORDER BY 
    total_orders_placed DESC;



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("MetaCustomerOrderFrequency") \
    .getOrCreate()

# 2. Create Sample Meta Order Ledger Data
order_data = [
    ("ORD_001", "CUST_10", "Sarah Connor", "DELIVERED"),
    ("ORD_002", "CUST_20", "John Doe",     "DELIVERED"),
    ("ORD_003", "CUST_10", "Sarah Connor", "DELIVERED"),
    ("ORD_004", "CUST_30", "Ellen Ripley", "CANCELLED"), # Should be excluded from metrics
    ("ORD_005", "CUST_20", "John Doe",     "DELIVERED"),
    ("ORD_006", "CUST_20", "John Doe",     "DELIVERED")
]

schema = ["order_id", "customer_id", "customer_name", "order_status"]
df_orders = spark.createDataFrame(order_data, schema=schema)

# 3. Filter for successful transactions and calculate order frequencies
df_order_counts = df_orders.filter(col("order_status") == "DELIVERED") \
    .groupBy("customer_id", "customer_name") \
    .agg(count("order_id").alias("total_orders_placed")) \
    .orderBy(col("total_orders_placed").desc())

# 4. Display user engagement breakdown
print("Meta Order Metric Rollup: Order Counts Per Customer")
df_order_counts.show()
