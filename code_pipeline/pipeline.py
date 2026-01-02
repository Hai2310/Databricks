from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable


spark = SparkSession.builder.getOrCreate()

# 1. CONFIGURATION
BRONZE_PATH = "/Volumes/default/bronze/raw_data"
SILVER_PATH = "/Volumes/default/silver"
GOLD_PATH   = "/Volumes/default/gold"

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


# 2. BRONZE LAYER – INGEST RAW CSV
def load_bronze(table_name, file_name):
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{BRONZE_PATH}/{file_name}")
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"bronze.{table_name}")
    print(f"Loaded bronze.{table_name}")

load_bronze("customers", "olist_customers_dataset.csv")
load_bronze("orders", "olist_orders_dataset.csv")
load_bronze("order_items", "olist_order_items_dataset.csv")
load_bronze("payments", "olist_order_payments_dataset.csv")
load_bronze("reviews", "olist_order_reviews_dataset.csv")
load_bronze("products", "olist_products_dataset.csv")
load_bronze("sellers", "olist_sellers_dataset.csv")
load_bronze("geolocation", "olist_geolocation_dataset.csv")
load_bronze("category_translation", "product_category_name_translation.csv")

# 3. SILVER LAYER – CLEAN & STANDARDIZE

# ---------- Customers ----------
silver_customers = (
    spark.table("bronze.customers")
    .dropDuplicates(["customer_id"])
    .withColumn("customer_city", lower(col("customer_city")))
    .withColumn("ingest_ts", current_timestamp())
)

silver_customers.write.mode("overwrite").format("delta").saveAsTable("silver.customers")

# ---------- Orders ----------
silver_orders = (
    spark.table("bronze.orders")
    .withColumn("order_purchase_ts", to_timestamp("order_purchase_timestamp"))
    .withColumn("delivery_days",
        datediff(col("order_delivered_customer_date"),
                 col("order_purchase_timestamp")))
)

silver_orders.write.mode("overwrite").format("delta").saveAsTable("silver.orders")

# ---------- Products ----------
silver_products = (
    spark.table("bronze.products")
    .join(
        spark.table("bronze.category_translation"),
        "product_category_name",
        "left"
    )
)

silver_products.write.mode("overwrite").format("delta").saveAsTable("silver.products")

# ---------- Order Items ----------
spark.table("bronze.order_items") \
    .write.mode("overwrite").format("delta") \
    .saveAsTable("silver.order_items")

# ---------- Payments ----------
spark.table("bronze.payments") \
    .write.mode("overwrite").format("delta") \
    .saveAsTable("silver.payments")


# 4. GOLD – DIMENSION TABLE (SCD TYPE 2)


dim_customer_path = "gold.dim_customer"

src = (
    spark.table("silver.customers")
    .withColumn("start_date", current_date())
    .withColumn("end_date", lit(None).cast("date"))
    .withColumn("is_current", lit(True))
)

if spark.catalog.tableExists(dim_customer_path):
    tgt = DeltaTable.forName(spark, dim_customer_path)

    (tgt.alias("t")
     .merge(
         src.alias("s"),
         "t.customer_id = s.customer_id AND t.is_current = true"
     )
     .whenMatchedUpdate(
         condition="t.customer_city <> s.customer_city",
         set={
             "end_date": current_date(),
             "is_current": lit(False)
         }
     )
     .whenNotMatchedInsertAll()
     .execute())
else:
    src.write.format("delta").saveAsTable(dim_customer_path)
# 5. GOLD – FACT SALES (STREAMING STYLE)
fact_sales = (
    spark.table("silver.orders")
    .join(spark.table("silver.order_items"), "order_id")
    .join(spark.table("silver.payments"), "order_id")
    .select(
        "order_id",
        "customer_id",
        "product_id",
        "price",
        "freight_value",
        "payment_value",
        "order_purchase_timestamp"
    )
)

fact_sales.write.mode("overwrite").format("delta").saveAsTable("gold.fact_sales")


# GOLD – FACT DELIVERY PERFORMANCE
fact_delivery = (
    spark.table("silver.orders")
    .select(
        "order_id",
        "customer_id",
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    )
    .withColumn(
        "delivery_days",
        datediff(
            col("order_delivered_customer_date"),
            col("order_purchase_timestamp")
        )
    )
    .withColumn(
        "is_late",
        when(
            col("order_delivered_customer_date") > col("order_estimated_delivery_date"),
            lit(1)
        ).otherwise(lit(0))
    )
)

fact_delivery.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold.fact_delivery")


# GOLD – FACT PAYMENT METHOD
fact_payment_method = (
    spark.table("silver.payments")
    .groupBy("payment_type")
    .agg(
        count("*").alias("num_transactions"),
        sum("payment_value").alias("total_payment_value"),
        avg("payment_value").alias("avg_payment_value")
    )
)

fact_payment_method.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold.fact_payment_method")

# 6. BUSINESS VIEW

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_sales_kpi AS
SELECT
    date(order_purchase_timestamp) AS order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(payment_value) AS revenue,
    ROUND(AVG(payment_value), 2) AS avg_order_value
FROM gold.fact_sales
GROUP BY date(order_purchase_timestamp)
""")

CREATE OR REPLACE VIEW gold.vw_delivery_kpi AS
SELECT
    date(order_purchase_timestamp) AS order_date,
    COUNT(*) AS total_orders,
    SUM(is_late) AS late_orders,
    ROUND(SUM(is_late)/COUNT(*) * 100, 2) AS late_rate_pct,
    AVG(delivery_days) AS avg_delivery_days
FROM gold.fact_delivery
GROUP BY date(order_purchase_timestamp);

print("FULL PIPELINE EXECUTED SUCCESSFULLY")