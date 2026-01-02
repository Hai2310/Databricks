import dlt
from pyspark.sql.functions import *

# BRONZE STREAMING

@dlt.table(
    name="bronze_orders",
    comment="Raw orders data from CSV",
    table_properties={"quality": "bronze"}
)
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/default/bronze/raw_data/olist_orders_dataset.csv")
    )

@dlt.table(
    name="bronze_order_items",
    comment="Raw order items",
    table_properties={"quality": "bronze"}
)
def bronze_order_items():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("/Volumes/default/bronze/raw_data/olist_order_items_dataset.csv")
    )

@dlt.table(
    name="bronze_payments",
    comment="Raw payment data",
    table_properties={"quality": "bronze"}
)
def bronze_payments():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("/Volumes/default/bronze/raw_data/olist_order_payments_dataset.csv")
    )

# SILVER – CLEAN + QUALITY CHECK
@dlt.table(
    name="silver_orders",
    comment="Cleaned orders",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_timestamp", "order_purchase_timestamp IS NOT NULL")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
        .withColumn("order_purchase_ts", to_timestamp("order_purchase_timestamp"))
    )

@dlt.table(
    name="silver_order_items",
    comment="Clean order items",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_fail("price_positive", "price >= 0")
def silver_order_items():
    return dlt.read_stream("bronze_order_items")

# GOLD – FACT SALES
@dlt.table(
    name="fact_sales",
    comment="Fact sales table",
    table_properties={"quality": "gold"}
)
def fact_sales():
    orders = dlt.read_stream("silver_orders")
    items = dlt.read_stream("silver_order_items")
    payments = dlt.read_stream("bronze_payments")

    return (
        orders
        .join(items, "order_id")
        .join(payments, "order_id")
        .select(
            "order_id",
            "customer_id",
            "product_id",
            "price",
            "payment_value",
            "order_purchase_timestamp"
        )
    )
# DIMENSION – CUSTOMER (SCD TYPE 2)
@dlt.table(
    name="dim_customer",
    comment="Customer dimension SCD Type 2",
    table_properties={"quality": "gold"}
)
def dim_customer():
    df = dlt.read("silver_customers")

    return (
        df.withColumn("start_date", current_date())
          .withColumn("end_date", lit(None).cast("date"))
          .withColumn("is_current", lit(True))
    )



