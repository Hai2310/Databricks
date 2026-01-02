from pyspark.sql.window import Window

# Alert Fact Sales
daily_revenue = (
    spark.table("gold.fact_sales")
    .groupBy(to_date("order_purchase_timestamp").alias("order_date"))
    .agg(sum("payment_value").alias("daily_revenue"))
)

window_7d = Window.orderBy("order_date").rowsBetween(-7, -1)

revenue_alert = (
    daily_revenue
    .withColumn("avg_7d", avg("daily_revenue").over(window_7d))
    .withColumn(
        "pct_change",
        (col("daily_revenue") - col("avg_7d")) / col("avg_7d") * 100
    )
    .withColumn(
        "alert_flag",
        when(col("pct_change") > 30, lit("SPIKE"))
        .when(col("pct_change") < -30, lit("DROP"))
        .otherwise(lit("NORMAL"))
    )
)

revenue_alert.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("alert.fact_revenue_alerts")

# Alert FACT_ORDERS

orders_alert = (
    spark.table("silver.orders")
    .groupBy(to_date("order_purchase_timestamp").alias("date"))
    .agg(countDistinct("order_id").alias("order_cnt"))
    .withColumn("avg_7d", avg("order_cnt").over(Window.orderBy("date").rowsBetween(-7, -1)))
    .withColumn("pct_change", (col("order_cnt") - col("avg_7d")) / col("avg_7d") * 100)
    .withColumn(
        "alert_type",
        when(col("pct_change") > 25, "ORDER_SPIKE")
        .when(col("pct_change") < -25, "ORDER_DROP")
        .otherwise("NORMAL")
    )
)

orders_alert.write.format("delta").mode("overwrite").saveAsTable("alert.alert_orders")

# Alert FACT_PAYMENTS
payments_alert = (
    spark.table("silver.payments")
    .join(
        spark.table("silver.orders").select(
            "order_id",
            to_date("order_purchase_timestamp").alias("payment_date")
        ),
        "order_id"
    )
    .groupBy("payment_date")
    .agg(sum("payment_value").alias("total_payment"))
    .withColumn(
        "avg_7d",
        avg("total_payment").over(
            Window.orderBy("payment_date").rowsBetween(-7, -1)
        )
    )
    .withColumn(
        "pct_change",
        (col("total_payment") - col("avg_7d")) / col("avg_7d") * 100
    )
    .withColumn(
        "alert_type",
        when(col("pct_change") > 40, "PAYMENT_SPIKE")
        .when(col("pct_change") < -40, "PAYMENT_DROP")
        .otherwise("NORMAL")
    )
)

payments_alert.write.mode("overwrite").format("delta").saveAsTable("alert.alert_payments")
