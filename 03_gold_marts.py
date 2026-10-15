# Databricks notebook source
from pyspark.sql import functions as F, Window

catalog = "workspace"
s = lambda t: spark.table(f"{catalog}.silver.{t}")

def save(df, name):
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{catalog}.gold.{name}"))
    print(f"gold.{name}: {spark.table(f'{catalog}.gold.{name}').count():,} rows")

# ---------- shared base: one row per valid order ----------
valid_orders = s("orders").filter(~F.col("order_status").isin("canceled", "unavailable"))

order_totals = (s("order_items").groupBy("order_id").agg(
    F.sum("price").alias("items_value"),
    F.sum("freight_value").alias("freight_value"),
    F.count("*").alias("items")))

orders_fact = (valid_orders.join(order_totals, "order_id", "inner")
    .withColumn("order_date", F.to_date("order_purchase_timestamp")))

# ---------- 1. daily_sales ----------
daily_sales = (orders_fact.groupBy("order_date").agg(
    F.count("*").alias("orders"),
    F.sum("items").alias("items_sold"),
    F.round(F.sum("items_value"), 2).alias("revenue"),
    F.round(F.sum("freight_value"), 2).alias("freight"),
    F.round(F.avg("items_value"), 2).alias("avg_order_value"))
    .orderBy("order_date"))
save(daily_sales, "daily_sales")

# ---------- 2. top_products ----------
products_agg = (s("order_items")
    .join(valid_orders.select("order_id"), "order_id", "inner")
    .join(s("products").select("product_id", "category"), "product_id", "left")
    .groupBy("product_id", "category").agg(
        F.count("*").alias("units_sold"),
        F.countDistinct("order_id").alias("orders"),
        F.round(F.sum("price"), 2).alias("revenue")))

w = Window.orderBy(F.col("revenue").desc())
w_cat = Window.partitionBy("category").orderBy(F.col("revenue").desc())
top_products = (products_agg
    .withColumn("rank_overall", F.row_number().over(w))
    .withColumn("rank_in_category", F.row_number().over(w_cat)))
save(top_products, "top_products")

# ---------- 3. customer_lifetime_value ----------
# customer_id is per order in Olist; customer_unique_id identifies the person
clv = (orders_fact
    .join(s("customers").select("customer_id", "customer_unique_id", "customer_state"),
          "customer_id", "inner")
    .groupBy("customer_unique_id").agg(
        F.max("customer_state").alias("customer_state"),
        F.count("*").alias("total_orders"),
        F.round(F.sum("items_value"), 2).alias("total_spent"),
        F.round(F.avg("items_value"), 2).alias("avg_order_value"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"))
    .withColumn("customer_type",
        F.when(F.col("total_orders") > 1, "repeat").otherwise("one-time")))
save(clv, "customer_lifetime_value")

# ---------- 4. delivery_performance ----------
review_avg = (s("order_reviews").groupBy("order_id")
    .agg(F.avg("review_score").alias("review_score")))

delivery = (s("orders")
    .filter("order_status = 'delivered' AND delivery_days IS NOT NULL")
    .join(s("customers").select("customer_id", "customer_state"), "customer_id", "inner")
    .join(review_avg, "order_id", "left")
    .withColumn("month", F.date_trunc("month", "order_purchase_timestamp").cast("date"))
    .groupBy("customer_state", "month").agg(
        F.count("*").alias("delivered_orders"),
        F.round(F.avg("delivery_days"), 1).alias("avg_delivery_days"),
        F.sum(F.col("is_late").cast("int")).alias("late_orders"),
        F.round(F.avg("review_score"), 2).alias("avg_review_score"))
    .withColumn("late_pct", F.round(100 * F.col("late_orders") / F.col("delivered_orders"), 1)))
save(delivery, "delivery_performance")

# COMMAND ----------

display(spark.sql(f"""
  SELECT category, ROUND(SUM(revenue),0) AS revenue, SUM(units_sold) AS units
  FROM {catalog}.gold.top_products GROUP BY category ORDER BY revenue DESC LIMIT 10"""))

# COMMAND ----------

display(spark.sql(f"""
  SELECT customer_type, COUNT(*) AS customers, ROUND(SUM(total_spent),0) AS revenue
  FROM {catalog}.gold.customer_lifetime_value GROUP BY customer_type"""))

# COMMAND ----------

display(spark.sql(f"""
  SELECT ROUND(AVG(late_pct),1) AS avg_late_pct, ROUND(AVG(avg_review_score),2) AS avg_review
  FROM {catalog}.gold.delivery_performance"""))