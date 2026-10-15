# Databricks notebook source
from pyspark.sql import functions as F, Window

catalog = "workspace"
b = lambda t: spark.table(f"{catalog}.bronze.{t}")

# ---------- helpers ----------
def dedupe(df, keys):
    """Keep the latest ingested row per key."""
    w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

def upsert(df, table, keys):
    """Create the table on the first run, MERGE on later runs."""
    full = f"{catalog}.silver.{table}"
    if not spark.catalog.tableExists(full):
        df.write.format("delta").saveAsTable(full)
    else:
        df.createOrReplaceTempView("src")
        cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        spark.sql(f"""MERGE INTO {full} t USING src s ON {cond}
                      WHEN MATCHED THEN UPDATE SET *
                      WHEN NOT MATCHED THEN INSERT *""")
    print(f"silver.{table}: {spark.table(full).count():,} rows")

# ---------- customers ----------
customers = (b("customers")
    .filter("customer_id IS NOT NULL")
    .withColumn("customer_city", F.initcap(F.trim("customer_city")))
    .withColumn("customer_state", F.upper(F.trim("customer_state")))
    .select("customer_id", "customer_unique_id", "customer_zip_code_prefix",
            "customer_city", "customer_state", "_ingested_at"))
upsert(dedupe(customers, ["customer_id"]), "customers", ["customer_id"])

# ---------- orders ----------
ts = lambda c: F.to_timestamp(c, "yyyy-MM-dd HH:mm:ss")
orders = (b("orders")
    .filter("order_id IS NOT NULL AND customer_id IS NOT NULL")
    .withColumn("order_status", F.lower(F.trim("order_status")))
    .withColumn("order_purchase_timestamp", ts("order_purchase_timestamp"))
    .withColumn("order_approved_at", ts("order_approved_at"))
    .withColumn("order_delivered_carrier_date", ts("order_delivered_carrier_date"))
    .withColumn("order_delivered_customer_date", ts("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_date", ts("order_estimated_delivery_date"))
    .withColumn("delivery_days",
        F.datediff("order_delivered_customer_date", "order_purchase_timestamp"))
    .withColumn("is_late",
        F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"))
    .drop("_source_file"))
upsert(dedupe(orders, ["order_id"]), "orders", ["order_id"])

# ---------- order_items ----------
items = (b("order_items")
    .filter("order_id IS NOT NULL AND product_id IS NOT NULL")
    .withColumn("order_item_id", F.col("order_item_id").cast("int"))
    .withColumn("shipping_limit_date", ts("shipping_limit_date"))
    .withColumn("price", F.col("price").cast("double"))
    .withColumn("freight_value", F.col("freight_value").cast("double"))
    .filter("price >= 0 AND freight_value >= 0")
    .drop("_source_file"))
upsert(dedupe(items, ["order_id", "order_item_id"]),
       "order_items", ["order_id", "order_item_id"])

# ---------- order_payments ----------
pay = (b("order_payments")
    .filter("order_id IS NOT NULL")
    .withColumn("payment_sequential", F.col("payment_sequential").cast("int"))
    .withColumn("payment_installments", F.col("payment_installments").cast("int"))
    .withColumn("payment_value", F.col("payment_value").cast("double"))
    .filter("payment_value >= 0")
    .drop("_source_file"))
upsert(dedupe(pay, ["order_id", "payment_sequential"]),
       "order_payments", ["order_id", "payment_sequential"])

# ---------- order_reviews ----------
rev = (b("order_reviews")
    .filter("review_id IS NOT NULL AND order_id IS NOT NULL")
    .withColumn("review_score", F.col("review_score").cast("int"))
    .filter("review_score BETWEEN 1 AND 5")
    .withColumn("review_creation_date", ts("review_creation_date"))
    .withColumn("review_answer_timestamp", ts("review_answer_timestamp"))
    .drop("_source_file"))
upsert(dedupe(rev, ["review_id", "order_id"]), "order_reviews", ["review_id", "order_id"])

# ---------- products (+ English category names) ----------
tr = b("category_translation").select("product_category_name", "product_category_name_english")
products = (b("products")
    .filter("product_id IS NOT NULL")
    .withColumnRenamed("product_name_lenght", "product_name_length")
    .withColumnRenamed("product_description_lenght", "product_description_length")
    .join(tr, "product_category_name", "left")
    .withColumn("category", F.coalesce("product_category_name_english",
                                       "product_category_name", F.lit("unknown")))
    .withColumn("product_weight_g", F.col("product_weight_g").cast("double"))
    .withColumn("product_photos_qty", F.col("product_photos_qty").cast("int"))
    .select("product_id", "category", "product_weight_g", "product_photos_qty",
            "_ingested_at"))
upsert(dedupe(products, ["product_id"]), "products", ["product_id"])

# ---------- sellers ----------
sellers = (b("sellers")
    .filter("seller_id IS NOT NULL")
    .withColumn("seller_city", F.initcap(F.trim("seller_city")))
    .withColumn("seller_state", F.upper(F.trim("seller_state")))
    .select("seller_id", "seller_zip_code_prefix", "seller_city",
            "seller_state", "_ingested_at"))
upsert(dedupe(sellers, ["seller_id"]), "sellers", ["seller_id"])