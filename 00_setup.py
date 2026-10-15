# Databricks notebook source
print(spark.sql("SELECT current_catalog()").first()[0])

# COMMAND ----------

catalog = "workspace"   # change if yours is different

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.raw")

# COMMAND ----------

# MAGIC %pip install kagglehub
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import kagglehub, shutil, os

path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
dest = "/Volumes/workspace/bronze/raw/olist"
os.makedirs(dest, exist_ok=True)

for f in os.listdir(path):
    shutil.copy(os.path.join(path, f), dest)

display(dbutils.fs.ls(dest))

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/bronze/raw/olist"))

# COMMAND ----------

from pyspark.sql import functions as F

catalog = "workspace"
raw_path = f"/Volumes/{catalog}/bronze/raw/olist"
chk_path = f"/Volumes/{catalog}/bronze/raw/_checkpoints"

# file name -> bronze table name
sources = {
    "olist_customers_dataset.csv":        "customers",
    "olist_geolocation_dataset.csv":      "geolocation",
    "olist_order_items_dataset.csv":      "order_items",
    "olist_order_payments_dataset.csv":   "order_payments",
    "olist_order_reviews_dataset.csv":    "order_reviews",
    "olist_orders_dataset.csv":           "orders",
    "olist_products_dataset.csv":         "products",
    "olist_sellers_dataset.csv":          "sellers",
    "product_category_name_translation.csv": "category_translation",
}

def ingest(file_name, table):
    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{chk_path}/{table}/schema")
        .option("pathGlobFilter", file_name)
        .option("header", True)
        .option("multiLine", True)          # reviews contain line breaks
        .option("escape", '"')
        .load(raw_path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path")))

    (df.writeStream
        .option("checkpointLocation", f"{chk_path}/{table}/checkpoint")
        .option("mergeSchema", True)
        .trigger(availableNow=True)
        .toTable(f"{catalog}.bronze.{table}")
        .awaitTermination())
    print(f"loaded {table}")

for f, t in sources.items():
    ingest(f, t)