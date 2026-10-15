# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime

catalog = "workspace"
tbl = lambda layer, name: f"{catalog}.{layer}.{name}"
results = []

def check(name, table, passed, detail="", critical=True):
    results.append((datetime.now(), name, table, bool(passed), str(detail), critical))
    print("PASS" if passed else "FAIL", "-", name, "|", table, "|", detail)

# ---------- 1. tables are not empty ----------
for layer, names in {
    "silver": ["customers", "orders", "order_items", "order_payments",
               "order_reviews", "products", "sellers"],
    "gold": ["daily_sales", "top_products", "customer_lifetime_value",
             "delivery_performance"],
}.items():
    for n in names:
        cnt = spark.table(tbl(layer, n)).count()
        check("row_count > 0", f"{layer}.{n}", cnt > 0, f"{cnt:,} rows")

# ---------- 2. no null keys ----------
not_null = {
    "customers": ["customer_id"],
    "orders": ["order_id", "customer_id"],
    "order_items": ["order_id", "order_item_id", "product_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id", "order_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
}
for n, cols in not_null.items():
    bad = spark.table(tbl("silver", n)).filter(" OR ".join(f"{c} IS NULL" for c in cols)).count()
    check("no null keys", f"silver.{n}", bad == 0, f"{bad} bad rows")

# ---------- 3. no duplicate primary keys ----------
primary_keys = {
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id", "order_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
}
for n, cols in primary_keys.items():
    dups = spark.table(tbl("silver", n)).groupBy(*cols).count().filter("count > 1").count()
    check("unique primary key", f"silver.{n}", dups == 0, f"{dups} duplicated keys")

# ---------- 4. referential integrity ----------
def orphans(child, child_col, parent, parent_col):
    c, p = spark.table(tbl("silver", child)), spark.table(tbl("silver", parent))
    return c.join(p, c[child_col] == p[parent_col], "left_anti").count()

for child, ccol, parent, pcol, crit in [
    ("orders", "customer_id", "customers", "customer_id", True),
    ("order_items", "order_id", "orders", "order_id", True),
    ("order_items", "product_id", "products", "product_id", False),
    ("order_items", "seller_id", "sellers", "seller_id", False),
]:
    n = orphans(child, ccol, parent, pcol)
    check("referential integrity", f"{child}.{ccol} -> {parent}", n == 0,
          f"{n} orphan rows", critical=crit)

# ---------- 5. value ranges ----------
rules = [
    ("order_items", "price >= 0 AND freight_value >= 0", True),
    ("order_payments", "payment_value >= 0", True),
    ("order_reviews", "review_score BETWEEN 1 AND 5", True),
    ("orders", "delivery_days IS NULL OR delivery_days >= 0", False),
]
for n, rule, crit in rules:
    bad = spark.table(tbl("silver", n)).filter(f"NOT ({rule})").count()
    check(f"range: {rule}", f"silver.{n}", bad == 0, f"{bad} violations", critical=crit)

# ---------- 6. reconciliation: gold must match silver ----------
valid = spark.table(tbl("silver", "orders")).filter(
    ~F.col("order_status").isin("canceled", "unavailable")).select("order_id")
items = spark.table(tbl("silver", "order_items"))

silver_rev = items.join(valid, "order_id").agg(F.sum("price")).first()[0]
gold_rev = spark.table(tbl("gold", "daily_sales")).agg(F.sum("revenue")).first()[0]
diff = abs(silver_rev - gold_rev) / silver_rev
check("revenue reconciles (silver vs gold)", "gold.daily_sales", diff < 1e-4,
      f"silver={silver_rev:,.2f} gold={gold_rev:,.2f} diff={diff:.6%}")

silver_orders = items.select("order_id").distinct().join(valid, "order_id").count()
gold_orders = spark.table(tbl("gold", "daily_sales")).agg(F.sum("orders")).first()[0]
check("order count reconciles (silver vs gold)", "gold.daily_sales",
      silver_orders == gold_orders, f"silver={silver_orders:,} gold={gold_orders:,}")

# ---------- save results + fail the job on critical failures ----------
schema = ("checked_at timestamp, check_name string, table_name string, "
          "passed boolean, detail string, critical boolean")
(spark.createDataFrame(results, schema)
    .write.format("delta").mode("append")
    .saveAsTable(tbl("gold", "dq_results")))

failed = [r for r in results if not r[3] and r[5]]
print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
if failed:
    raise Exception(f"{len(failed)} critical data quality checks failed: "
                    + "; ".join(f"{r[1]} on {r[2]}" for r in failed))