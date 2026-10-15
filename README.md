# Olist E-commerce Sales Analytics Pipeline (Databricks)

End-to-end ETL pipeline on Databricks using the medallion architecture (Bronze / Silver / Gold),
built on the public Olist Brazilian e-commerce dataset (about 100K orders, 9 related tables).

## Architecture

```
Olist CSVs  ->  Bronze (raw Delta)  ->  Silver (clean, typed)  ->  Gold (reporting)  ->  Dashboard
                Auto Loader             MERGE INTO upserts         4 business tables
                                                 ^
                                        Data quality checks (35)
```

Orchestrated by a Databricks Workflow job (`olist_etl_pipeline`) with four chained tasks:
`bronze_ingest` -> `silver_clean` -> `gold_marts` -> `data_quality`.

![Run graph](images/run_graph.png)

## Tech stack
Databricks, PySpark, Delta Lake, Auto Loader, Unity Catalog, Workflows, Databricks SQL dashboards

## Layers

| Layer | What it does |
|---|---|
| Bronze | Incremental ingestion of 9 CSV files with Auto Loader. Raw data is kept as-is, plus `_ingested_at` and `_source_file` audit columns |
| Silver | Type casting, deduplication with window functions, null and invalid-value filtering, English category names, derived `delivery_days` and `is_late` columns, idempotent `MERGE INTO` upserts |
| Gold | Four reporting tables: `daily_sales`, `top_products`, `customer_lifetime_value`, `delivery_performance` |

## Data quality
35 automated checks run after every load: row counts, null keys, unique primary keys,
referential integrity, value ranges, and Silver-to-Gold reconciliation.
Revenue reconciles to the cent between Silver and Gold (13,494,400.74 BRL across 98,199 orders).
Results are logged to `gold.dq_results`, and any critical failure fails the job.

## Dashboard
A Databricks SQL dashboard on the Gold tables: KPIs, monthly revenue, top categories, customer retention, and delivery performance by state.

![Dashboard](images/dashboard.png)

## Key findings
- Total revenue is about 13.5M BRL across 98,199 valid orders, with an average order value of 137.42 BRL
- Monthly revenue grew steadily through 2017, peaked around November 2017, and then levelled off in 2018
- Top categories by revenue: health_beauty, watches_gifts, bed_bath_table
- Only about 3% of customers (2,887 of 94,983) made a repeat purchase, but they spent about 1.9x more per customer than one-time buyers, so retention is the biggest growth opportunity
- Late deliveries are concentrated in the north and northeast: about 24% of orders in AL and 20% in MA arrive after the estimated date, versus about 3 to 6% in RO, PR, MG and SP

## Repo structure
```
notebooks/   01_bronze_ingest, 02_silver_clean, 03_gold_marts, 04_data_quality
images/      run_graph.png, dashboard.png
README.md
```

## How to run
1. Upload the Olist CSV files to `/Volumes/workspace/bronze/raw/olist/`
2. Create the schemas `bronze`, `silver` and `gold` in your catalog
3. Create a Databricks job with the four notebooks as chained tasks
4. Run the job, then open the dashboard

## Dataset
[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (Kaggle)
