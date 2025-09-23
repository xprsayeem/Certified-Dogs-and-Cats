# Runbook: Raw to Bronze

## Purpose
How to build, validate, and re-run the Bronze Delta table for the Licensed Pets dataset.

## Before you start
- Workspace is linked to Unity Catalog
- External Location to S3 is validated
- You have `USE` and `CREATE TABLE` permisssions on the target catalog and schema
- Cluster uses a Unity Catalog access mode

## One-time workspace setup
```sql
USE CATALOG pets;
USE SCHEMA core;
```

## Notebook variables
```python
bucket_path = "s3a://<your-bucket>"
dataset_name = "licensed_pets"

raw_csv_path      = f"{bucket_path}/raw/{dataset_name}/ingestion_date=*/licensed_dogs_and_cats.csv"
bronze_table_path = f"{bucket_path}/bronze/{dataset_name}"
```

## Build Bronze
1. Upload the CSV to `raw/licensed_pets/ingestion_date=YYYY-MM-DD/`.
2. In the Bronze notebook:
   - read CSV with the explicit schema
   - standardize text to uppercase and trim
   - create `FSA_VALID` boolean and `ingestion_ts` timestamp
   - drop duplicate `_id`
   - write Delta to `bronze_table_path` with `mode("overwrite")` and `partitionBy("Year","ANIMAL_TYPE")`
3. Register the table:
```sql
CREATE TABLE IF NOT EXISTS pets.core.licensed_pets_bronze
USING DELTA
LOCATION 's3://<your-bucket>/bronze/licensed_pets';
```

## Validate
```sql
-- total rows
SELECT COUNT(*) FROM pets.core.licensed_pets_bronze;

-- validity distribution
SELECT FSA_VALID, COUNT(*) FROM pets.core.licensed_pets_bronze GROUP BY FSA_VALID;

-- counts by year and type
SELECT Year, ANIMAL_TYPE, COUNT(*) AS cnt
FROM pets.core.licensed_pets_bronze
GROUP BY Year, ANIMAL_TYPE
ORDER BY Year, ANIMAL_TYPE;
```

## Re-run and backfill
- New drop: change writer to `mode("append")`, re-run the notebook.
- Full rebuild: use `mode("overwrite")`, re-run.
- Rebuild a slice: filter the DataFrame to the target `Year` and write only that subset. Example:
```python
bronze_df.filter("Year = 2024")   .write.format("delta").mode("overwrite")   .partitionBy("Year","ANIMAL_TYPE")   .save(bronze_table_path)
```

## Maintenance
```sql
-- compact small files when needed
OPTIMIZE pets.core.licensed_pets_bronze;

-- remove old file versions after a retention window
VACUUM pets.core.licensed_pets_bronze RETAIN 168 HOURS;
```

## Cleanup
- Drop the registered table:
```sql
DROP TABLE IF EXISTS pets.core.licensed_pets_bronze;
```
- If you also want to remove files, delete the `bronze/licensed_pets` folder in S3.

## Troubleshooting
- Access denied: check External Location grants and cluster access mode
- Empty read: confirm the `ingestion_date=...` path matches your wildcard
- Schema mismatch: ensure CSV header names match the explicit schema.
