# Runbook: Raw → Bronze → Silver

This file shows how to run, validate, and re-run the pipeline at a glance.

## Prereqs
- Workspace is linked to Unity Catalog
- External Location to S3 is validated
- You have `USE` and `CREATE TABLE` on `pets.core`
 
## One-time setup (SQL)
```sql
USE CATALOG pets;
USE SCHEMA core;
```

---

## Bronze quick run

### Inputs
S3 path per drop:
```
s3://<bucket>/raw/licensed_pets/ingestion_date=YYYY-MM-DD/licensed_dogs_and_cats.csv
```

### Notebook params
```python
bucket_path    = "s3a://<bucket>"
dataset_name   = "licensed_pets"
ingestion_date = "YYYY-MM-DD"  # or set by widget/job
raw_path       = f"{bucket_path}/raw/{dataset_name}/ingestion_date={ingestion_date}/*.csv"
```

### Steps
1. Read CSV with explicit schema.
2. Standardize text (upper, trim), compute `FSA_VALID`, add `ingestion_ts` and `ingestion_date`.
3. De-duplicate in batch by `_id`.
4. Left anti join with existing Bronze to insert only new `_id`.
5. Write Delta partitioned by `Year, ANIMAL_TYPE` to `pets.core.licensed_pets_bronze`.

### Validate (SQL)
```sql
SELECT COUNT(*) FROM pets.core.licensed_pets_bronze;
SELECT Year, ANIMAL_TYPE, COUNT(*) c
FROM pets.core.licensed_pets_bronze GROUP BY Year, ANIMAL_TYPE ORDER BY Year, ANIMAL_TYPE;
```

### Re-run
- New drop: run the job with that `ingestion_date` (append by table name).
- Full rebuild: truncate table, then re-run dates in order.
- Slice rebuild: overwrite specific partitions as needed.

### Maintenance
```sql
OPTIMIZE pets.core.licensed_pets_bronze;
-- optional
OPTIMIZE pets.core.licensed_pets_bronze ZORDER BY (_id, FSA);
VACUUM pets.core.licensed_pets_bronze RETAIN 168 HOURS;
```

---

## Silver quick run

### Inputs
- Source table: `pets.core.licensed_pets_bronze`
- Optional mapping table: `pets.ref.breed_mapping`

### Notebook params
```python
ingestion_date = "YYYY-MM-DD"  # same as Bronze run
```

### Steps
1. Read Bronze for that `ingestion_date`.
2. Re-standardize strings.
3. Set `FSA` to NULL when invalid, keep boolean `FSA_VALID`.
4. Build `breed_variant_key`; left join to mapping; set `breed_standard` with fallback to `breed_raw`; flag `breed_mapped` - for tracking mapping coverage.
5. De-duplicate in batch by `_id` using latest `ingestion_ts`.
6. Left anti join vs existing Silver to insert only new `_id`.
7. Write Delta partitioned by `Year, ANIMAL_TYPE` to `pets.core.licensed_pets_silver`.

### Validate (SQL)
```sql
-- Compare Total vs Distinct by _id
SELECT COUNT(*) total, COUNT(DISTINCT _id) distinct_ids
FROM pets.core.licensed_pets_silver;

-- Compare # of Breed Mapped vs. Total to fin mapping coverage - currently at 81.44%!
SELECT 
  SUM(CASE WHEN NOT breed_mapped THEN 1 ELSE 0 END) AS unmapped,
  ROUND(100.0 * SUM(CASE WHEN breed_mapped THEN 1 ELSE 0 END)/COUNT(*), 2) AS pct_mapped
FROM pets.core.licensed_pets_silver;

-- FSA flag matches column
SELECT COUNT(*) bad
FROM pets.core.licensed_pets_silver
WHERE (FSA_VALID AND FSA IS NULL) OR (NOT FSA_VALID AND FSA IS NOT NULL);
```

### Maintenance
```sql
OPTIMIZE pets.core.licensed_pets_silver;
VACUUM   pets.core.licensed_pets_silver RETAIN 168 HOURS;
```

---

## Troubleshooting
- **Access denied**: check External Location grants and cluster mode.
- **Empty read**: confirm `ingestion_date` path exists in S3.
- **Dupes**: ensure in-batch dedup by `_id` and anti join are active.
- **Mapping 100%**: verify `breed_mapped` is computed from mapping hit, not fallback.
