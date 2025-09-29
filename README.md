# Certified-Dogs-and-Cats
This project turns the City of Toronto’s “Licensed Cats and Dogs” CSV into a small, production-like lakehouse on AWS with Databricks. The goal is to show clear, reliable data engineering practices on a real civic dataset while keeping cost low and the code reproducible.

Open Data Link: https://open.toronto.ca/dataset/licensed-dogs-and-cats

# What this project is about
- Build a small end to end lakehouse on AWS with Databricks, from raw files to modeled tables you can query
- Practice production habits: reliable pipelines, simple tests, clear lineage, and cost awareness
- Apply basic governance with Unity Catalog and IAM so access is deliberate and auditable
- Ship in small milestones with concise docs and reproducible steps so others can follow along

# Milestone 1 - Ingested raw data to S3, transformed to Bronze Delta tables in S3 with Databricks
- Created new S3 bucket, uploaded raw data from City of Toronto Open Data
- In AWS IAM, made a role that can read/write the bucket, then set its trust policy to the Databricks principal with the ExternalId Databricks gave me.
- In Databricks, created a Storage Credential using that role’s ARN.
- Still in Databricks, created an External Location pointing to my S3 bucket, validated it, and granted myself appropriate permissions.
  
**From Raw CSV to Bronze Delta**
- Created Catalog and Schema
  - Made pets catalog and pets.core schema in Unity Catalog, granted myself USE and CREATE TABLE, then set context with USE CATALOG pets; USE SCHEMA core;
- Defined paths and a strict schema
  - Set variables like bucket_path, dataset_name = licensed_pets, and S3 prefixes for raw and bronze. Declared an explicit PySpark schema to read raw data: _id int not null, Year int, FSA string, ANIMAL_TYPE string, PRIMARY_BREED string
- Read raw and wrote Bronze
  - Loaded CSV from raw/.../ingestion_date=*, normalized text to uppercase and trimmed, added FSA_VALID boolean, added ingestion_ts, dropped duplicate _id's. Wrote a Delta table to s3://<bucket>/bronze/licensed_pets partitioned by Year and ANIMAL_TYPE, then registered it as pets.core.licensed_pets_bronze

**Quick Validation**
- Row count: 173,937 total
- Missing or malformed FSA: 301 rows
- ANIMAL_TYPE only in {DOG, CAT}
- Partitions created: Year=2023/2024/2025 with ANIMAL_TYPE=CAT and ANIMAL_TYPE=DOG subfolders
- Sample queries run: counts by year and type, duplicate _id check

# Milestone 2 - Hardened Bronze, Built Silver tables with breed normalization and a two-task Workflow
**What I shipped this update**
- Job workflow: One Databricks Workflow with two tasks that share an ingestion_date parameter.
  - Raw_To_Bronze runs the Bronze notebook.
  - Bronze_To_Silver runs the silver.py notebook depending on if Bronze is successful and no-ops if there are no new IDs to insert.
- Bronze notebook hardening:
  - Added a date widget with default to the current date, allows for automation and easy backfill.
  - pets.core.load_control table to record successful loads per day (probably will need one for silver -> gold as well).
  - File presence check in S3, then read CSV with explicit schema.
  - Normalize text, generate FSA_VALID, set ingestion_ts and ingestion_date.
  - In-batch dedup by _id -> assert that batches dont have entries with duplicate _id.
  - Left-anti join with existing Bronze to only insert entries with new _id.
  - Write by table name with partitioning on Year and ANIMAL_TYPE.
- Silver notebook v1:
  - Re-standardize - repeat some of the normalization from bronze (to make extra sure).
  - Some breeds listed in primary_breed had several variations, the primary focus of silver was to normalize these breeds.
  - Build breed_variant_key from PRIMARY_BREED and map via pets.ref.breed_mapping which ruled how the different variants should map to singular stadard breeds.
  - Set breed_standard to mapped value, else fall back to breed_raw; boolean breed_mapped for coverage.
  - **My initial attempt, Silver V1 was able to map 81.44% of all animal breeds to standard breed names!!**
  - In-batch dedup by _id using ingestion_ts as tiebreaker.
  - Left-anti join vs existing Silver to avoid re-inserts of old data.
  - Write partitioned Delta table pets.core.licensed_pets_silver.
  - Health view pets.core.v_silver_health for quick checks.
- Operational policies
  - Idempotent loads: Insert-only with anti-join, so re-runs do not insert duplicate data.
  - Earliest sighting: With insert-only, the first day that writes an _id sets its ingestion_date.

**Corrections to Milestone 1**
- Realized Unity Catalog tables must be registered before writing to them: Created UC table at a clean S3 path with DEFAULT COLLATION UTF8_BINARY, then wrote by table name. This avoided the collation mismatch I hit when pointing a table at an already-written path.
- Paths in SQL: Used LOCATION 's3://...' in SQL DDL. Kept s3a:// only in PySpark read paths.

**Next up ‼️‼️**
- Silver data quality dashboard in Databricks SQL.
- Gold models for common queries: yearly license counts by FSA, top breeds by area, trend lines AND MORE.






