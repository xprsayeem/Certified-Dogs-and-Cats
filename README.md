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
  - Set variables like bucket_path, dataset_name = licensed_pets, and S3 prefixes for raw and bronze. Declared an explicit PySpark schema: _id int not null, Year int, FSA string, ANIMAL_TYPE string, PRIMARY_BREED string
- Read raw and wrote Bronze
  - Loaded CSV from raw/.../ingestion_date=*, normalized text to uppercase and trimmed, added FSA_VALID boolean, added ingestion_ts, dropped duplicate _id. Wrote a Delta table to s3://<bucket>/bronze/licensed_pets partitioned by Year and ANIMAL_TYPE, then registered it as pets.core.licensed_pets_bronze

**Quick Validation**
- Row count: 173,937 total
- Missing or malformed FSA: 301 rows
- ANIMAL_TYPE only in {DOG, CAT}
- Partitions created: Year=2023/2024/2025 with ANIMAL_TYPE=CAT and ANIMAL_TYPE=DOG subfolders
- Sample queries run: counts by year and type, duplicate _id check
