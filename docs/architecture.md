# Architecture

## Raw
1. Files land in S3  
   `s3://<bucket>/raw/licensed_pets/ingestion_date=YYYY-MM-DD/licensed_dogs_and_cats.csv`
2. Source format  
   CSV with explicit schema: `_id INT NOT NULL, Year INT, FSA STRING, ANIMAL_TYPE STRING, PRIMARY_BREED STRING`

## Bronze
1. Read CSV with schema
2. Standardize
   - uppercase and trim text
   - compute `FSA_VALID`
   - add `ingestion_ts` and `ingestion_date`
   - de-duplicate within batch by `_id`
3. Only new IDs
   - left anti join with existing Bronze on `_id`
4. Storage and table
   - Delta Lake partitioned by `Year` and `ANIMAL_TYPE`
   - Registered as `pets.core.licensed_pets_bronze`  
     Location: `s3://<bucket>/bronze_v2/licensed_pets`

## Silver
1. Read from Bronze for a given `ingestion_date`
2. Clean and validate
   - re-apply uppercase and trim
   - set `FSA` to NULL if invalid, keep `fsa_is_valid` flag
3. Breed normalization
   - `breed_variant_key` from `PRIMARY_BREED`
   - left join to `pets.ref.breed_mapping`
   - `breed_standard` if mapped, else `breed_raw`
4. De-duplicate within batch by `_id`, newest by `ingestion_ts`
5. Only new IDs to Silver using left anti join
6. Storage and table
   - Delta Lake partitioned by `Year` and `ANIMAL_TYPE`
   - Registered as `pets.core.licensed_pets_silver`  
     Location: `s3://<bucket>/silver/licensed_pets`

## Orchestration
- Databricks Workflow with two tasks  
  1) Raw_To_Bronze  
  2) Bronze_To_Silver (depends on Bronze)
- Shared `ingestion_date` parameter
- Load control table: `pets.core.load_control` records successful days

## Governance
- Unity Catalog for tables and permissions
