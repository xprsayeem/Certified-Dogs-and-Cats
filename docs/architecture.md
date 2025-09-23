# Architecture: Raw to Bronze

1) Raw files land in S3 at  
   `s3://<your-bucket>/raw/licensed_pets/ingestion_date=YYYY-MM-DD/licensed_dogs_and_cats.csv`

2) Databricks reads the CSV with an explicit schema  
   `_id INT NOT NULL, Year INT, FSA STRING, ANIMAL_TYPE STRING, PRIMARY_BREED STRING`

3) Standardization in Bronze  
   uppercase and trim text, boolean `FSA_VALID`, `ingestion_ts`, de-duplicate on `_id`

4) Write Bronze as Delta to  
   `s3://<your-bucket>/bronze/licensed_pets` partitioned by `Year` and `ANIMAL_TYPE`

5) Register the table in Unity Catalog  
   `pets.core.licensed_pets_bronze` then query with SQL
