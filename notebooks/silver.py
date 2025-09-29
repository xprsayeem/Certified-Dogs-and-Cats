# Databricks notebook source
# DBTITLE 1,Unity Catalog - Context
# MAGIC %sql
# MAGIC USE CATALOG pets;
# MAGIC USE SCHEMA core;

# COMMAND ----------

# DBTITLE 1,Configuration + Ingestion Date Widget
# Config and params
bucket_path   = "s3a://<your-bucket>"
dataset_name  = "licensed_pets"

bronze_table  = "pets.core.licensed_pets_bronze"
silver_table  = "pets.core.licensed_pets_silver"
mapping_table = "pets.ref.breed_mapping"

# Job ingestion date parameter
dbutils.widgets.text("ingestion_date", "")
ingestion_date = dbutils.widgets.get("ingestion_date") or spark.sql(
    "select date_format(current_date(), 'yyyy-MM-dd') d").first().d

ingestion_date_date = spark.sql(f"select to_date('{ingestion_date}','yyyy-MM-dd') d").first().d
assert ingestion_date_date is not None, "ingestion_date must be YYYY-MM-DD"


# COMMAND ----------

# DBTITLE 1,Standardization + Writing Silver
from pyspark.sql.window import Window
from pyspark.sql.functions import col, upper, trim, regexp_replace, when, lit, to_date, current_timestamp, row_number

bronze_data_frame = spark.table(bronze_table)
if ingestion_date:
    bronze_data_frame = bronze_data_frame.filter(f"ingestion_date = DATE('{ingestion_date}')")

# Defensive/repeated standardizations from Bronze
standardized_breeds = (bronze_data_frame
        .withColumn("PRIMARY_BREED", col("PRIMARY_BREED"))
        .withColumn("FSA", upper(trim(col("FSA"))))
        .withColumn("ANIMAL_TYPE", upper(trim(col("ANIMAL_TYPE"))))
        .withColumn("FSA_VALID", col("FSA").rlike(r"^[A-Z][0-9][A-Z]$"))
        .withColumn("FSA", when(col("FSA_VALID"), col("FSA")).otherwise(lit(None)))
)

# Build breed_raw and breed_variant_key, then map to breed_standard
standardized_breeds = (standardized_breeds
        .withColumn("breed_raw", upper(trim(col("PRIMARY_BREED"))))
        .withColumn("breed_variant_key", regexp_replace(col("breed_raw"), "[^A-Z0-9]", ""))
)

# Load the breed mapping table, selecting only the columns needed for mapping
mapping = spark.table(mapping_table).select("breed_variant_key", "breed_standard").alias("m")

# Join the standardized breeds with the mapping table to map raw breeds to standard breeds
mapped = (
    standardized_breeds.alias("b")  # Give the standardized breeds DataFrame an alias "b"
    .join(mapping, on="breed_variant_key", how="left")  # Left join on breed_variant_key to add breed_standard if available
    .withColumn(
        "breed_mapped",
        col("m.breed_standard").isNotNull()  # True if a mapping was found, else False
    )
    .withColumn(
        "breed_standard",
        when(col("m.breed_standard").isNotNull(), col("m.breed_standard"))  # Use mapped breed if available
        .otherwise(col("breed_raw"))  # Otherwise, keep the original breed value
    )
)

# Keep only rows that have all valid required fields present 
valid = (mapped
        .filter(col("_id").isNotNull())
        .filter(col("Year").isNotNull())
        .filter(col("ANIMAL_TYPE").isin("DOG", "CAT"))
        .filter(col("PRIMARY_BREED").isNotNull())
        .filter(col("ingestion_ts").isNotNull())
        .filter(col("ingestion_date").isNotNull())
)

# De-duplicate within the batch by _id, pick the newest by ingestion_ts
w = Window.partitionBy("_id").orderBy(col("ingestion_ts").desc(), col("Year").desc_nulls_last())
dedup_batch = (valid
            .withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .drop("rn"))


# Prepare the final Silver DataFrame with required columns and metadata
silver_data_frame = (
    dedup_batch
    # Add silver processing timestamp for auditability
    .withColumn("processed_ts", current_timestamp())
    # Add ingestion_date as a date column for partitioning and tracking
    .withColumn("ingestion_date", to_date(lit(ingestion_date)))
    # Select and order columns for the Silver table schema
    .select(
        "_id",                # Unique identifier for each animal
        "Year",               # Year of license or record
        "ANIMAL_TYPE",        # Standardized animal type (DOG or CAT)
        "FSA",                # Validated Forward Sortation Area (postal code prefix)
        "FSA_VALID",          # Boolean flag for FSA validity
        "PRIMARY_BREED",      # Original Primary Breed label
        "breed_raw",          # Cleaned original breed string
        "breed_variant_key",  # Normalized breed key for mapping
        "breed_standard",     # Mapped or standardized breed name
        "breed_mapped",       # Boolean flag if breed was mapped
        "ingestion_date",     # Date the data was ingested
        "ingestion_ts",       # Original ingestion timestamp from Bronze
        "processed_ts"        # Timestamp when processed into Silver
    )
)

# Guards
# 1) No null _id
assert silver_data_frame.filter(col("_id").isNull()).limit(1).count() == 0

# 2) Batch self-uniqueness
assert silver_data_frame.select("_id").distinct().count() == silver_data_frame.count()

# 3) Animal type whitelist
assert silver_data_frame.filter(~col("ANIMAL_TYPE").isin("DOG","CAT")).limit(1).count() == 0

# keep only animals not already present in Silver
existing_ids = spark.table(silver_table).select("_id").dropDuplicates()
to_write = silver_data_frame.join(existing_ids, on="_id", how="left_anti")
# quick exit if nothing new
if to_write.limit(1).count() == 0:
    dbutils.notebook.exit(f"Silver: no new IDs for {ingestion_date}")

# write only the new rows to Silver!
(to_write.write
    .format("delta")
    .mode("append")
    .partitionBy("Year","ANIMAL_TYPE")
    .saveAsTable(silver_table))

# COMMAND ----------

# DBTITLE 1,Registering Unity Catalog Table
# MAGIC %sql
# MAGIC /*
# MAGIC CREATE TABLE IF NOT EXISTS pets.core.licensed_pets_silver (
# MAGIC   _id               INT,
# MAGIC   Year              INT,
# MAGIC   FSA               STRING,
# MAGIC   FSA_VALID         BOOLEAN,
# MAGIC   ANIMAL_TYPE       STRING,
# MAGIC   PRIMARY_BREED     STRING,
# MAGIC   breed_raw         STRING,
# MAGIC   breed_variant_key STRING,
# MAGIC   breed_standard    STRING,
# MAGIC   breed_mapped      BOOLEAN,
# MAGIC   ingestion_date    DATE,
# MAGIC   ingestion_ts      TIMESTAMP,
# MAGIC   processed_ts      TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (Year, ANIMAL_TYPE)
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC LOCATION 's3://<your-bucket>/silver/licensed_pets';
# MAGIC */
# MAGIC

# COMMAND ----------

# DBTITLE 1,Silver Health View
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW pets.core.v_silver_health AS
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   COUNT(DISTINCT _id) AS distinct_ids,
# MAGIC   SUM(CASE WHEN NOT breed_mapped THEN 1 ELSE 0 END) AS unmapped_breeds,
# MAGIC   SUM(CASE WHEN FSA IS NULL THEN 1 ELSE 0 END) AS null_fsa_rows,
# MAGIC   MAX(processed_ts) AS last_processed_ts
# MAGIC FROM pets.core.licensed_pets_silver;
# MAGIC