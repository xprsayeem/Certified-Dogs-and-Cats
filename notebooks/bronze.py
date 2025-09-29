# Databricks notebook source
# DBTITLE 1,Unity Catalog - Context
# MAGIC %sql
# MAGIC USE CATALOG pets;
# MAGIC USE SCHEMA core;

# COMMAND ----------

# DBTITLE 1,Defining Schema for reading Raw Data
# Schema for reading raw data
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

csv_schema = StructType([
    StructField("_id", IntegerType(), False),
    StructField("Year", IntegerType(), True),
    StructField("FSA", StringType(), True),
    StructField("ANIMAL_TYPE", StringType(), True),
    StructField("PRIMARY_BREED", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Ingestion Date Widget, load control table, skip logic, set raw path
from pyspark.sql.functions import current_date, date_format, to_date, lit, col

# Establishing Paths
bucket_path = "s3a://<your-bucket>"
dataset_name = "licensed_pets"
bronze_table  = "pets.core.licensed_pets_bronze"

# Create widget and default ingestion date parameter
dbutils.widgets.text("ingestion_date", "")
ingestion_date = dbutils.widgets.get("ingestion_date") or spark.sql(
    "select date_format(current_date(),'yyyy-MM-dd') d").first().d

# Validate format and keep a Spark Date type for comparisons
ingestion_date_date = spark.sql(f"select to_date('{ingestion_date}', 'yyyy-MM-dd') d").first().d
assert ingestion_date_date is not None, "ingestion_date must be YYYY-MM-DD"

# Ensure control table exists (SQL executed from Python)
spark.sql("""
CREATE TABLE IF NOT EXISTS pets.core.load_control (
  dataset STRING,
  ingestion_date DATE,
  loaded_ts TIMESTAMP
) USING DELTA
""")

# Skip if already processed
already = (
  spark.table("pets.core.load_control")
       .where((col("dataset") == lit("licensed_pets")) &
              (col("ingestion_date") == lit(ingestion_date_date)))
       .limit(1).count() > 0)
if already:
    dbutils.notebook.exit(f"Skip. {ingestion_date} already processed")

# Build the raw file path for this run
raw_path = f"{bucket_path}/raw/{dataset_name}/ingestion_date={ingestion_date}/*.csv"

try:
    files = dbutils.fs.ls(f"{bucket_path}/raw/{dataset_name}/ingestion_date={ingestion_date}/")
except Exception:
    files = []
if len(files) == 0:
    dbutils.notebook.exit(f"No files for {ingestion_date}")


# COMMAND ----------

# DBTITLE 1,Standardization + Writing Bronze
from pyspark.sql.functions import *

# Read Raw .csv file
raw_data_frame = (
    spark.read
    .option("header", True)
    .schema(csv_schema)
    .csv(raw_path)
)

# Create Cleaned Bronze data frame
fsa_pattern = '^[A-Z][0-9][A-Z]$'
bronze_data_frame = (
    raw_data_frame
        # Normalizing text fields - make all upper case, remove white spaces
        .withColumn("FSA", upper(trim(col("FSA"))))
        .withColumn("ANIMAL_TYPE", upper(trim(col("ANIMAL_TYPE"))))
        .withColumn("PRIMARY_BREED", upper(trim(col("PRIMARY_BREED"))))
        # Create Column FSA_VALID to make sure all FSA's match Canada's A1A pattern, True if yes/ False if no or null
        .withColumn("FSA_VALID", when(col("FSA").isNotNull() & col("FSA").rlike(fsa_pattern), True).otherwise(False))
        # Ingestion datestamp and timestamp
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("ingestion_date", to_date(lit(ingestion_date)))
)

# Guards
# No records with null identifier "_id"
has_null_id = bronze_data_frame.filter(col("_id").isNull()).limit(1).count() == 1
assert not has_null_id, "Found null _id in batch"
# No duplicate _id numbers within the data frame
assert (bronze_data_frame.select("_id").distinct().count() == bronze_data_frame.count()), \
       "_id not unique within this batch"
# Count how many rows are not DOG or CAT
invalid_count = bronze_data_frame.filter(~col("ANIMAL_TYPE").isin("DOG","CAT")).count()
# Fail if there are any invalid rows
assert invalid_count == 0

# Write to UC Bronze table and Load Control table only if successful
# existing_ids left anti-join -> to_write is a table with only rows that are not in existing_ids
existing_ids = spark.table(bronze_table).select("_id").dropDuplicates()
to_write = bronze_data_frame.join(existing_ids, on="_id", how="left_anti")

if to_write.limit(1).count() != 1:
    dbutils.notebook.exit(f"No new IDs to load for {ingestion_date}")

# Write to Bronze table
to_write.write.format("delta").mode("append").saveAsTable(bronze_table)
spark.sql(f"""
  INSERT INTO pets.core.load_control 
  VALUES ('licensed_pets', DATE('{ingestion_date}'), current_timestamp())
""")
print(f"Bronze loaded {ingestion_date}: inserted {to_write.count()} new rows")



# COMMAND ----------

# DBTITLE 1,Registering Unity Catalog Table
# MAGIC %sql
# MAGIC /*
# MAGIC CREATE TABLE pets.core.licensed_pets_bronze (
# MAGIC   _id INT,
# MAGIC   Year INT,
# MAGIC   FSA STRING,
# MAGIC   FSA_VALID BOOLEAN,
# MAGIC   ANIMAL_TYPE STRING,
# MAGIC   PRIMARY_BREED STRING,
# MAGIC   ingestion_ts TIMESTAMP,
# MAGIC   ingestion_date DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (Year, ANIMAL_TYPE)
# MAGIC DEFAULT COLLATION UTF8_BINARY
# MAGIC LOCATION 's3://<your-bucket>/bronze_v2/licensed_pets';
# MAGIC */

# COMMAND ----------

# DBTITLE 1,Bronze Health View
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW pets.core.v_bronze_health AS
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   SUM(CASE WHEN FSA_VALID THEN 0 ELSE 1 END) AS invalid_fsa_rows,
# MAGIC   COUNT(DISTINCT Year) AS years,
# MAGIC   MAX(ingestion_ts) AS last_commit_ts
# MAGIC FROM pets.core.licensed_pets_bronze;
# MAGIC