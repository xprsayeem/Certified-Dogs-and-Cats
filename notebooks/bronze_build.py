# Databricks notebook source
# DBTITLE 1,Creating a Catalog and Schema
# MAGIC %sql
# MAGIC -- Creating a Catalog and Schema
# MAGIC -- Catalog - "Project Space", Schema - "Folder inside my catalog. Contains tables, views, data assets" 
# MAGIC CREATE CATALOG IF NOT EXISTS pets COMMENT "Dogs and Cats Proj Catalog";
# MAGIC GRANT USE CATALOG ON CATALOG pets TO `sayeem.m@hotmail.com`;
# MAGIC CREATE SCHEMA IF NOT EXISTS pets.core COMMENT "Core tables for licensed pets";
# MAGIC GRANT USE SCHEMA, CREATE TABLE ON SCHEMA pets.core TO `sayeem.m@hotmail.com`;
# MAGIC
# MAGIC -- Ensure I'm writing into the correct Catalog and Schema
# MAGIC USE CATALOG pets;
# MAGIC USE SCHEMA core;

# COMMAND ----------

# DBTITLE 1,Defining Paths and Schema
# Establishing Paths
bucket_path = "s3a://<your-bucket>"
dataset_name = "licensed_pets"

raw_csv_path = f"{bucket_path}/raw/{dataset_name}/ingestion_date=*/licensed_dogs_and_cats.csv"
bronze_table_path = f"{bucket_path}/bronze/{dataset_name}"
silver_table_path = f"{bucket_path}/silver/{dataset_name}"
gold_table_path = f"{bucket_path}/gold/{dataset_name}" # Workin on it

# Schema
from pyspark.sql.types import *

schema = StructType([
    StructField("_id", IntegerType(), False),
    StructField("Year", IntegerType(), True),
    StructField("FSA", StringType(), True),
    StructField("ANIMAL_TYPE", StringType(), True),
    StructField("PRIMARY_BREED", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Read Raw CSV and Write Bronze Delta Tables
from pyspark.sql.functions import *

# Read Raw .csv file
raw_data_frame = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv(raw_csv_path)
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
        # Ingestion timestamp
        .withColumn("ingestion_ts", current_timestamp())
        .dropDuplicates(["_id"])
)

# Quick error checking - Fail if any assertions do not pass
# No records with null identifier "_id"
assert bronze_data_frame.filter(col("_id").isNull()).isEmpty()
# Count how many rows are not DOG or CAT
invalid_count = bronze_data_frame.filter(~col("ANIMAL_TYPE").isin("DOG","CAT")).count()
# Fail if there are any invalid rows
assert invalid_count == 0

(
    bronze_data_frame.write
        # Choose the Delta Lake table format.
        .format("delta")
        # Overwrite any tables which may already be at the specified location, in the future we will use append for an automated load of additional data into the bronze table
        .mode("overwrite")
        # Partitioning data into separate subfolders via Year and Animal_Type, other columns would cause over-partitioning
        .partitionBy("Year", "ANIMAL_TYPE")
        # Writing the delta table to the path within our bucket which we specified earlier 
        .save(bronze_table_path)
)