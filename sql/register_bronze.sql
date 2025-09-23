USE CATALOG pets; USE SCHEMA core;
CREATE TABLE IF NOT EXISTS pets.core.licensed_pets_bronze
USING DELTA
LOCATION 's3://<your-bucket>/bronze/licensed_pets';
