# Databricks notebook source
# DBTITLE 1,Creating Pets.gold
# MAGIC %sql
# MAGIC USE CATALOG pets;
# MAGIC CREATE SCHEMA IF NOT EXISTS pets.gold COMMENT 'Curated analytics for licensed pets';
# MAGIC USE SCHEMA gold;

# COMMAND ----------

# DBTITLE 1,Creating Gold Source View
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW pets.gold.licensed_pets_gold_src AS
# MAGIC SELECT
# MAGIC   _id,
# MAGIC   Year,
# MAGIC   ANIMAL_TYPE,
# MAGIC   FSA,
# MAGIC   breed_standard,
# MAGIC   breed_mapped,
# MAGIC   ingestion_ts,
# MAGIC   processed_ts
# MAGIC FROM pets.core.licensed_pets_silver
# MAGIC WHERE Year IS NOT NULL
# MAGIC   AND ANIMAL_TYPE IN ('DOG','CAT')
# MAGIC   AND breed_standard IS NOT NULL;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Totals By Year and Type
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW pets.gold.v_totals_by_year_type AS
# MAGIC WITH totals AS (
# MAGIC   SELECT
# MAGIC     Year,
# MAGIC     ANIMAL_TYPE,
# MAGIC     COUNT(*) AS total_count
# MAGIC   FROM pets.gold.licensed_pets_gold_src
# MAGIC   GROUP BY Year, ANIMAL_TYPE
# MAGIC ),
# MAGIC breed_counts AS (
# MAGIC   SELECT
# MAGIC     Year,
# MAGIC     ANIMAL_TYPE,
# MAGIC     breed_standard,
# MAGIC     COUNT(*) AS breed_count
# MAGIC   FROM pets.gold.licensed_pets_gold_src
# MAGIC   GROUP BY Year, ANIMAL_TYPE, breed_standard
# MAGIC ),
# MAGIC ranked AS (
# MAGIC   SELECT 
# MAGIC     Year,
# MAGIC     ANIMAL_TYPE,
# MAGIC     breed_standard,
# MAGIC     breed_count,
# MAGIC     ROW_NUMBER() OVER(
# MAGIC       PARTITION BY ANIMAL_TYPE, Year
# MAGIC       ORDER BY breed_count DESC, breed_standard ASC
# MAGIC     ) AS popularity
# MAGIC   FROM breed_counts
# MAGIC )
# MAGIC SELECT
# MAGIC   t.Year,
# MAGIC   t.ANIMAL_TYPE,
# MAGIC   t.total_count,
# MAGIC   r.popularity,
# MAGIC   r.breed_standard AS Top_Breeds,
# MAGIC   r.breed_count AS Breed_Count,
# MAGIC   ROUND(100.0 * r.breed_count / t.total_count, 2) AS top_breed_pct
# MAGIC FROM totals t JOIN ranked r 
# MAGIC   ON t.year = r.year 
# MAGIC   AND t.ANIMAL_TYPE = r.ANIMAL_TYPE 
# MAGIC WHERE r.popularity <= 10
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pets.gold.v_totals_by_year_type

# COMMAND ----------

# DBTITLE 1,Breed stats with share, rank, and YoY deltas
# MAGIC %sql
# MAGIC -- View: Breed stats with share, rank, and YoY deltas
# MAGIC CREATE OR REPLACE VIEW pets.gold.v_breed_stats AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     Year,
# MAGIC     ANIMAL_TYPE,
# MAGIC     breed_standard AS breed,
# MAGIC     COUNT(*) AS cnt
# MAGIC   FROM pets.gold.licensed_pets_gold_src
# MAGIC   GROUP BY Year, ANIMAL_TYPE, breed_standard
# MAGIC ),
# MAGIC with_share AS (
# MAGIC   SELECT
# MAGIC     Year,
# MAGIC     ANIMAL_TYPE,
# MAGIC     breed,
# MAGIC     cnt,
# MAGIC     SUM(cnt) OVER (PARTITION BY Year, ANIMAL_TYPE) AS total_in_group,
# MAGIC     RANK() OVER (PARTITION BY Year, ANIMAL_TYPE ORDER BY cnt DESC, breed) AS rnk
# MAGIC   FROM base
# MAGIC ),
# MAGIC final AS (
# MAGIC   SELECT
# MAGIC     Year,
# MAGIC     ANIMAL_TYPE AS Animal_Type,
# MAGIC     breed,
# MAGIC     cnt,
# MAGIC     total_in_group,
# MAGIC     ROUND(cnt / total_in_group, 4) AS share,
# MAGIC     rnk,
# MAGIC     LAG(cnt)   OVER (PARTITION BY ANIMAL_TYPE, breed ORDER BY Year) AS prev_cnt,
# MAGIC     LAG(ROUND(cnt / total_in_group, 4))
# MAGIC                 OVER (PARTITION BY ANIMAL_TYPE, breed ORDER BY Year) AS prev_share
# MAGIC   FROM with_share
# MAGIC )
# MAGIC SELECT
# MAGIC   Year,
# MAGIC   Animal_Type,
# MAGIC   breed,
# MAGIC   cnt,
# MAGIC   total_in_group AS total,
# MAGIC   share,
# MAGIC   rnk AS rank_in_year_type,
# MAGIC   prev_cnt,
# MAGIC   (cnt - prev_cnt) AS yoy_cnt_diff,
# MAGIC   CASE WHEN prev_cnt  > 0 THEN ROUND((cnt  - prev_cnt)  / prev_cnt, 4) END AS yoy_cnt_pct_change,
# MAGIC   prev_share,
# MAGIC   ROUND(share - prev_share, 4) AS yoy_share_point_diff,
# MAGIC   CASE WHEN prev_share > 0 THEN ROUND((share - prev_share) / prev_share, 4) END AS yoy_share_pct_change
# MAGIC FROM final;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pets.gold.v_breed_stats

# COMMAND ----------

# DBTITLE 1,Top 3 breeds by FSA
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW pets.gold.v_fsa_top3_breeds AS
# MAGIC WITH src AS (
# MAGIC   SELECT
# MAGIC     Year,
# MAGIC     ANIMAL_TYPE,
# MAGIC     FSA,
# MAGIC     breed_standard AS breed
# MAGIC   FROM pets.gold.licensed_pets_gold_src
# MAGIC   WHERE FSA IS NOT NULL  
# MAGIC ),
# MAGIC counts AS (
# MAGIC   SELECT
# MAGIC     Year,
# MAGIC     ANIMAL_TYPE,
# MAGIC     FSA,
# MAGIC     breed,
# MAGIC     COUNT(*) AS cnt
# MAGIC   FROM src
# MAGIC   GROUP BY Year, ANIMAL_TYPE, FSA, breed
# MAGIC ),
# MAGIC totals AS (
# MAGIC   SELECT
# MAGIC     Year,
# MAGIC     ANIMAL_TYPE,
# MAGIC     FSA,
# MAGIC     COUNT(*) AS total
# MAGIC   FROM src
# MAGIC   GROUP BY Year, ANIMAL_TYPE, FSA
# MAGIC ),
# MAGIC ranked AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY Year, ANIMAL_TYPE, FSA ORDER BY cnt DESC, breed
# MAGIC     ) AS rnk
# MAGIC   FROM counts
# MAGIC ),
# MAGIC top3 AS (
# MAGIC   SELECT *
# MAGIC   FROM ranked
# MAGIC   WHERE rnk <= 3
# MAGIC )
# MAGIC SELECT
# MAGIC   t.Year,
# MAGIC   t.ANIMAL_TYPE AS Animal_Type,
# MAGIC   t.FSA,
# MAGIC   tot.total,
# MAGIC   MAX(CASE WHEN t.rnk = 1 THEN t.breed END) AS top1_breed,
# MAGIC   MAX(CASE WHEN t.rnk = 1 THEN t.cnt   END) AS top1_cnt,
# MAGIC   MAX(CASE WHEN t.rnk = 2 THEN t.breed END) AS top2_breed,
# MAGIC   MAX(CASE WHEN t.rnk = 2 THEN t.cnt   END) AS top2_cnt,
# MAGIC   MAX(CASE WHEN t.rnk = 3 THEN t.breed END) AS top3_breed,
# MAGIC   MAX(CASE WHEN t.rnk = 3 THEN t.cnt   END) AS top3_cnt
# MAGIC FROM top3 t
# MAGIC JOIN totals tot
# MAGIC   ON t.Year = tot.Year
# MAGIC   AND t.ANIMAL_TYPE = tot.ANIMAL_TYPE
# MAGIC   AND t.FSA = tot.FSA
# MAGIC GROUP BY 
# MAGIC   t.Year, t.ANIMAL_TYPE, t.FSA, tot.total;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pets.gold.v_fsa_top3_breeds

# COMMAND ----------

# DBTITLE 1,Top 3 breeds by FSA2
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW pets.gold.v_fsa2_top3_breeds AS
# MAGIC WITH src AS (
# MAGIC   SELECT
# MAGIC     Year,
# MAGIC     ANIMAL_TYPE,
# MAGIC     substr(FSA, 1, 2) AS FSA2,
# MAGIC     breed_standard AS breed
# MAGIC   FROM pets.gold.licensed_pets_gold_src
# MAGIC   WHERE FSA IS NOT NULL AND length(FSA) >= 2  
# MAGIC ),
# MAGIC counts AS (
# MAGIC   SELECT
# MAGIC     Year,
# MAGIC     ANIMAL_TYPE,
# MAGIC     FSA2,
# MAGIC     breed,
# MAGIC     COUNT(*) AS cnt
# MAGIC   FROM src
# MAGIC   GROUP BY Year, ANIMAL_TYPE, FSA2, breed
# MAGIC ),
# MAGIC totals AS (
# MAGIC   SELECT
# MAGIC     Year,
# MAGIC     ANIMAL_TYPE,
# MAGIC     FSA2,
# MAGIC     COUNT(*) AS total
# MAGIC   FROM src
# MAGIC   GROUP BY Year, ANIMAL_TYPE, FSA2
# MAGIC ),
# MAGIC ranked AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY Year, ANIMAL_TYPE, FSA2 ORDER BY cnt DESC, breed
# MAGIC     ) AS rnk
# MAGIC   FROM counts
# MAGIC ),
# MAGIC top3 AS (
# MAGIC   SELECT *
# MAGIC   FROM ranked
# MAGIC   WHERE rnk <= 3
# MAGIC )
# MAGIC SELECT
# MAGIC   t.Year,
# MAGIC   t.ANIMAL_TYPE AS Animal_Type,
# MAGIC   t.FSA2,
# MAGIC   tot.total,
# MAGIC   MAX(CASE WHEN t.rnk = 1 THEN t.breed END) AS top1_breed,
# MAGIC   MAX(CASE WHEN t.rnk = 1 THEN t.cnt   END) AS top1_cnt,
# MAGIC   MAX(CASE WHEN t.rnk = 2 THEN t.breed END) AS top2_breed,
# MAGIC   MAX(CASE WHEN t.rnk = 2 THEN t.cnt   END) AS top2_cnt,
# MAGIC   MAX(CASE WHEN t.rnk = 3 THEN t.breed END) AS top3_breed,
# MAGIC   MAX(CASE WHEN t.rnk = 3 THEN t.cnt   END) AS top3_cnt
# MAGIC FROM top3 t
# MAGIC JOIN totals tot
# MAGIC   ON t.Year = tot.Year
# MAGIC   AND t.ANIMAL_TYPE = tot.ANIMAL_TYPE
# MAGIC   AND t.FSA2 = tot.FSA2
# MAGIC GROUP BY 
# MAGIC   t.Year, t.ANIMAL_TYPE, t.FSA2, tot.total

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pets.gold.v_fsa2_top3_breeds

# COMMAND ----------

# DBTITLE 1,Gold Quality View
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW pets.gold.licensed_pets_gold_quality AS
# MAGIC WITH a AS (
# MAGIC   SELECT
# MAGIC     Year,
# MAGIC     ANIMAL_TYPE,
# MAGIC     COUNT(*) AS rows,
# MAGIC     SUM(CASE WHEN breed_mapped THEN 1 ELSE 0 END) AS mapped_rows,
# MAGIC     SUM(CASE WHEN FSA IS NULL THEN 1 ELSE 0 END) AS null_fsa_rows,
# MAGIC     MAX(processed_ts) AS last_processed_ts
# MAGIC   FROM pets.core.licensed_pets_silver
# MAGIC   GROUP BY Year, ANIMAL_TYPE
# MAGIC )
# MAGIC SELECT
# MAGIC   Year,
# MAGIC   ANIMAL_TYPE,
# MAGIC   rows,
# MAGIC   mapped_rows,
# MAGIC   CASE WHEN rows = 0 THEN NULL ELSE CAST(mapped_rows AS DOUBLE) / rows END AS pct_mapped,
# MAGIC   null_fsa_rows,
# MAGIC   last_processed_ts
# MAGIC FROM a;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM pets.gold.licensed_pets_gold_quality
# MAGIC ORDER BY Year DESC, ANIMAL_TYPE;

# COMMAND ----------

# DBTITLE 1,Daily Totals
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW pets.gold.v_daily_totals AS
# MAGIC SELECT
# MAGIC   date(ingestion_ts) AS day,
# MAGIC   ANIMAL_TYPE,
# MAGIC   COUNT(*) AS total
# MAGIC FROM pets.gold.licensed_pets_gold_src
# MAGIC GROUP BY day, ANIMAL_TYPE ORDER BY day;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pets.gold.v_daily_totals

# COMMAND ----------

# DBTITLE 1,Citywide breed share
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW pets.gold.v_breed_share_citywide AS
# MAGIC WITH c AS (
# MAGIC   SELECT Year, ANIMAL_TYPE, breed_standard AS breed, COUNT(*) cnt
# MAGIC   FROM pets.gold.licensed_pets_gold_src
# MAGIC   GROUP BY Year, ANIMAL_TYPE, breed_standard
# MAGIC ),
# MAGIC t AS (
# MAGIC   SELECT Year, ANIMAL_TYPE, SUM(cnt) tot
# MAGIC   FROM c GROUP BY Year, ANIMAL_TYPE
# MAGIC )
# MAGIC SELECT c.Year, c.ANIMAL_TYPE, c.breed, c.cnt, ROUND(c.cnt / t.tot, 4) AS share
# MAGIC FROM c JOIN t
# MAGIC ON c.Year = t.Year AND c.ANIMAL_TYPE = t.ANIMAL_TYPE;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pets.gold.v_breed_share_citywide ORDER BY share DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW pets.gold.v_breed_rank_citywide AS
# MAGIC WITH c AS (
# MAGIC   SELECT Year, ANIMAL_TYPE, breed_standard AS breed, COUNT(*) cnt
# MAGIC   FROM pets.gold.licensed_pets_gold_src
# MAGIC   GROUP BY Year, ANIMAL_TYPE, breed_standard
# MAGIC )
# MAGIC SELECT
# MAGIC   Year, ANIMAL_TYPE, breed, cnt,
# MAGIC   ROW_NUMBER() OVER (PARTITION BY Year, ANIMAL_TYPE ORDER BY cnt DESC, breed) AS rnk
# MAGIC FROM c;
# MAGIC

# COMMAND ----------

# DBTITLE 1,citywide breed rank
# MAGIC %sql
# MAGIC SELECT * FROM pets.gold.v_breed_rank_citywide