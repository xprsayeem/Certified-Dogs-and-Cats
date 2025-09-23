# Bronze snapshot: data checks and counts

Date of run: 2025-09-22  
Source: City of Toronto Licensed Cats and Dogs

## Row counts
- Total rows loaded: **173,937**
- Rows with missing or invalid FSA: **301**  
- Valid FSA share: ~**99.83%**

## Integrity and quality checks
- `_id` is non null and de-duplicated
- `ANIMAL_TYPE` is only in {DOG, CAT}
- Partition layout created as expected: `Year=2023`, `Year=2024`, `Year=2025` with `ANIMAL_TYPE` subfolders

## Handy queries
```sql
-- row count
SELECT COUNT(*) AS total_rows FROM pets.core.licensed_pets_bronze;

-- FSA validity distribution
SELECT FSA_VALID, COUNT(*) FROM pets.core.licensed_pets_bronze GROUP BY FSA_VALID;

-- duplicates by id
SELECT _id, COUNT(*) c
FROM pets.core.licensed_pets_bronze
GROUP BY _id HAVING c > 1;

-- counts by year and type
SELECT Year, ANIMAL_TYPE, COUNT(*) AS petcount
FROM pets.core.licensed_pets_bronze
GROUP BY Year, ANIMAL_TYPE
ORDER BY Year, ANIMAL_TYPE;
```

## Notes
Keeping everything in Bronze close to source. For example, Spelling and synonym cleanup for PRIMARY_BREED will be handled in Silver.