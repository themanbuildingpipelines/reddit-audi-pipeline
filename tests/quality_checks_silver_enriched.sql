--This tests are for the Silver.audi_data_enriched
/* --tests
1. check for nulls in id
2. check for duplicates in id
3. Trim spaces in id column
4. check for NULLs or [deleted] in author column, for ones deleted return NULLs
5. check for NULLs in URL
6. check for duplicates in issue, delete duplicates
7. check for created datetime inconsistencies
8. Remove rows with uncategorized and store them in a seperate table
*/

--1. check for nulls in id, expectation 0
SELECT id FROM Silver.audi_data_enriched
WHERE id IS NULL

--2. check for duplicates in id, expectation is 0
SELECT COUNT(id) FROM Silver.audi_data_enriched
GROUP BY id
HAVING COUNT(id) > 1

--3. Trim spaces in id column, first check if there are any spaces in the first place, expectation is 0
SELECT id FROM Silver.audi_data_enriched
WHERE id != TRIM(id)

--comment: Every expectation has been met

--4. check for NULLs or [deleted] in author column, for ones deleted return NULLs
SELECT author FROM Silver.audi_data_enriched
WHERE author IS NULL OR author = '[Deleted]'

--Comment: around 180 author names comes back as [Deleted], sort before loading into new tables

--5. check for NULLs in URL, expectatio is 0
SELECT url FROM Silver.audi_data_enriched
WHERE url IS NULL

--Commment: expectation is met

--6. check for duplicates in issue, delete duplicates
SELECT
*
FROM (
SELECT
*,
DENSE_RANK() OVER(PARTITION BY user_issue ORDER BY id) AS rank_issue
FROM Silver.audi_data_enriched)t
WHERE rank_issue > 1

--Comment: expectation met

-- 7. check for created datetime inconsistencies, expectations none

SELECT issue_raise_date FROM Silver.audi_data_enriched
WHERE issue_raise_date IS NULL OR issue_raise_date < '1990-01-01' OR issue_raise_date > GETDATE()

--Comment: met expectations
