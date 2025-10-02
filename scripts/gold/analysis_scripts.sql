/* ============================================================
   View: Gold.top_issues
============================================================ */
CREATE OR ALTER VIEW Gold.top_issues AS
WITH split_issues AS (
    SELECT 
        user_id AS user_id,
        LTRIM(RTRIM(value)) AS issue
    FROM Gold.audi_data_categorized
    CROSS APPLY STRING_SPLIT(issue_category, ',')
)
SELECT 
    issue,
    COUNT(*) AS mentions
FROM split_issues
WHERE issue IS NOT NULL AND issue <> ''
GROUP BY issue;
GO

/* ============================================================
   View: Gold.top_models
============================================================ */
CREATE OR ALTER VIEW Gold.top_models AS
SELECT 
    car_model,
    COUNT(*) AS mentions
FROM Gold.audi_data_categorized
WHERE car_model IS NOT NULL
GROUP BY car_model;
GO

/* ============================================================
   View: Gold.trending_issues
============================================================ */
CREATE OR ALTER VIEW Gold.trending_issues AS
WITH split_issues AS (
    SELECT 
        CAST(issue_raise_date AS DATE) AS issue_date,
        LTRIM(RTRIM(value)) AS issue
    FROM Gold.audi_data_categorized
    CROSS APPLY STRING_SPLIT(issue_category, ',')
    WHERE issue_raise_date IS NOT NULL
)
SELECT 
    issue_date,
    issue,
    COUNT(*) AS mentions
FROM split_issues
WHERE issue IS NOT NULL AND issue <> ''
GROUP BY issue_date, issue;
GO

/* ============================================================
   View: Gold.engagement_per_issue
============================================================ */
CREATE OR ALTER VIEW Gold.engagement_per_issue AS
WITH split_issues AS (
    SELECT 
        LTRIM(RTRIM(value)) AS issue,
        num_of_comments
    FROM Gold.audi_data_categorized
    CROSS APPLY STRING_SPLIT(issue_category, ',')
    WHERE issue_category IS NOT NULL
)
SELECT 
    issue,
    AVG(num_of_comments) AS avg_comments,
    SUM(num_of_comments) AS total_comments,
    COUNT(*) AS total_mentions
FROM split_issues
WHERE issue IS NOT NULL AND issue <> ''
GROUP BY issue;
GO

/* ============================================================
   View: Gold.issue_split_summary
============================================================ */
CREATE OR ALTER VIEW Gold.issue_split_summary AS
WITH split_issues AS (
    SELECT
        user_id AS user_id,
        LTRIM(RTRIM(value)) AS issue
    FROM Gold.audi_data_categorized
    CROSS APPLY STRING_SPLIT(issue_category, ',')
)
SELECT
    issue,
    COUNT(DISTINCT user_id) AS customers_with_issue
FROM split_issues
WHERE issue IS NOT NULL AND issue <> ''
GROUP BY issue;
GO
