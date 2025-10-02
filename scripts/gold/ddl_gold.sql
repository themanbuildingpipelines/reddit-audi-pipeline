/* This script creates two view tables;
1. audi_data_uncategoried - where issued raised by audi owners have been uncategorized.
Goal is for ML Engineers to make meaning of them and then we can add them back to 
our categoried table.

2. audi_data_categorized - here the data is available for analysts to make meaning of
and present to stakeholders. Which in this case, might be marketing teams, 
competitors of Audi cars, etc.
*/

--Gold view for uncategprized data for ML Engineers and Data Scientists to make meaning of

--Drop if exits then recreate
IF OBJECT_ID('Gold.audi_data_uncategorized', 'V') IS NOT NULL
    DROP VIEW Gold.audi_data_uncategorized;
GO
--recreating
CREATE VIEW Gold.audi_data_uncategorized AS 
SELECT
    id AS user_id,
    author AS username,
    num_of_comments,
    url AS source_url,
    user_issue,
    issue_raise_date,
    issue_category,
    car_model,
    year_model,
    mileage 
FROM Silver.audi_data_enriched
WHERE issue_category = 'Uncategorized';
GO

--Gold view for categorized data for Data Analysts/BI Engineers to make meaning of
IF OBJECT_ID('Gold.audi_data_categorized', 'V') IS NOT NULL
    DROP VIEW Gold.audi_data_categorized;
GO

--recreating
CREATE VIEW Gold.audi_data_categorized AS 
SELECT
    id AS user_id,
    author AS username,
    num_of_comments,
    url AS source_url,
    user_issue,
    issue_raise_date,
    issue_category,
    car_model,
    year_model,
    mileage  
FROM Silver.audi_data_enriched
WHERE issue_category != 'Uncategorized';
GO
