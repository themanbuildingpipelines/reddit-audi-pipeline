/* 
This stored procedure loads data into the silver schema from the silver base table

Parameters: None
This stored procedure does not accept any parameters or return any values.

Usage Example: EXEC Silver.enriched_load_silver
*/

CREATE OR ALTER PROCEDURE Silver.enriched_load_silver AS
BEGIN 
DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        --Truncate table
        TRUNCATE TABLE Silver.audi_data_enriched;

        --Insert into table
        INSERT INTO Silver.audi_data_enriched (
            id,
            author,
            num_of_comments,
            url,
            user_issue,
            issue_raise_date,
            issue_category
        )
        SELECT
            id,
            CASE WHEN author = '[Deleted]' THEN 'n/a'
                ELSE author
            END AS username,
            num_comments,
            url,
            issue,
            created_datetime,
            categories 
        FROM (
        SELECT
        *,
        DENSE_RANK() OVER(PARTITION BY issue ORDER BY id) AS rank_issue
        FROM Silver.audi_data_base)t
        WHERE rank_issue = 1
        SET @batch_end_time = GETDATE();
	    PRINT 'Batch Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' ' + 'seconds';
    END TRY

    BEGIN CATCH
	  PRINT '==========================================================';
	  PRINT 'ERROR OCCURED DURING LOADING THIS LAYER';
	  PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
	  PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
	  PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================================';
	END CATCH
END
