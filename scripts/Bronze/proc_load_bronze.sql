/* 
This stored procedure loads data into the ''bronze schema from external CSV files

It performs the following functions:
-Truncates the bronze audi table in the bronze before loading the data
-Uses the INSERT INTO command to load the CSV Files to Bronze Tables

Parameters: None
This stored procedure does not accept any parameters or return any values.

Usage Example: EXEC Bronze.load_bronze
*/

CREATE OR ALTER PROCEDURE Bronze.load_bronze AS
BEGIN
--Create a start time and end time operator to calculate load times for the table and the entire bronze layer
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	--Try
	BEGIN TRY
		
		SET @batch_start_time = GETDATE();
		PRINT '==============================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==============================================================';

		--Make the table empty
		PRINT '>>Truncating the audi raw data table';
		TRUNCATE TABLE Bronze.audi_rdata;
        --loading the table
        INSERT INTO Bronze.audi_rdata (
            id,
            subreddit,
            title,
            selftext,
            author,
            created_utc,
            num_comments,
            upvote_ratio,
            link_flair_text,
            permalink,
            url,
            subreddit_subscribers,
            source
        )
        SELECT
            TRY_CAST(id AS NVARCHAR) AS id,
            subreddit,
            title,
            selftext,
            author,
            TRY_CAST(created_utc AS BIGINT) AS created_utc,
            TRY_CAST(num_comments AS NVARCHAR(100)) AS num_comments,
            TRY_CAST(upvote_ratio AS NVARCHAR(100)) AS upvote_ratio,
            link_flair_text,
            permalink,
            url,
            TRY_CAST(subreddit_subscribers AS NVARCHAR(100)) AS subreddit_subscribers,
            source
        FROM dbo.reddit_posts
        WHERE TRY_CAST(id AS NVARCHAR) IS NOT NULL
          
  --get the entire batch load time
	PRINT '=============================================================================================================';
	PRINT 'Loading Bronze Layer is Complete';
	SET @batch_end_time = GETDATE();
	PRINT 'Batch Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' ' + 'seconds';
	PRINT '=============================================================================================================';
	END TRY

  BEGIN CATCH
	  PRINT '==========================================================';
	  PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
	  PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
	  PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
	  PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================================';
	END CATCH
END
