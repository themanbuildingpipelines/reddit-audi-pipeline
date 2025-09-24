--SQL DDL scripts for Bronze layer

--Drop the table if it already exists
IF OBJECT_ID('Bronze.audi_rdata', 'U') IS NOT NULL
    DROP TABLE Bronze.audi_rdata;

--create bronze table
CREATE TABLE Bronze.audi_rdata (
    id NVARCHAR(100),
    subreddit NVARCHAR(100),
    title NVARCHAR(MAX),
    selftext NVARCHAR(MAX),
    author NVARCHAR(100),
    created_utc BIGINT,
    num_comments NVARCHAR(100),
    upvote_ratio NVARCHAR(100),
    link_flair_text NVARCHAR(100),
    permalink NVARCHAR(100),
    url NVARCHAR(500),
    subreddit_subscribers NVARCHAR(100),
    source NVARCHAR(100)
)
