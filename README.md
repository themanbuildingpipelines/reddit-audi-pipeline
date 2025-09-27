# Reddit-audi-pipeline

## Why this project
![2020-audi-s5-sportback-311-1588268368](https://github.com/user-attachments/assets/c69ded4b-4009-469b-86ec-56050a4e29da)
I have been an Audi fan for years now. I picture myself speeding with my Audi S5 to wherever the road takes me. 
That aside, I'm in most of Audi subreddits and thought, why not create a project where I get to analyze
what Audi cars are most loves, common issues with Audis, mods that are popular in the community, etc.

## By the end of this project, I should be able to...
1. Track patterns in upvotes, comments, and posting activity (e.g., which models get the most community attention).
2. Identify common issues, popular modifications, and recurring themes in the Audi community.
3. Compare mentions and engagement across Audi A4, A6, Q5, S5, R8, etc.
4. Track patterns in upvotes, comments, and posting activity (e.g., which models get the most community attention).

## Data Architecture
<img width="1231" height="441" alt="Rakuten Data Pipeline High Level Architecture drawio" src="https://github.com/user-attachments/assets/da1e1920-04e1-4f19-9e0c-ccd38e7508a2" />
There are six layers in my architecture:
1. **Bronze Landing** - This layer consists of two files. A JSON file that is uploaded to Azure Blob storage (however, it is limited). And, a CSV file that has data spanning across several months that acts as my source of truth.
2. **Bronze Raw** - The JSON file from the Landing Layer is transformed to CSV in this section and uploaded to Azure SQL through ADF.
3. **Silver Base** - The now converted (but limited CSV file) is joined with the source of truth (from the landing area) to make it more meaningful for analysis, which comes later. While at it, I remove duplicates from the now joined tables and store them in my Silver schema.
4. **Silver Enriched** - Here, more data cleaning is done, dropping unwanted columns, categorizing text data, converting BIGINT to datetime where necessary, and renaming columns.
5. **Gold Curated** - Since this dataset isn't transactional, I thought there's no need to classify the tables here as facts or dims. However, I have two tables. Categorized and uncategorized. The categorized consist of text data that has been categorized, think of engine issues, HVAC, brake issues, suspension issues. The latter is uncategorized, with customer sentiments not easily understood, so machine analysis might need to take place here to make sense of the issues. I'll probably handle this in the future when I get my hands dirty with machine learning. 
   
