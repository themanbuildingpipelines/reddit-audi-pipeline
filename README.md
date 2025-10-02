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
 
3. **Bronze Raw** - The JSON file from the Landing Layer is transformed to CSV in this section and uploaded to Azure SQL through ADF.

5. **Silver Base** - The now converted (but limited CSV file) is joined with the source of truth (from the landing area) to make it more meaningful for analysis, which comes later. While at it, I remove duplicates from the now joined tables and store them in my Silver schema.

7. **Silver Enriched** - Here, more data cleaning is done, dropping unwanted columns, categorizing text data, converting BIGINT to datetime where necessary, and renaming columns.

9. **Gold Curated** - Since this dataset isn't transactional, I thought there's no need to classify the tables here as facts or dims. However, I have two tables. Categorized and uncategorized. The categorized consist of text data that has been categorized, think of engine issues, HVAC, brake issues, suspension issues. The latter is uncategorized, with customer sentiments not easily understood, so machine analysis might need to take place here to make sense of the issues. I'll probably handle this in the future when I get my hands dirty with machine learning.

## Visualization
I was to use Power BI for visualization, but Tableau just worked way better for me than the former. Still have a lot to learn about visualization, but hey, if I keep waiting for the right time, this project might never get finished. So, I work with what I can and improve on what I have already published, as I continue to acquire more knowledge.

## Top issues
<img width="717" height="278" alt="Top issues" src="https://github.com/user-attachments/assets/8fd535ab-4a2d-4e6b-a6f0-87561e001853" />

The chart above shows the most mentioned Audi issues in the subreddits I pulled data from. Issues range from engine to emission control to HVAC. 
The chart above shows the most mentioned Audi issues in the subreddits I pulled data from. To make sense of all the different ways people describe problems, I grouped related terms into broader categories. For example, anything like fuel pump, injectors, or intake manifold falls under **Air and Fuel Delivery**, while brake pads, rotors, or ABS are grouped under **Brakes**. Mods like tuning, ECU, or turbo upgrades go into Mods, and parts like bumpers, grilles, or paint are categorized as **Body**.

Overall, I organized the issues into system-level categories like **Engine, Exhaust, Cooling, Suspension, Transmission, Electrical & Lighting, Emission Control, HVAC, Hybrid/Electric, Tires & Wheels, Tools & Equipment, and Audi Accessories**. This makes it easier to see which areas Audi owners talk about the most.

