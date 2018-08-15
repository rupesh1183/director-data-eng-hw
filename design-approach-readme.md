
## Design Approach, Assumptions and Considerations

### Notes: Bash script has not been included to run the hive script. Redshift table DDLs and copy statements are not included. The logic to create metrics has been included  (hive_script.hql).

### Tooling
1. Keep ETL processing completely out of Redshift. This allows for massive scalability without expensive redshift usage. 
2. Use AWS Glue even while using EMRs which allows for more robust validations and anomaly detection processes before copying data to Redshift. Also Athena is a great way for data anlysis on tables built and stood up by Hive/Presto pipelines.
3. Copy only the final target metrics tables to Redshift - The row count of these tables will be far less than indidvidual player level ratings tables and so will save disk space.

### Other Design Considerations : S3 partioning for Hive tables
1. Use External tables with year/month/day partitions which allows for better performance tuning using partioned processing of data.
2. Use managed tables for individual date partitions which can be dropped and recreated if process needs to re run for the day
3. Using s3 folders as tables also removes dependencies on file names as the Hive table is agnostic of file names.
4. It was **interesting** to note how the homework readme file suggested to create separate tables for each of the date part files on S3 - Remains to be seen as to why this was a requirement - nevertheless if we ever want to track which s3 file parts the ratings/interactions belonged too - this can be easily achieved by using the built in Hive column available table which is **INPUT__FILE__NAME** - This can be stored on the tables holding processed data to help store source file part name.

### Method to tie interactions
1. Step 6 of the Hive script suggests of an approach to tie historical interactions (which have a player rating and a subject response) together by assigning a unique interaction id
2. Step 6 essentially allows for data to be partitioned using the unique_interaction_id and then complex rating types such as remove vs reject vs block can be assigned/inferred by ordering the records using timestamp

### Logic to hold daily ratings metrics
1. Step 10 of the hive script proposes an approach to store various metrics at a daily time grain.
2. These daily metrics can be averaged each month for average metrics.

### Possible additional analysis
1. Table created in Step 6 which holds the most recent snapshot of all interactions (Ratings and response) between player and subject could be used to create a metric such as average_duration of an interaction - in other words what is the average duration of the association between a subject and player.
