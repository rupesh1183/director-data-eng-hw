# Design Approach, Assumptions and Considerations

## Design Considerations and Tooling

### Notes: Bash script has not been included to run the hive script. Redshift table DDLs and copy statements are not included. The logic to create metrics has been included.

### Tooling
1. Keep ETL processing completely out of Redshift. This allows for massive scalability without expensive redshift usage. 
2. Use AWS Glue for all ETL processing which allows for more robust validations and anomaly detection processes before copying data to Redshift.Also Athena is a great way for data anlysis on tables built and stood up by Hive/Presto pipelines.
3. Copy only the final target metrics tables to Redshift - The row count of these tables will be far less than indidvidual player level ratings tables and so will save disk space.

### Other Design Considerations : S3 partioning for Hive tables
1. Use External table which year/month/day partitions which allows for better performance tuning using partioned processing of data
2. Use managed tables for individual date partitions which can be dopped and recreated if process needs to re run for the day

### Method to tie interactions
1. Step 6 of the Hive script suggests of an approach to tie historical interactions (which have a player rating and a subject response) together by assigning a unique interaction id
2. Step 6 essentially allows for data to be partitioned using the unique_interaction_id and then complex rating types such as remove vs reject vs block can be assigned/inferred by ordering the records using timestamp
