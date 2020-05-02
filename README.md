# Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.
Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They require an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

The project requires loading of the data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

#### Data is a sample from [here](http://millionsongdataset.com/pages/field-list/). The data is available at : 

* Song data: s3://udacity-dend/song_data
* Log data: s3://udacity-dend/log_data

# State and justify your database schema design and ETL pipeline.
## Schema for Song Play Analysis
Star schema optimized for queries on song play analysis. The tables are optimize to leverage the highly parallel nature of Amazon Redshift by defining Redshift Distribution Keys (Redshift DIST Keys).

This includes the following tables.

### Fact Table
* songplays - records in event data associated with song plays i.e. records with page NextSong
** songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
### Dimension Tables
* users - users in the app
** user_id, first_name, last_name, gender, level
* songs - songs in music database
** song_id, title, artist_id, year, duration
* artists - artists in music database
** artist_id, name, location, lattitude, longitude
* time - timestamps of records in songplays broken down into specific units
** start_time, hour, day, week, month, year, weekday

#### Project Template
The project template includes four files:

* create_table.py is where you'll create your fact and dimension tables for the star schema in Redshift.
* etl.py is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
* sql_queries.py is where you'll define you SQL statements, which will be imported into the two other files above.
* README.md is where you'll provide discussion on your process and decisions for this ETL pipeline.

#### Project Steps
Below are steps you can follow to complete each component of this project.

* Create Table Schemas
** Design schemas for your fact and dimension tables
** Write a SQL CREATE statement for each of these tables in sql_queries.py
** Complete the logic in create_tables.py to connect to the database and create these tables
** Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist. This way, you can run create_tables.py whenever you want to reset your database and test your ETL pipeline.
** Launch a redshift cluster and create an IAM role that has read access to S3.
## Run the notebook from lesson 2
** Add redshift database and IAM role info to dwh.cfg.
** Test by running create_tables.py and checking the table schemas in your redshift database. You can use Query Editor in the AWS Redshift console for this.

* Build ETL Pipeline
** Implement the logic in etl.py to load data from S3 to staging tables on Redshift.
** Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.
** Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.

### The rows are limited to 100 for testing purposes.

* Delete your redshift cluster when finished.

## References
* https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_examples.html
* https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html
* Exercise 2: Creating Redshift Cluster using the AWS python SDK
* Exercise 4: Optimizing Redshift Table Design
* https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift
* https://docs.aws.amazon.com/redshift/latest/dg/r_EXTRACT_function.html
* https://docs.aws.amazon.com/redshift/latest/dg/r_Dateparts_for_datetime_functions.html