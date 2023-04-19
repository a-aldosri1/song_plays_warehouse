# Project: Song Plays Data Warehouse

### Introduction
A music streaming startup, Sparkify, has grown its user base and song database and wants to analyze its data on a large scale. The current data structure consists of JSON files residing in an S3 bucket. The goal of this project is to transform the JSON files into a Redshift warehouse using a star schema model.


### Building The ETL Pipeline
We build an ETL to extract the two datasets 
1. Song data residing at: s3://2. 2. udacity-dend/song_data

2. Log data residing at: s3://udacity-dend/log_data

into staging tables at redshift using the copy command in redshift to read from s3 bucket, then we transformed the staging tables into the following star schema:

![song plays schema](https://github.com/a-aldosri1/song_plays_warehouse/main/Song_Plays_Star_schema.jpeg)

as you can see we divided the tables into one fact table: `song_plays_fact`, and 4 dimension tables: `time_dim`, `users_dim`, `artists_dim`, and `songs_dim`.
We choose `song_id` to be the distribution key, because we assume that it is the most column we will aggregate on and order by.


### How to run
There is multiple prerequisites to run the ETL which is:
1. Create an aws account 
2. Create an IAM user 
3. Retrieve the access keys.
4. Create an IAM role and attach `AmazonS3ReadOnlyAccess` policy to it
5. Create a redshift cluster with the previous IAM role, preferred ` dc2.large` with 4 nodes.
6. Retrieve the IAM role arn, and the cluster endpoint.
7. Store the access keys, IAM role arn, and the cluster endpoint in `dwh.cfg` file
8. Install python 3.6
9. Create a python virtual environment and install the pachages in ` requirements.txt` with the following command ` pip install -r requirements.txt`

Now you are ready to run the ETL, run ` create_tables` to create the tables or dropping them and recreating them.

Run ` etl.py` to copy the data from S3 bucket into the stagging tables, and loading the data into the main tables.
