# Udacity Project 4 - Data Lake
Task for creating ETL pipeline to data lake using Spark. The input / output will be placed on AWS s3. Input is JSON and output is parquet files. 
ETL will :
  1. load song and log data in JSON format from S3.
  2. processes the data into analytics tables. 
  3. writes output into parquet files on S3. 

## Files Involved
Files on project
  - `etl.py` : main ETL file to read, process, and write the output to S3
  - `dl.cfg` : AWS credentials 

## Schema
Fact table:
  - `songplays` - log of user song plays

Dimension tables:
  - `users`
  - `songs`
  - `artists`
  - `time`

## How To Use
Add file `dl.cfg` with content:
```
[AWS]
AWS_ACCESS_KEY_ID=your_aws_access_key_id
AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
```

To execute the ETL pipeline, open terminal and enter `python etl.py`