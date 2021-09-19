# Airflow Data Pipeline
## Overview

This is data pipeline using Apache Airflow that automates and monitors the ETL pipeline.
  
The pipeline will load song and log data in JSON from S3 ten processes the data into analytics tables in a fact-dim table on redshift. Airflow schedules this ETL and monitors its status.

## Structure

* `udac_example_dag.py` contains the tasks and dependencies of the DAG. Place it under `dag` subfolder on airflow
* `create_tables.sql` contains the SQL queries to create all the required tables in Redshift. Run this file on Redshift prior running DAG.
* `sql_queries.py` contains the SQL queries used. Place it under `plugins/helpers` directory of airflow.
* `stage_redshift.py` contains `StageToRedshiftOperator`, to copy JSON data from S3 to staging tables in Redshift. Place it under `plugins/operators` directory of airflow.
* `load_dimension.py` contains `LoadDimensionOperator`, to load dimension table from data in the staging area. Place it under `plugins/operators` directory of airflow.
* `load_fact.py` contains `LoadFactOperator`, to load fact table from data in the staging area. Place it under `plugins/operators` directory of airflow.
* `data_quality.py` contains `DataQualityOperator`, to do data quality check by passing an SQL query and expected result as arguments. Place it under `plugins/operators` directory of airflow.

## Configuration

* Add the following airflow connections:
    * AWS credentials (name : `aws_credentials`)
    * Connection to Postgres database (name : `redshift`)
* Make sure tables already exists on redshift. Run the create_tables.sql from Redshift before runs the airflow DAG>