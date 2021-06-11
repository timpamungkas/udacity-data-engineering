# Udacity Data Engineering - PostgreSQL Data Modelling
This is the solution for project PostgreSQL data modelling.
When run, this will read files from data folders, create database tables, and fetch the files content into respective tables.
The main scripts are are:

  - `sql_queries.py` : contains all of the sql statements used to create database structure, and inserting / updating data
  - `create_tables.py` : contains steps to create database schema, which the DDL will refers to `sql_queries.py`
  - `etl.py` : read data on `data` folders (and subfolders), fetch the json files in it, transform the data into matching structure and columns, and insert data into database. This is dependent to `sql_queries.py` and `create_tables.py`
    
The .ipynb files is just for trial-error, not critical file.
  
## Run the scripts
From terminal

```
  #> python create_tables.py
  #> python etl.py
```