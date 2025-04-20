# ETL Pipeline with AWS
This project performs a basic ETL (Extract, Transform, Load) process using AWS S3 and AWS RDS.

# Objectives

- `clean_data`: Clean and preprocess the data  
- `load_data`: Load cleaned data into a remote SQL database (AWS RDS - MySQL)  

Steps
1. Extract
    a) Upload raw CSV, JSON, and XML files to an S3 bucket.
    b) Download files from S3 for processing.

2. Transform
    a) Convert units:
        Inches ➡️ Meters
       Pounds ➡️ Kilograms
    b) Clean and standardize the data.

3. Load
    a) Save the transformed data as transformed_data.csv.
    b) Upload the file to a different S3 bucket.
    c) Load the data into an AWS RDS database using SQLAlchemy and pandas.

## Tech Stack

```yaml
language: Python
database: MySQL (AWS RDS)
etl-library: pandas, mysql-connector-python

steps:
  - step: Data Ingestion
    tool: pandas
    description: Load CSV, JSON, XML files data 

  - step: Data Cleaning
    tasks:
      - Converting Inched to Meters
      - Converting Pounds to Kilograms

  - step: Data Modeling & Upload
    description: Create SQL tables and insert data into AWS RDS using mysql-connector
