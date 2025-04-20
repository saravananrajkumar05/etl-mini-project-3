# ETL Pipeline with AWS
This project performs a basic ETL (Extract, Transform, Load) process using AWS S3 and AWS RDS.

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
