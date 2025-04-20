import glob
import boto3
import pandas as pd
import xml.etree.ElementTree as xml
import logging
from datetime import datetime
from sqlalchemy import create_engine


#Initializing the Dataframes
data_df = pd.DataFrame()

bucket_name = "my-etl-project-bucket-1"
s3_path = ""

def upload_files_to_s3(files):
    
    """
        Uploading all the files into the S3-Bucket
    """

    logging.info("Uploading the File to S3 is started")

    s3 = boto3.client('s3')

    for file in files:
        file_name = file.split("/")[-1]
        s3_path = f"{file_name}"
        logging.info(f"Uploading {file} to s3://{bucket_name}/{s3_path}")
        s3.upload_file(file, bucket_name,s3_path)

    logging.info("Files Uploaded Successfully to S3 Path")
    

def download_files_from_s3(files):

    """ Downloading Files from S3 """

    logging.info("Downloading Files from S3 is started")

    s3 = boto3.client('s3')
    
    for file in files:
        file_name = file.split("/")[-1]
        local_path = f"download_sources/{file_name}"
        try :
            s3.download_file(bucket_name,file_name,local_path)
            logging.info(f"Downloaded {file_name} to {local_path}")
        except Exception as e :
            logging.error(f"Failed to download {file_name} : {e}")


def extract_data(downloaded_sources):
    """ Extracting the data from the files """
    global data_df

    def extract_csv(file_name):
        """ Function to Extract the data from CSV """
        try:
            logging.info(f"Extracting CSV file {file_name} is started")
            temp_df = pd.read_csv(file_name)
        except Exception as e:
            logging.error(f"Failed to extract data from {file_name} due to {e}")
            return None
        else:
            logging.info(f"Extraction Completed for {file_name}")
            return temp_df

    def extract_json(file_name):
        """ Function to extract the data from JSON """
        try:
            logging.info(f"Extracting JSON file {file_name} is started")
            temp_df = pd.read_json(file_name, lines = True)
        except Exception as e:
            logging.error(f"Failed to extract data from {file_name} due to {e}")
            return None
        else:
            logging.info(f"Extraction Completed for {file_name}")
            return temp_df        

    def extract_xml(file_name):
        """ Function to extract the data from XML """
        try:
            logging.info(f"Extracting XML file {file_name} is started")
            data_list = []
            tree = xml.parse(file_name)
            root = tree.getroot()

            for person in root.findall('person'):
                name = person.find('name').text
                height = person.find('height').text
                weight = person.find('weight').text
                data_list.append([name, height, weight])

            temp_df = pd.DataFrame(data_list, columns=['name', 'height', 'weight'])
        except Exception as e:
            logging.error(f"Failed to extract data from {file_name} due to {e}")
            return None
        else:
            logging.info(f"Extraction Completed for {file_name}")
            return temp_df

    for file in downloaded_sources:
        if file.lower().endswith(".csv"):
            data_df = pd.concat([data_df, extract_csv(file)], ignore_index=True)
        elif file.lower().endswith(".json"):
            data_df = pd.concat([data_df, extract_json(file)], ignore_index=True)
        elif file.lower().endswith(".xml"):
            data_df = pd.concat([data_df, extract_xml(file)], ignore_index=True)


def transformation_data():
    
    """ Function to Transform the data"""

    global data_df 

    logging.info("Transformation Started:")

    # Convert columns to numeric
    data_df['height'] = pd.to_numeric(data_df['height'], errors='coerce')
    data_df['weight'] = pd.to_numeric(data_df['weight'], errors='coerce')

    # Perform unit conversions
    data_df['height'] = round(data_df['height'] * 0.0254, 2)  # inches to meters
    data_df['weight'] = round(data_df['weight'] * 0.45359237, 2)  # pounds to kilograms

    logging.info("Transformation Ended:")


def loading_clean_data_to_csv():

    """Function to Load the Transformed Data to CSV"""

    logging.info("Loading the transformed data in to CSV file for future use")

    data_df.to_csv("transformed_data.csv",index=False)

    logging.info("Succesfully Loaded the data into csv file")

#main program

#Logging Configuration
logging.basicConfig(filename='log_file.txt',  
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S' 
)

files = glob.glob("upload_sources/source*")
print(files)

logging.info(f"Files to be Uploaded : {files}")

upload_files_to_s3(files)
download_files_from_s3(files)

downloaded_sources = glob.glob("download_sources/source*")
print(downloaded_sources)

#Calling function to extract the data
extract_data(downloaded_sources)

#Calling function to transform the data
transformation_data()

#Calling function to load the clean data to CSV
loading_clean_data_to_csv()

#db Details
host = "host"
port = 3306
username = "username"
password = "password"
database = "etl_database"


logging.info("Loading the transformed data to database (RDS)")
engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}")

data_df.to_sql('person', con=engine, if_exists='append', index=False)
