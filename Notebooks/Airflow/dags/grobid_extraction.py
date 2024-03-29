from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from tasks.grobid_client.grobid_client import GrobidClient
from pydantic import BaseModel, Field, ValidationError, validator
import boto3
import csv
from io import StringIO
from lxml import etree
import tempfile
import os
import re

import random
from urllib.parse import urlparse
from pathlib import Path

from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from sqlalchemy.engine import URL
import pandas as pd
import numpy as np

import os
import PyPDF2
import pandas as pd
import datetime
from datetime import date
import csv
import json
import numpy as np
import logging


from pydantic import ( 
    BaseModel,
    field_validator,
    ValidationError,
    ValidationInfo,
)
import re
from datetime import datetime
import datetime as dt


load_dotenv()
# Fetch AWS credentials from Airflow connection
aws_conn_id = 'my_aws_credentials'  # Corrected AWS connection ID
aws_credentials = BaseHook.get_connection(aws_conn_id)
aws_access_key_id = aws_credentials.login
aws_secret_access_key = aws_credentials.password

#Keys
XCOM_FOLDER_PATH='xcom_folder_path'
XCOM_FILE_PATH='xcom_file_path'
XCOM_PDF_FILE_PATH='xcom_pdf_file_path'
XCOM_TXT_FILE_PATH='xcom_txt_file_path'

# Set up boto3 client with Airflow connection credentials
s3 = boto3.client('s3',
                  aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 27),
    'email': ['shubhpatel2800@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('grobid_extraction',
          default_args=default_args,
          description='A DAG for processing PDFs with Grobid and storing results in S3',
          schedule_interval=None)

# client = GrobidClient(url="http://host.docker.internal:8070")  # Grobid server URL
GROBID_CONFIG_FILE_PATH = './dags/tasks/grobid_client/config.json'
client = GrobidClient(config_path=GROBID_CONFIG_FILE_PATH)

# def read_pdf_from_s3(bucket_name, file_key):
#     pdf_file = s3.get_object(Bucket=bucket_name, Key=file_key)
#     pdf_content = pdf_file['Body'].read()
#     return pdf_content

def _generate_random_string() -> str:
    return ''.join(random.choice('0123456789abcdef') for i in range(16))

def _get_path_from_url(url):
    parsed_url = urlparse(url)
    return parsed_url.path.lstrip('/')

def extract_text_from_pdf(**kwargs):
    Path('/tmp/processed_data').mkdir(parents=True, exist_ok=True)
    l1='https://assignment-4bigdata.s3.us-east-2.amazonaws.com/2024-l1-topics-combined-2.pdf'
    local_folder_name = _generate_random_string()
    local_folder_path = os.path.join('/tmp/processed_data', local_folder_name)
    local_file_path = os.path.join(local_folder_path, os.path.basename(l1))
    Path(local_folder_path).mkdir(parents=True, exist_ok=True)
    pdf_path = _get_path_from_url(l1)
    print(f'pdf path {pdf_path}')
    print(f'pdf local file path {local_file_path}')

    s3.download_file('assignment-4bigdata', pdf_path ,local_file_path)

    # Process the temporary file with GrobidClient
    client.process("processFulltextDocument",
                              local_folder_path,
                              output=local_folder_path,
                              consolidate_header=True,
                              consolidate_citations=True,
                              include_raw_affiliations=True)
    
    temp_xml_file_path = _find_xml_files(local_folder_path)
    ti = kwargs['ti']

    ti.xcom_push(key=XCOM_FILE_PATH, value=temp_xml_file_path)


    #ti.xcom_push(key=constants.XKEY_TEMP_FOLDER_NAME, value=local_folder_name)
    ti.xcom_push(key=XCOM_FOLDER_PATH, value=local_folder_path)
    ti.xcom_push(key=XCOM_PDF_FILE_PATH, value=l1)



# def upload_csv_to_s3(bucket_name, file_name, csv_content):
#     s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_content)

# def convert_to_csv_and_upload():
#     csv_output = StringIO()
#     writer = csv.writer(csv_output)
#     writer.writerow(['Heading', 'Paragraph'])  # Adjust columns as needed
#     for section in sections:
#         writer.writerow([section.heading, section.paragraph])
    
#     upload_csv_to_s3(bucket_name, file_name, csv_output.getvalue())


class DocumentSection(BaseModel):
    heading: str = ''  # Add a default value for the heading field
    paragraph: str = Field(...)

    @validator('paragraph')
    def paragraph_must_not_be_empty(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('Paragraph must not be empty')
        return v.strip()

def _find_xml_files(folder_path):
    xml_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.xml'):
                xml_files.append(os.path.join(root, file))
    return xml_files[0]

# Map categories to row index ranges for categorization purposes
combined_category_ranges = {
    # Each category is associated with one or more tuples of (start_index, end_index)
    # indicating the row indices that fall under that category
    'Quantitative Methods': [(1, 11), (88, 95)],
    'Economics': [(12, 19), (96, 98)],
    'Portfolio Management': [(20, 25), (137, 147)],
    'Corporate Issuers': [(26, 32), (106, 109)],
    'Financial Statement Analysis': [(33, 44), (99, 105)],
    'Equity Investments': [(45, 52), (154, 157)],
    'Fixed Income': [(53, 70), (116, 120), (150, 153)],
    'Derivatives': [(71, 80), (121, 122), (148, 149)],
    'Alternative Investments': [(81, 87), (123, 126), (158, 161)],
    'Equity Valuation': [(110, 115)],
    'Analysis of Active Portfolio': [(127, 132)],
    'Economies': [(133, 136)],

}

class RowModel(BaseModel):
    level: str 
    category: str  # A string to hold the category
    topic: str  # A string to hold the topic
    learningOutcomes: str  # A string to hold the learning outcomes

def find_category(row_index):
    # Determines the category of a given row index based on predefined ranges
    for category, ranges in combined_category_ranges.items():
        for start, end in ranges:
            if start <= row_index <= end:
                return category
    return "Unknown"  # Returns "Unknown" if no range matches the index

def parse_tei_xml_and_merge_paragraphs_to_csv(output_csv_path,xml_file_path):
    # Initializes counters for tracking validation successes and failures
    print(output_csv_path)
    print(xml_file_path)

    row_index = 1
    successful_validations = 0
    unsuccessful_validations = 0
    
    # Opens the output CSV file for writing
    with open(output_csv_path, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        # Writes the header row to the CSV file
        writer.writerow(['Level', 'Category', 'Topic', 'Learning Outcomes'])

        # Iterates over the XML files, parsing and processing each one
        
        tree = etree.parse(xml_file_path)  # Parses the XML file
        ns = {'tei': 'http://www.tei-c.org/ns/1.0'}  # Namespace definition for XPath

        def write_merged_paragraphs(sections, default_heading):
            # Helper function to process and write data for each section
            nonlocal row_index, successful_validations, unsuccessful_validations
            root=tree.getroot()
            div_elements = root.findall(".//")
            text=""
            for element in div_elements:
                if element.tag == "{http://www.tei-c.org/ns/1.0}p" or element.tag == "{http://www.tei-c.org/ns/1.0}head" :
                    text = text + "" + str(element.text)
            
            for section in sections:
                # Extracts the heading and paragraphs, merging paragraphs as needed
                heading = section.xpath('.//tei:head/text()', namespaces=ns)[0] if section.xpath('.//tei:head/text()', namespaces=ns) else default_heading
                paragraphs = section.xpath('.//tei:p/text()', namespaces=ns)
                merged_paragraphs = '\n'.join(paragraphs)
                if merged_paragraphs.strip() and "LEARNING OUTCOMES" not in heading.upper():
                    category = find_category(row_index)
                    # Attempts to validate the data against the Pydantic model before writing to CSV
                    matches =re.findall(r"Level (?:I|II|III)", text)
                    level = matches[0]
                    try:
                        valid_data = RowModel(level=level, category=category, topic=heading, learningOutcomes=merged_paragraphs)
                        writer.writerow([valid_data.level, valid_data.category, valid_data.topic, valid_data.learningOutcomes])
                        successful_validations += 1
                    except ValidationError as e:
                        unsuccessful_validations += 1
                        print(e)
                    row_index += 1

            # Processes abstract and content sections separately
        abstract_sections = tree.xpath('//tei:profileDesc/tei:abstract/tei:div', namespaces=ns)
        write_merged_paragraphs(abstract_sections, "Abstract")
        content_sections = tree.xpath('//tei:text/tei:body/tei:div', namespaces=ns)
        write_merged_paragraphs(content_sections, "Content")

    # Prints the counts of successful and unsuccessful validations
    print(f"Successful validations: {successful_validations}")
    print(f"Unsuccessful validations: {unsuccessful_validations}")

def xml_to_structured_text(**kwargs):
    ti = kwargs['ti']
    folder_path = ti.xcom_pull(key=XCOM_FOLDER_PATH, task_ids='extract_text')
    xml_file_path = ti.xcom_pull(key=XCOM_FILE_PATH, task_ids='extract_text')
    root = etree.parse(xml_file_path).getroot()
    #os.path.join(folder_path,'content_data.csv')
    parse_tei_xml_and_merge_paragraphs_to_csv(os.path.join(folder_path,'content_data.csv'),xml_file_path)


TABLE_NAME_CONTENT_DATA = 'grobid_content_data'
TABLE_NAME_META_DATA = 'meta_content_data'
DATABASE_NAME = 'pdfdata'
WAREHOUSE_NAME = 'compute_wh'


base_url = URL.create(
    "snowflake",
    username=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASS'),
    host=os.getenv('SNOWFLAKE_ACC_ID'),
)

# Creating database for storing cfa data
create_cfa_database_query = f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME};"

# Creating warehouse for the cfa databases
create_cfa_warehouse_query = f"""CREATE WAREHOUSE IF NOT EXISTS {WAREHOUSE_NAME} WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 180
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE; 
"""

def create_contentdata_table(connection):
    # Creating table for scraped data
    create_content_data_table_query = f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME_CONTENT_DATA} (
        level STRING,
        category STRING,
        topic STRING,
        learningOutcomes TEXT
    )
    """
    connection.execute(create_content_data_table_query)

def create_content_metadata_table(connection):
    # Creating table for scraped data
    create_meta_data_table_query = f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME_META_DATA} (
        file_size_bytes INTEGER,
        num_pages INTEGER,
        file_path STRING,
        encryption STRING,
        date_updated STRING
    )
    """
    connection.execute(create_meta_data_table_query)

def execute_ddl_queries(connection):
    connection.execute(create_cfa_warehouse_query)
    connection.execute(create_cfa_database_query)
    connection.execute(f'USE WAREHOUSE {WAREHOUSE_NAME};')
    connection.execute(f'USE DATABASE {DATABASE_NAME};')
    #create_webscraped_table(connection=connection)
    create_contentdata_table(connection=connection)
    create_content_metadata_table(connection=connection)


def upload_into_content_data_table(connection, temp_folder_path):
    data_file_name = 'content_data.csv'
    file_path = f'file://{temp_folder_path}/{data_file_name}'
    copy_into_content_table = f"""COPY INTO {DATABASE_NAME}.PUBLIC.{TABLE_NAME_CONTENT_DATA}
        FROM '@{DATABASE_NAME}.PUBLIC.%{TABLE_NAME_CONTENT_DATA}'
        FILES = ('{data_file_name}.gz')
        FILE_FORMAT = (
            TYPE=CSV,
            SKIP_HEADER=1,
            FIELD_DELIMITER=',',
            TRIM_SPACE=FALSE,
            FIELD_OPTIONALLY_ENCLOSED_BY='"',
            REPLACE_INVALID_CHARACTERS=TRUE,
            DATE_FORMAT=AUTO,
            TIME_FORMAT=AUTO,
            TIMESTAMP_FORMAT=AUTO
        )
        ON_ERROR=ABORT_STATEMENT
        PURGE=TRUE
    """
    connection.execute(f"PUT {file_path} @{DATABASE_NAME}.PUBLIC.%{TABLE_NAME_CONTENT_DATA};")
    connection.execute(copy_into_content_table)

def upload_into_metadata_table(connection, temp_folder_path):
    data_file_name = 'meta_data.csv'
    file_path = f'file://{temp_folder_path}/{data_file_name}'
    copy_into_metadata_table = f"""COPY INTO {DATABASE_NAME}.PUBLIC.{TABLE_NAME_META_DATA}
        FROM '@{DATABASE_NAME}.PUBLIC.%{TABLE_NAME_META_DATA}'
        FILES = ('{data_file_name}.gz')
        FILE_FORMAT = (
            TYPE=CSV,
            SKIP_HEADER=1,
            FIELD_DELIMITER=',',
            TRIM_SPACE=FALSE,
            FIELD_OPTIONALLY_ENCLOSED_BY='"',
            REPLACE_INVALID_CHARACTERS=TRUE,
            DATE_FORMAT=AUTO,
            TIME_FORMAT=AUTO,
            TIMESTAMP_FORMAT=AUTO
        )
        ON_ERROR=ABORT_STATEMENT
        PURGE=TRUE
    """
    connection.execute(f"PUT {file_path} @{DATABASE_NAME}.PUBLIC.%{TABLE_NAME_META_DATA};")
    connection.execute(copy_into_metadata_table)

class MetaDataClass(BaseModel):
    file_size_bytes: int
    num_pages: int
    file_path: str 
    encryption: str
    date_updated: str

def _get_pdf_metadata(pdf_file_path, pdf_s3_link):
    """Extracts metadata from one or more PDF files."""
    all_metadata = []
    try:
        # Check for file existence:
        if not os.path.exists(pdf_file_path):
            raise FileNotFoundError(f"File not found: {pdf_file_path}")

        # Open the PDF file in binary mode:
        with open(pdf_file_path, 'rb') as file:
            # Create a PdfReader object:
            pdf_reader = PyPDF2.PdfReader(file)

            # Extract metadata:
            metadata = {
                "file_size_bytes": os.path.getsize(pdf_file_path),
                "num_pages": len(pdf_reader.pages),
                "file_path": pdf_s3_link,
                "encryption": "Yes" if pdf_reader.is_encrypted else "No",
                "date_updated": date.today().strftime("%m/%d/%Y")
            }
            all_metadata.append(metadata)

    except Exception as e:
        print(f"Error processing {pdf_file_path}: {e}")

    return all_metadata

def write_to_csv(obj_list, temp_folder_path):
    fieldnames = list(MetaDataClass.schema()["properties"].keys())
    meta_data_file_path = f"{temp_folder_path}/meta_data.csv"
    with open(meta_data_file_path, "w") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames, quotechar='"', quoting=csv.QUOTE_STRINGS)
        writer.writeheader()
        for obj in obj_list:
            writer.writerow(obj.model_dump())

def _parse_pdf_and_perform_validation(pdf_local_file_path, pdf_s3_link, temp_folder_path):
    allmetadata = _get_pdf_metadata(pdf_local_file_path, pdf_s3_link)
    df = pd.DataFrame(allmetadata)
    df = df.fillna(np.nan).replace([np.nan], [None])
    validate_record_count = 0
    metadatainstance_list = []

    for i, row in df.iterrows():
        try:
            obj = MetaDataClass(file_size_bytes=row.file_size_bytes, num_pages= row.num_pages,
                        file_path=row.file_path, encryption= row.encryption, date_updated= row.date_updated)

            metadatainstance_list.append(obj)
            validate_record_count += 1
        except Exception as ex:
            logging.info(ex)

    if validate_record_count == df.shape[0]:
        logging.info("Successfully validated")
    else:
        logging.info("Validation failed in some records. Please fix and retry")
    write_to_csv(metadatainstance_list, temp_folder_path)


def _find_pdf_files(folder_path):
    pdf_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.pdf'):
                pdf_files.append(os.path.join(root, file))
    return pdf_files


# Entry point of task
def parse_to_csv(**kwargs):
    print("Starting meta data parsing")
    ti = kwargs['ti']
    temp_folder_path = ti.xcom_pull(key=XCOM_FOLDER_PATH, task_ids='extract_text')
    pdf_s3_link = ti.xcom_pull(key=XCOM_FILE_PATH, task_ids='extract_text')
    pdf_files = _find_pdf_files(temp_folder_path)

    _parse_pdf_and_perform_validation(pdf_files[0], pdf_s3_link, temp_folder_path)

# Entry point
def start_upload(**kwargs):
    engine = create_engine(base_url)
    print("Starting snowflake upload")
    ti = kwargs['ti']
    temp_folder_path = ti.xcom_pull(key=XCOM_FOLDER_PATH, task_ids='extract_text')

    try:
        connection = engine.connect()
        execute_ddl_queries(connection=connection)
        print('Completed databases, warehouse and table creation')
        print('Completed role creation and granted permissions successfully')
        # upload_into_web_scraped_table(connection=connection)
        print('Data upload into web scraped table successful')
        upload_into_content_data_table(connection=connection, temp_folder_path=temp_folder_path)
        print('Data upload into content data table successful')
        upload_into_metadata_table(connection=connection, temp_folder_path=temp_folder_path)
        print('Data upload into meta data table successful')

    except Exception as e:
        print(e)
        exit(1)
    finally:
        connection.close()
        engine.dispose()  


extract_task = PythonOperator(
    task_id='extract_text',
    python_callable=extract_text_from_pdf,
    op_kwargs={'bucket_name': 'assignment-4bigdata'},
    dag=dag,
)

structured_task = PythonOperator(
    task_id='xml_to_structured',
    python_callable=xml_to_structured_text,
    op_kwargs={'structured_data': '{{ ti.xcom_pull(task_ids="extract_text") }}'},
    dag=dag,
)

metadata_task = PythonOperator(
    task_id='metadata_task',
    python_callable=parse_to_csv,
    #op_kwargs={'sections': '{{ ti.xcom_pull(task_ids="validate_data") }}', 'bucket_name': 'assignment4-ext', 'file_name': 'extracted_data.csv'},
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_into_snowflake',
    python_callable=start_upload,
    #op_kwargs={'sections': '{{ ti.xcom_pull(task_ids="validate_data") }}', 'bucket_name': 'assignment4-ext', 'file_name': 'extracted_data.csv'},
    dag=dag,
)

extract_task >> structured_task >> metadata_task >> load_task

