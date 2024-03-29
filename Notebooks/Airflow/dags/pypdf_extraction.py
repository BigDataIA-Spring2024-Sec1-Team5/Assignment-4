from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
from io import BytesIO
import boto3
import PyPDF2
import os
import pandas as pd
import re as StringIO
from airflow.hooks.base_hook import BaseHook
import json

def clean_text(text):
    return re.sub(r'\n\s*\n', '\n\n', text).strip()


def download_pdf_from_s3(bucket_name, s3_key, local_path, aws_conn_id):
    connection = BaseHook.get_connection(aws_conn_id)
    
    # Create an S3 client using the stored credentials
    s3 = boto3.client(
        's3',
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        region_name=connection.extra_dejson.get('region_name', 'us-east-1')  # Default to 'us-east-1' if not specified
    )
    
    s3.download_file(bucket_name, s3_key, local_path)


def extract_text_pypdf(file_path):
    with open(file_path, 'rb') as f:
        # Use PdfFileReader for PyPDF2 versions older than 2.0.0
        pdf_reader = PyPDF2.PdfFileReader(f)
        text = ''
        for page_num in range(pdf_reader.numPages):
            text += pdf_reader.getPage(page_num).extractText()
    return text


def save_text_to_file(text, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(text)


def upload_text_to_s3(bucket_name, s3_key, text, aws_conn_id):
    connection = BaseHook.get_connection(aws_conn_id)
    
    # Create an S3 resource using the stored credentials
    s3 = boto3.resource(
        's3',
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        region_name=connection.extra_dejson.get('region_name', 'us-east-1')  # Default to 'us-east-1' if not specified
    )
    
    # Convert the text to bytes
    text_bytes = BytesIO(text.encode('utf-8'))
    
    # Upload the text to the specified S3 bucket
    s3.Object(bucket_name, s3_key).upload_fileobj(text_bytes)


def process_pdf(bucket_name, s3_keys, output_s3_bucket, aws_conn_id):
    for s3_key in s3_keys:
        local_path = f'/tmp/{s3_key}'  # Generate a unique local file path for each PDF
        output_s3_key = f'pypdf2_extracted_text_{s3_key.replace(" ", "_").replace(".pdf", ".txt")}'  # Generate a unique S3 key for the output text

        download_pdf_from_s3(bucket_name, s3_key, local_path, aws_conn_id)
        text = extract_text_pypdf(local_path)
        
        # Upload the extracted text to a new S3 bucket
        upload_text_to_s3(output_s3_bucket, output_s3_key, text, aws_conn_id)
        print(f"Extracted text uploaded to: s3://{output_s3_bucket}/{output_s3_key}")

dag = DAG(
    'pypdf_extraction',
    default_args={'start_date': days_ago(1)},
    schedule_interval=None,  # Run this manually or set your schedule
    catchup=False
)


process_pdf_task = PythonOperator(
    task_id='process_pdf',
    python_callable=process_pdf,
    op_kwargs={
        'bucket_name': 'assignment-4bigdata',
        's3_keys': [
           # 'Shikhar Patel.pdf',
           # 'Shikhar_Patel_Resume_Data_Analytics_Munich Re.pdf'
           '2024-l1-topics-combined-2.pdf',
           '2024-l2-topics-combined-2.pdf',
           '2024-l3-topics-combined-2.pdf'
        ],
        'aws_conn_id': 'my_aws_credentials',  # Reference to your Airflow AWS connection
        'output_s3_bucket': 'assignment4-ext',
    },
    dag=dag
)