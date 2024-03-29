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

from pathlib import Path

# Fetch AWS credentials from Airflow connection
aws_conn_id = 'my_aws_credentials'  # Corrected AWS connection ID
aws_credentials = BaseHook.get_connection(aws_conn_id)
aws_access_key_id = aws_credentials.login
aws_secret_access_key = aws_credentials.password

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

def read_pdf_from_s3(bucket_name, file_key):
    pdf_file = s3.get_object(Bucket=bucket_name, Key=file_key)
    pdf_content = pdf_file['Body'].read()
    return pdf_content

def extract_text_from_pdf(bucket_name, file_key):
    Path('/tmp/processed_data').mkdir(parents=True, exist_ok=True)
    s3.download_file('assignment-4bigdata', '2024-l1-topics-combined-2.pdf', '/tmp/processed_data/l1.pdf')

    # Process the temporary file with GrobidClient
    client.process("processFulltextDocument",
                              '/tmp/processed_data',
                              output='/tmp/processed_data',
                              consolidate_header=True,
                              consolidate_citations=True,
                              include_raw_affiliations=True)


def upload_csv_to_s3(bucket_name, file_name, csv_content):
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_content)

def convert_to_csv_and_upload(sections, bucket_name='assignment4-ext', file_name='extracted_data.csv'):
    csv_output = StringIO()
    writer = csv.writer(csv_output)
    writer.writerow(['Heading', 'Paragraph'])  # Adjust columns as needed
    for section in sections:
        writer.writerow([section.heading, section.paragraph])
    
    upload_csv_to_s3(bucket_name, file_name, csv_output.getvalue())

class DocumentSection(BaseModel):
    heading: str = ''  # Add a default value for the heading field
    paragraph: str = Field(...)

    @validator('paragraph')
    def paragraph_must_not_be_empty(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('Paragraph must not be empty')
        return v.strip()

def xml_to_structured_text(xml_data):
    # Parse the XML data
    xml_str = xml_data.decode('utf-8')

    root = etree.fromstring(xml_str)
    ns = {'tei': 'http://www.tei-c.org/ns/1.0'}

    # Initialize a list to hold the structured sections
    structured_sections = []

    # Extract abstract sections
    abstract_sections = root.xpath('//tei:profileDesc/tei:abstract/tei:div', namespaces=ns)
    for section in abstract_sections:
        heading = section.xpath('.//tei:head/text()', namespaces=ns)[0] if section.xpath('.//tei:head/text()', namespaces=ns) else "Abstract"
        paragraphs = "\n\n".join(section.xpath('.//tei:p/text()', namespaces=ns))
        structured_sections.append({
            'heading': heading,
            'paragraph': paragraphs
        })

    # Extract content sections
    content_sections = root.xpath('//tei:text/tei:body/tei:div', namespaces=ns)
    for section in content_sections:
        heading = section.xpath('.//tei:head/text()', namespaces=ns)[0] if section.xpath('.//tei:head/text()', namespaces=ns) else "Content"
        paragraphs = "\n\n".join(section.xpath('.//tei:p/text()', namespaces=ns))
        structured_sections.append({
            'heading': heading,
            'paragraph': paragraphs
        })

    return structured_sections

def validate_and_structure_data(structured_data):
    sections = []
    for section in structured_data:
        try:
            document_section = DocumentSection(**section)
            sections.append(document_section)
        except ValidationError as e:
            print(e.json())
    return sections

extract_task = PythonOperator(
    task_id='extract_text',
    python_callable=extract_text_from_pdf,
    op_kwargs={'bucket_name': 'assignment-4bigdata', 'file_key': '2024-l1-topics-combined-2.pdf'},
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_and_structure_data,
    op_kwargs={'structured_data': '{{ ti.xcom_pull(task_ids="extract_text") }}'},
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_into_s3',
    python_callable=convert_to_csv_and_upload,
    op_kwargs={'sections': '{{ ti.xcom_pull(task_ids="validate_data") }}', 'bucket_name': 'assignment4-ext', 'file_name': 'extracted_data.csv'},
    dag=dag,
)

extract_task >> validate_task >> load_task