from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
from io import BytesIO, StringIO
import boto3
import PyPDF2
import os
import pandas as pd
import re

def clean_text(text):
    return re.sub(r'\n\s*\n', '\n\n', text).strip()

def download_pdf_from_s3(bucket_name, s3_key, local_path, aws_conn_id):
    connection = BaseHook.get_connection(aws_conn_id)
    s3 = boto3.client(
        's3',
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        region_name=connection.extra_dejson.get('region_name', 'us-east-1')
    )
    s3.download_file(bucket_name, s3_key, local_path)

def extract_text_pypdf(file_path):
    with open(file_path, 'rb') as f:
        pdf_reader = PyPDF2.PdfFileReader(f)
        text = ''
        for page_num in range(pdf_reader.numPages):
            text += pdf_reader.getPage(page_num).extractText()
    return text

def upload_text_to_s3(bucket_name, s3_key, text, aws_conn_id):
    connection = BaseHook.get_connection(aws_conn_id)
    s3 = boto3.resource(
        's3',
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        region_name=connection.extra_dejson.get('region_name', 'us-east-1')
    )
    text_bytes = BytesIO(text.encode('utf-8'))
    s3.Object(bucket_name, s3_key).upload_fileobj(text_bytes)

def extract_titles_from_text(text):
    pattern = re.compile(r'(?P<title>[A-Z][\w\s]+)\nLEARNING OUTCOMES', re.MULTILINE)
    titles = []
    for match in pattern.finditer(text):
        title = match.group("title").strip()
        lines = title.split("\n")
        last_line_words = lines[-1].split()
        if len(last_line_words) == 1 and len(lines) > 1:
            pre_last_line_words = lines[-2].split()
            if len(pre_last_line_words) >= 2 and all(word[0].isupper() for word in pre_last_line_words):
                title = f"{lines[-2]} {last_line_words[0]}"
        titles.append(title)
    return titles

def clean_learning_outcome(val):
    val = val.replace('\t', ' ')
    cleaned_val = re.sub(r'[^\w\s.-]', '', val)
    if not cleaned_val.endswith('.'):
        cleaned_val += '.'
    cleaned_val = cleaned_val.capitalize()
    return cleaned_val

def clean_topics(val):
    cleaned_val = re.sub(r'\d+', '', val)
    return cleaned_val

def process_dataframe(df):
    df['Learning Outcomes'] = df['Learning Outcomes'].apply(clean_learning_outcome)
    df['Topic'] = df['Topic'].apply(clean_topics)
    return df

def generate_and_upload_csv(text, bucket_name, output_s3_key, aws_conn_id):
    titles_before_outcomes = extract_titles_from_text(text)
    lines = text.split("\n")
    data = []
    current_topic = ""
    current_heading = ""
    outcome = ""
    for i, line in enumerate(lines):
        line = line.strip()
        if not line or line == "LEARNING OUTCOMES":
            continue
        if line[0].isupper() and not line.startswith("□") and not "The candidate should be able to" in line:
            if current_topic and current_heading and outcome:
                data.append([current_topic, current_heading, outcome.strip("□ ").replace("\n", " ")])
                outcome = ""
            if line in titles_before_outcomes:
                current_topic = line
                current_heading = ""
            else:
                current_heading = line
        elif line.startswith("□") or "The candidate should be able to" in line:
            if outcome:
                data.append([current_topic, current_heading, outcome.strip("□ ").replace("\n", " ")])
                outcome = ""
            outcome = line
        if i == len(lines) - 1 and outcome:
            data.append([current_topic, current_heading, outcome])
    data_corrected = [[i+1, item[0], item[1], item[2]] for i, item in enumerate(data)]
    df = pd.DataFrame(data_corrected, columns=["Column No", "Topic", "Heading", "Learning Outcomes"])
    processed_df = process_dataframe(df)
    
    # Convert DataFrame to CSV
    csv_buffer = StringIO()
    processed_df.to_csv(csv_buffer, index=False)
    
    # Upload CSV to S3
    upload_text_to_s3(bucket_name, output_s3_key.replace('.txt', '.csv'), csv_buffer.getvalue(), aws_conn_id)

def process_pdf(bucket_name, s3_keys, output_s3_bucket, aws_conn_id):
    for s3_key in s3_keys:
        local_path = f'/tmp/{s3_key}'
        output_s3_key = f'pypdf2_extracted_text_{s3_key.replace(" ", "_").replace(".pdf", ".txt")}'
        download_pdf_from_s3(bucket_name, s3_key, local_path, aws_conn_id)
        text = extract_text_pypdf(local_path)
        upload_text_to_s3(output_s3_bucket, output_s3_key, text, aws_conn_id)
        generate_and_upload_csv(text, output_s3_bucket, output_s3_key, aws_conn_id)
        print(f"Extracted text and CSV uploaded to: s3://{output_s3_bucket}/{output_s3_key}")

# Airflow DAG and task setup
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'pdf_to_csv_extraction',
    default_args=default_args,
    description='A simple DAG to extract text from PDF and convert to CSV',
    schedule_interval=None,
)

def task_wrapper(**kwargs):
    bucket_name = 'assignment-4bigdata'
    s3_keys = ['2024-l1-topics-combined-2.pdf','2024-l2-topics-combined-2.pdf','2024-l3-topics-combined-2.pdf']  # List of PDF files to process
    output_s3_bucket = 'assignment4-ext'
    aws_conn_id = 'my_aws_credentials'
    
    process_pdf(bucket_name, s3_keys, output_s3_bucket, aws_conn_id)

run_extraction = PythonOperator(
    task_id='extract_and_convert_to_csv',
    python_callable=task_wrapper,
    dag=dag,
)