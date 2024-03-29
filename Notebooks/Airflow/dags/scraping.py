# Assuming this is saved as dags/cfa_scraper_dag.py in your Airflow directory
import time
import csv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import boto3
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
from airflow.hooks.base_hook import BaseHook
import re

# Setup Chrome WebDriver
def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    #driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver = webdriver.Remote(
    command_executor='http://selenium-standalone-chrome:4444/wd/hub',
    options=chrome_options)
    return driver

# Get reading links from a given URL
def get_reading_links(url):
    driver = setup_driver()
    driver.get(url)
    time.sleep(2)  # Wait for the page to load
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    reading_links = [a['href'] for a in soup.select('a.CoveoResultLink')]
    driver.quit()
    return reading_links


# Modified Scrape content from each reading link function
def scrape_reading_content(links):
    driver = setup_driver()
    readings = []
    for link in links:
        driver.get(link)
        time.sleep(2)  # Wait for the page to load
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        title = soup.find('h1').text.strip() if soup.find('h1') else 'Title Not Found'
        
        # New scraping logic
        topic = soup.find("span", class_="content-utility-topics").text.strip() if soup.find("span", class_="content-utility-topics") else "Topic Not Found"
        year_text = soup.find("span", class_="content-utility-curriculum").text.strip() if soup.find("span", class_="content-utility-curriculum") else "Year Not Found"
        # Use regular expression to find all digits in the year_text
        year_matches = re.findall(r'\d+', year_text)
        year = year_matches[0] if year_matches else "Year Not Found"  # Take the first match as the year
        
        level = soup.find("span", class_="content-utility-topic").text.strip() if soup.find("span", class_="content-utility-topic") else "Level Not Found"
        link_to_full_pdf = soup.find("a", class_="locked-content")["href"].strip() if soup.find("a", class_="locked-content") else "Link Not Found"
        
        # Extracts the introduction, learning outcomes, and summary using the provided functions
        introduction = extract_text_by_header(soup, ['Introduction', 'Overview', 'INTRODUCTION'])
        learning_outcomes = extract_learning_outcomes(soup)
        summary = extract_text_by_header(soup, ['Summary'])

        readings.append({
            'Title': title,
            'Topic': topic,
            'Year': year,
            'Level': level,
            'Introduction': introduction,
            'Learning Outcomes': learning_outcomes,
            'Summary': summary,
            'Link': link,
            'Link to PDF': link_to_full_pdf
        })
    driver.quit()
    return readings


# Helper functions for extracting introduction, learning outcomes, and summary
def extract_text_by_header(soup, header_texts):
    content = ""
    header_texts = [text.lower() for text in header_texts]
    headers = soup.find_all(['h2', 'h3'], text=lambda text: text.lower() in header_texts)
    for header in headers:
        current_element = header.find_next_sibling()
        while current_element and current_element.name not in ['h2', 'h3']:
            if current_element.name in ['p', 'div', 'ol', 'ul']:
                content += '\n' + ' '.join(current_element.stripped_strings)
            current_element = current_element.find_next_sibling()
        break  # Assuming only one section is needed
    return content.strip()

def extract_learning_outcomes(soup):
    content = ""
    header = soup.find('h2', text=lambda t: "Learning Outcomes" in t)
    if header:
        section = header.find_next_sibling('section')
        if section:
            content = ' '.join(section.stripped_strings)
    return content.strip()



# Save the scraped data into a CSV file
def save_to_csv(readings, filename="/tmp/cfa_readings.csv"):
    keys = readings[0].keys()
    with open(filename, 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(readings)

# Upload file to S3
def upload_to_s3(filename, bucket_name, object_name=None, aws_conn_id='my_aws_credentials'):
    connection = BaseHook.get_connection(aws_conn_id)
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        # Optionally, specify a region:
        # region_name='us-east-1'
    )
    
    if object_name is None:
        object_name = filename
    s3_client.upload_file(filename, bucket_name, object_name)

def scrape_and_upload():
    urls = ['https://www.cfainstitute.org/en/membership/professional-development/refresher-readings#sort=%40refreadingcurriculumyear%20descending&numberOfResults=100',
            'https://www.cfainstitute.org/en/membership/professional-development/refresher-readings#first=100&sort=%40refreadingcurriculumyear%20descending&numberOfResults=100',
            'https://www.cfainstitute.org/en/membership/professional-development/refresher-readings#first=200&sort=%40refreadingcurriculumyear%20descending&numberOfResults=100']

    all_links = []
    for url in urls:
        links = get_reading_links(url)
        all_links.extend(links)
    readings = scrape_reading_content(all_links)
    save_to_csv(readings)
    # Specify the bucket name and AWS connection ID here
    upload_to_s3("/tmp/cfa_readings.csv", "assignment4-ext", "cfa_readings.csv", "my_aws_credentials")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cfa_scraper',
    default_args=default_args,
    description='Scrape CFA website and upload to S3',
    schedule_interval=None,
)

t1 = PythonOperator(
    task_id='scrape_and_upload',
    python_callable=scrape_and_upload,
    dag=dag,
)