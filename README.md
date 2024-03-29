# Assignment-4

## Live application Links
[![codelabs](https://img.shields.io/badge/codelabs-4285F4?style=for-the-badge&logo=codelabs&logoColor=white)]( https://codelabs-preview.appspot.com/?file_id=1RqHOoUqMIQfOulQol3h0uNZgvZv2XoVlEHEZQiwK2no#9)
[![workflow_architecture](https://img.shields.io/badge/workflow_architecture-FC6600?style=for-the-badge&logo=jupyter&logoColor=white)]( https://colab.research.google.com/drive/1ow6ueLi-AwGcwQSJhwZnQRYhPNbN8msh?usp=sharing)



## Problem Statement
*You are now tasked to build an end-to-end pipeline using Airflow to automate the data extraction and storing of meta-data and content of pdf files into Snowflake.*

## Project Goals
*The project aims to automate the extraction, validate,and loading of PDF data into the Snowflake using Airflow. FastAPI will power two APIs: one to trigger the pipeline and another to interact with Snowflake. Streamlit will provide a user-friendly interface fro uploading files and viewing query results. Dockerization enables easy deployment, while online hosting ensures accessibility.*

## Technologies Used
[![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)](https://www.python.org/)
[![Beautiful Soup](https://img.shields.io/badge/beautiful_soup-109989?style=for-the-badge&logo=beautiful_soup&logoColor=white)](https://pypi.org/project/beautifulsoup4/)
[![Selenium](https://img.shields.io/badge/Selenium-39e75f?style=for-the-badge&logo=selenium&logoColor=blue)](https://www.selenium.dev/)
[![Pydantic2](https://img.shields.io/badge/Pydantic_2-EF007E?style=for-the-badge&logo=pydantic&logoColor=blue)](https://docs.pydantic.dev/latest/)
[![Pytest](https://img.shields.io/badge/Pytest-D4E86D?style=for-the-badge&logo=pytest&logoColor=white)](https://docs.pytest.org/en/8.0.x/)
[![Snowflake](https://img.shields.io/badge/Snowflake-90e0ef?style=for-the-badge&logo=snowflake&logoColor=blue)](https://www.snowflake.com/en/)
[![DBT](https://img.shields.io/badge/DBT-f97f50?style=for-the-badge&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/)

## Data Sources
*The data source is the [CFA Institute's Refresher Readings](https://www.cfainstitute.org/membership/professional-development/refresher-readings/#sort=%40refreadingcurriculumyear%20descending)* and the provided PDF files.

## Architecture Workflow
![CFA Workflow](https://github.com/BigDataIA-Spring2024-Sec1-Team5/Assignment-3/blob/main/Images/Assignment-3.png)

## Pre requisites
*Installed Libraries of Python, PyPDF2, lxml eTree, Snowflake, SQLAlchemy, Pydantic 2, Pytest, Airflow, FastAPI, Streamlit. <br>
Existing accounts of Snowflake and DBT*

## Project Structure
```
📦 Assignment3
├─ ReadME
|- Images
│  ├─ Assignment-3.png
│  ├─ CSV.png
│  ├─ Content_Class.png
│  ├─ Metadata_Class.png
│  ├─ Previous_Architecture.png
│  ├─ PyTest_MetaData.png
│  ├─ S3.png
│  ├─ Snowflake.png
│  ├─ URL_Class.png
│  ├─ XML.png
│  └─ dbt.png
├─ Notebook
│  ├─ Part_1
│  │  ├─ URL Class
│  │  │  ├─ WebScraping.ipynb
│  │  │  ├─ 02_URLClass.ipynb
│  │  │  └─ 03_URL_Pytest.py
│  │  └─ PDF_Class
│  │     ├─ Metadata PDF
│  │     │  ├─ MetaDataClass.ipynb
│  │     │  └─ MetaDataClass_Pytest.py
│  │     └─ Content PDF
│  │        ├─ ContentClass.ipynb
│  │        └─ ContentClass_Pytest.py
│  └─ Part_2
│     └─ DBT.ipynb
└─ Outputs
   ├─ Part _1
   │  ├─ PyTest_MetaData.png
   │  ├─ URL_Pytest.png
   │  ├─ ContentClass_Cleaned.csv
   │  ├─ MetaData_Cleaned.csv
   │  └─ Updated_Scrapped_Data.csv
   └─ Part_2
      ├─ Lineage
      │  ├─ DBT_Model.png
      │  ├─ stg_learning.png
      │  ├─ stg_metadata.png
      │  └─ stg_summary.png
      └─ Data
         ├─ Screenshots
         │  ├─ Environments.png
         │  ├─ Prod_Job.png
         │  ├─ Test_Job.png
         │  ├─ cfa_prod_clean_csv.png
         │  ├─ cfa_test_clean_csv.png
         │  ├─ cfa_test_sql.png
         │  ├─ dbt_docs.png
         │  ├─ dbt_preview.png
         │  ├─ stg_learning.png
         │  ├─ stg_metadata.png
         │  └─ stg_summary.png
         └─ CSV Files
            ├─ models_cfa_test.csv
            ├─ models_stg_learning.csv
            ├─ models_stg_metadata.csv
            └─ models_stg_summary.csv
```
©generated by [Project Tree Generator](https://woochanleee.github.io/project-tree-generator)

## References
https://www.getdbt.com/ <br>
https://docs.getdbt.com/guides/snowflake?step=1 <br>
https://docs.snowflake.com/en/developer-guide/python-connector/sqlalchemy  <br>
https://docs.pytest.org/en/8.0.x/ <br>
https://docs.pydantic.dev/latest/

## Learning Outcomes
* Through this project, learners will acquire expertise in orchestrating data pipelines with Airflow, leveraging FastAPI for API development, and implementing user interfaces with Streamlit. They will gain proficiency in containerization using Docker, ensuring seamless deployment of services, and online hosting for accessibility. This hands-on experience will equip them with valuable skills in modern data engineering practices, including workflow automation, API development, and cloud-native architecture design, paving the way for successful implementation of similar projects in real-world scenarios. *

## Team Information and Contribution 

Name | Contribution %| Contributions |
--- |--- | --- |
Aditya Kanala | 33.5% | DBT|
Shubh Patel | 33% | PDF Classes |
Shikhar Patel | 33.5% | Test cases using Pytest|
