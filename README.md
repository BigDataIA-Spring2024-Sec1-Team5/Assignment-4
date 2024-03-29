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
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/)
[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-109989?style=for-the-badge&logo=airflow&logoColor=white)](https://airflow.apache.org/)
[![Grobid](https://img.shields.io/badge/grobid-909090?style=for-the-badge&logo=grobid&logoColor=blue)](https://grobid.readthedocs.io/en/latest/Introduction/)
[![PyPDF2](https://img.shields.io/badge/PyPDF2-123499?style=for-the-badge&logo=python&logoColor=blue)](https://pypi.org/project/PyPDF2/)
[![FastAPI](https://img.shields.io/badge/FastAPI-cfe3e4?style=for-the-badge&logo=python&logoColor=green)](https://fastapi.tiangolo.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-fa722a?style=for-the-badge&logo=python&logoColor=white)](https://docs.streamlit.io/)

## Data Sources
*The data source is the [CFA Institute's Refresher Readings](https://www.cfainstitute.org/membership/professional-development/refresher-readings/#sort=%40refreadingcurriculumyear%20descending)* and the provided PDF files.

## Architecture Workflow
![CFA Workflow](https://github.com/BigDataIA-Spring2024-Sec1-Team5/Assignment-4/blob/main/Images/architecture.jpg)

## Pre requisites
*Installed Libraries of Python, PyPDF2, lxml eTree, Snowflake, SQLAlchemy, Pydantic 2, Pytest, Airflow, FastAPI, Streamlit. <br>
Existing accounts of Snowflake and DBT*

## Project Structure
```
📦 Assignment4
├─ ReadME
├─ Documentation
├─ Notebooks
│  ├─ Streamlit
│  │  ├─ Streamlit.py
│  │  └─ requirements.txt
│  ├─ FastAPI
│  │  ├─ fastapi_logs.log
│  │  ├─ main.py
│  │  ├─ requirements.txt
│  │  └─ snowflake_fastapi.py
│  └─ Airflow
│     ├─ dags
│     │  ├─ tasks
│     │  │  └─ grobid_client
│     │  │     ├─ client.py
│     │  │     ├─ __init__.py
│     │  │     ├─ config.json
│     │  │     └─ grobid_client.py
│     │  ├─ grobid_extraction.py
│     │  ├─ pypdf_csv.py
│     │  ├─ pypdf_extraction.py
│     │  └─ scraping.py
│     ├─ requirements.txt
│     ├─ docker-compose.yaml
│     └─ Dockerfile
└─ Images
   ├─ architecture.jpg
   ├─ FastAPI_Streamlit
   │  ├─ FastAPI-2.jpeg
   │  ├─ FastAPI.jpeg
   │  ├─ PDF_Upload.png
   │  ├─ Query_PDF.png
   │  ├─ Query_Response.png
   │  └─ Streamlit_Query.png
   └─ Airflow
      ├─ airflow.png
      └─ extracted_s3.png
```
©generated by [Project Tree Generator](https://woochanleee.github.io/project-tree-generator)

## References
https://airflow.apache.org/docs/apache-airflow/stable/index.html <br>
https://docs.docker.com/ <br>
https://docs.snowflake.com/en/developer-guide/python-connector/sqlalchemy <br>
https://docs.pytest.org/en/8.0.x/ <br>
https://docs.pydantic.dev/latest/ <br>
https://grobid.readthedocs.io/en/latest/Introduction/ <br>
https://learn.microsoft.com/en-us/azure/architecture/ai-ml/ <br>

## Learning Outcomes
* Through this project, learners will acquire expertise in orchestrating data pipelines with Airflow, leveraging FastAPI for API development, and implementing user interfaces with Streamlit. They will gain proficiency in containerization using Docker, ensuring seamless deployment of services, and online hosting for accessibility. This hands-on experience will equip them with valuable skills in modern data engineering practices, including workflow automation, API development, and cloud-native architecture design, paving the way for successful implementation of similar projects in real-world scenarios. *

## Team Information and Contribution 

Name | Contribution %| Contributions |
--- |--- | --- |
Aditya Kanala | 33.5% | Apache Airflow|
Shubh Patel | 33.0% | FastAPI |
Shikhar Patel | 33.5% | Streamlit|
