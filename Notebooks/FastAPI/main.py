import os
from typing import Optional
from fastapi import FastAPI, HTTPException, Query
from dotenv import load_dotenv
import requests
import boto3  # Import boto3 for S3 operations

# Load environment variables
load_dotenv()

# Get AWS credentials from environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
s3_bucket_name = os.getenv('S3_BUCKET_NAME')
aws_region_name = os.getenv('AWS_REGION_NAME')

app = FastAPI()

# Airflow DAG URL - Replace placeholders with actual values

AIRFLOW_DAG_URL = "http://<airflow_host>:<port>/api/v1/dags/<dag_id>/dag_runs"
#AIRFLOW_DAG_URL = "http://localhost:8080/api/v1/dags/pdf_extract/dag_runs"

@app.get("/trigger_dag/")
async def trigger_airflow_dag(file_key: str = Query(..., description="The key of the file in S3")):
    try:
        # Get file content from S3
        s3 = boto3.client('s3',
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key,
                          region_name=aws_region_name)
        response = s3.get_object(Bucket=s3_bucket_name, Key=file_key)
        file_content = response['Body'].read().decode('utf-8')

        # Trigger Airflow DAG with file content as parameter
        response = requests.post(AIRFLOW_DAG_URL, json={"conf": {"file_content": file_content}})
        response.raise_for_status()
        
        # Inspect the response
        if response.status_code == 200:
            return {"message": f"Airflow DAG trigger request sent successfully. DAG will be triggered if properly configured."}
        else:
            return {"message": "Received unexpected response from Airflow server."}

    except Exception as e:
        # Log the error
        print(f"Error triggering Airflow DAG: {e}")
        # Raise HTTPException with appropriate message
        raise HTTPException(status_code=500, detail=f"Failed to trigger Airflow DAG: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000)
