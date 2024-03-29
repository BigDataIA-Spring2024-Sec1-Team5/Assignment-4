import streamlit as st
import requests
import os
import boto3
import pandas as pd
import snowflake.connector

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Function to send query to FastAPI endpoint
def send_query_to_fastapi(file_key):
    endpoint = "http://localhost:8000/trigger_dag/"
    try:
        response = requests.get(endpoint, params={"file_key": file_key})
        response.raise_for_status()  # Raise error if request fails
        return response.json()
    except requests.exceptions.HTTPError as err:
        st.error(f"Error: {err}")

# # Function to upload file to S3
# def upload_to_s3(file_path, bucket_name, object_name):
#     s3 = boto3.client('s3',
#                       aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
#                       aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
#     try:
#         s3.upload_file(file_path, bucket_name, object_name)
#         st.success("File uploaded successfully to S3!")
#         return object_name  # Return the file key (object name)
#     except Exception as e:
#         st.error(f"An error occurred: {e}")
# Function to upload file to S3
def upload_to_s3(file_path, bucket_name, object_name):
    s3 = boto3.client('s3',
                      aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                      aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
    try:
        s3.upload_file(file_path, bucket_name, object_name)
        st.success("File uploaded successfully to S3!")
        return object_name  # Return the file key (object name)
    except Exception as e:
        st.error(f"An error occurred: {e}")

# Function to connect to Snowflake and execute query
def execute_query(query):
    # Snowflake connection parameters
    conn_params = {
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA')
    }

    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(**conn_params)
        cursor = conn.cursor()

        # Execute query
        cursor.execute(query)

        # Get query results
        results = cursor.fetchall()
        columns = [col[0] for col in cursor.description]

        # Create DataFrame
        df = pd.DataFrame(results, columns=columns)

        return df

    except Exception as e:
        st.error(f"An error occurred: {e}")

    finally:
        # Close connection
        cursor.close()
        conn.close()

def main():
    pages = ['Upload PDF', 'Query Response']
    choice = st.sidebar.selectbox('Select Page', pages)

    if choice == 'Upload PDF':
        st.title("PDFParsePro🤖")

        # Embedding the image
        st.image("pdfimg.jpg", caption="PDF Image", use_column_width=True)

        uploaded_file = st.file_uploader("Upload a PDF file", type=["pdf"])

        if uploaded_file is not None:
            st.write("You've uploaded the following file:")
            st.write(uploaded_file.name)

            if st.button("Upload to S3"):
                # Save the uploaded file locally
                with open(uploaded_file.name, 'wb') as f:
                    f.write(uploaded_file.getvalue())
                # Get the full file path
                file_path = os.path.abspath(uploaded_file.name)
                file_key = upload_to_s3(file_path, os.getenv('S3_BUCKET_NAME'), uploaded_file.name)
                # Delete the local file after uploading
                os.remove(file_path)
                # Trigger FastAPI with the file key
                send_query_to_fastapi(file_key)  # Pass file_key to FastAPI

    elif choice == 'Query Response':
        st.title("Query Response")
        user_query = st.text_input("Enter your query here:")
        if st.button("Submit Query"):
            # Execute query
            df = execute_query(user_query)

            # Display results
            if df is not None:
                st.write("Query Result:")
                st.write(df)
            else:
                st.error("No data returned or error occurred")

if __name__ == "__main__":
    main()
