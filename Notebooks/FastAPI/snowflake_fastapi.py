from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
import os
import snowflake.connector
import logging

# Configure logging
log_file_path = 'fastapi_logs.log'
logging.basicConfig(filename=log_file_path, level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Initialize FastAPI
app = FastAPI()

# Snowflake connection configuration
snowflake_config = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA')
}

# Function to execute Snowflake queries
def execute_query(query):
    try:
        logger.debug("Connecting to Snowflake")
        conn = snowflake.connector.connect(**snowflake_config)
        logger.debug("Connected to Snowflake")
        
        cursor = conn.cursor()
        logger.debug(f"Executing query: {query}")
        cursor.execute(query)
        
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        logger.debug("Query execution successful")
        return result
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail=f"Error executing query: {e}")

# FastAPI endpoint to handle queries from Streamlit
@app.post("/query/")
async def query_endpoint(query: str):
    if not query:
        raise HTTPException(status_code=400, detail="Query parameter is required")
    try:
        logger.debug("Received query request")
        result = execute_query(query)
        return result
    except snowflake.connector.errors.ProgrammingError as pe:
        logger.error(f"Snowflake programming error: {pe}")
        raise HTTPException(status_code=422, detail=str(pe))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("snowflake_fastapi:app", host="0.0.0.0", port=8000)
