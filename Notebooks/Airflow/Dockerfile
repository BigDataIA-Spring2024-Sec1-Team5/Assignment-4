# Use the official Airflow image as a parent image
FROM apache/airflow:2.9.0b2-python3.12

# Optionally, set a user to avoid running as root
USER root

# Install Chrome for Selenium
RUN apt-get update && apt-get install -y wget gnupg2
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
RUN apt-get update && apt-get install -y google-chrome-stable
# Install additional system dependencies if needed
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     <your-dependencies> \
#  && apt-get clean && rm -rf /var/lib/apt/lists/*
# Copy your requirements.txt file into the image
COPY requirements.txt /requirements.txt



# Switch back to the airflow user
USER airflow

# Install any dependencies you have in your requirements.txt file
RUN pip install --no-cache-dir -r /requirements.txt
#RUN pip install --upgrade setuptools wheel




