FROM apache/airflow:2.3.0
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*


# Install Chromium and Chromium Driver for Selenium
RUN apt-get update && \
    apt-get install -y chromium chromium-driver && \
    apt-get -f install

# Create QR Code Studio Directory and Give Necessary Permissions
RUN mkdir -p /tmp/qr_code_studio
RUN chmod 777 /tmp/qr_code_studio


USER airflow
COPY /requirements.txt /requirements.txt

RUN pip install --no-cache-dir --user google-api-python-client google-auth-httplib2  google-auth-oauthlib python-dotenv
RUN pip install --no-cache-dir --user  gspread gspread-dataframe fuzzywuzzy paramiko argparse mysql-connector-python SQLAlchemy
RUN pip install --no-cache-dir --user selenium==4.9.1
RUN pip install --no-cache-dir --user xlrd==2.0.1
RUN pip install --no-cache-dir --user openpyxl==3.1.2