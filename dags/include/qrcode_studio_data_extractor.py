import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from sqlalchemy import create_engine, text
import xlrd
import re
from datetime import datetime
from airflow.models import Variable

pg_host = Variable.get("PG_HOST")
pg_user = Variable.get("PG_USERNAME_WRITE")
pg_password = Variable.get("PG_PASSWORD_WRITE")
pg_database = Variable.get("PG_DATABASE")

qr_code_studio_email = Variable.get("qr_code_studio_email")
qr_code_studio_password = Variable.get("qr_code_studio_password")
campaigns = {
    67830: "2023_CCEP_QR-Codes",
    69008: "2023_02_Krombacher_QR_Codes",
    62292: "2022/2023_Bitburger_QR-Codes"
}


def download_qrcode_studio_data(date_list=None):
    # Setup Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Ensure GUI is off
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.BinaryLocation = "/usr/bin/chromium"

    # Pass Download Directory Path For xls files
    prefs = {"download.default_directory": "/tmp/qr_code_studio"}
    chrome_options.add_experimental_option('prefs', prefs)

    # Set path to chromedriver as per your configuration
    browser = webdriver.Chrome(executable_path="/usr/bin/chromedriver", options=chrome_options)

    # Open the website
    browser.get("https://account.qrcode.studio/login")

    # Allow the page to load
    time.sleep(5)

    # Find the email and password input fields
    email_input_form = '/html/body/app-root/website/div/ng-component/div/div/div[2]/div/form/div[1]/input'
    email_input = browser.find_element(By.XPATH, email_input_form)
    password_input_form = '/html/body/app-root/website/div/ng-component/div/div/div[2]/div/form/div[2]/input'
    password_input = browser.find_element(By.XPATH, password_input_form)

    # Enter the email and password
    email_input.send_keys(qr_code_studio_email)
    password_input.send_keys(qr_code_studio_password)

    # Press ENTER to log in
    password_input.send_keys(Keys.RETURN)

    # Allow the page to load after logging in
    time.sleep(5)

    # Find and click campaigns button from navigation bar
    campaigns_button_xpath = '/html/body/app-root/manager/div/div/div[1]/div[1]/ul[1]/li[4]/a'
    campaigns_button = browser.find_element(By.XPATH, campaigns_button_xpath)
    browser.execute_script("arguments[0].click();", campaigns_button)

    # Allow the page to load after navigating campaigns page
    time.sleep(5)

    for campaign in list(campaigns.values()):

        # Find and navigate through each campaign page
        campaign_element = browser.find_element(By.XPATH, f'//div[contains(text(), "{campaign}")]')
        browser.execute_script("arguments[0].click();", campaign_element)

        # Allow the page to load after navigating specific campaign page
        time.sleep(5)

        # Go through Statistics Section
        statistics_element = browser.find_element(By.XPATH, '//a[contains(text(), "Statistics")]')
        browser.execute_script("arguments[0].click();", statistics_element)

        # Allow the page to load after navigating specific statistics page
        time.sleep(5)

        if date_list is not None:

            for date in date_list:
                # Create Select object and Select by visible text
                select_element_xpath = \
                    '/html/body/app-root/manager/div/div/div[2]/div/campaign/form/div[3]/div[2]/statistics/div[1]/div[1]/div/select'

                select_element = browser.find_element(By.XPATH, select_element_xpath)
                select = Select(select_element)
                select.select_by_visible_text('Custom')

                # Allow the page to load Custom Date Forms
                time.sleep(5)

                # Locate from part of date form
                from_date_input = \
                    '/html/body/app-root/manager/div/div/div[2]/div/campaign/form/div[3]/div[2]/statistics/div[1]/div[2]/div/p-calendar[1]/span/input'

                from_date_form = browser.find_element(By.XPATH, from_date_input)

                # Clear from part of date form
                browser.execute_script("arguments[0].value = '';", from_date_form)

                # Send a new date to the form
                from_date_form.send_keys(f"{date}")

                # Locate until part of date form
                until_date_input = \
                    '/html/body/app-root/manager/div/div/div[2]/div/campaign/form/div[3]/div[2]/statistics/div[1]/div[2]/div/p-calendar[2]/span/input'

                until_date_form = browser.find_element(By.XPATH, until_date_input)

                # Clear until part of date form
                browser.execute_script("arguments[0].value = '';", until_date_form)

                # Send new date to until part of date form
                until_date_form.send_keys(f"{date}")

                time.sleep(5)

                # Locate and click on refresh button after adding dates
                refresh_button_xpath = \
                    '/html/body/app-root/manager/div/div/div[2]/div/campaign/form/div[3]/div[2]/statistics/div[1]/div[2]/div/div/button'

                refresh_button_element = browser.find_element(By.XPATH, refresh_button_xpath)
                browser.execute_script("arguments[0].click();", refresh_button_element)

                # Let it complete the refresh
                time.sleep(5)

                # Click export button for specific date
                export_button = browser.find_element(By.XPATH, '//a[contains(text(), "Export")]')
                browser.execute_script("arguments[0].click();", export_button)

                # Let it download the xls file
                time.sleep(5)

        else:
            # Create Select object and Select by visible text
            select_element_xpath = \
                '/html/body/app-root/manager/div/div/div[2]/div/campaign/form/div[3]/div[2]/statistics/div[1]/div[1]/div/select'

            select_element = browser.find_element(By.XPATH, select_element_xpath)
            select = Select(select_element)
            select.select_by_visible_text('Today')

            # Allow the page to load current day statistics
            time.sleep(5)

            # Click export button for current day statistics
            export_button = browser.find_element(By.XPATH, '//a[contains(text(), "Export")]')
            browser.execute_script("arguments[0].click();", export_button)

            # Let it download the xls file
            time.sleep(5)

        # Find and click campaigns button from navigation bar to start other campaign iteration
        browser.execute_script("arguments[0].click();", campaigns_button)

        # Allow the page to load campaigns page again
        time.sleep(5)

    time.sleep(5)

    # Close the browser
    browser.quit()


def insert_qr_code_studio_data():
    # Establish a connection to the database using SQLAlchemy's create_engine function
    connection_string = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    engine = create_engine(connection_string)
    connection = engine.connect()

    # Change working directory and get list of excels
    os.chdir('/tmp/qr_code_studio')
    xls_list = os.listdir()

    # Iterate xls files
    for xls in xls_list:
        workbook = xlrd.open_workbook(xls, ignore_workbook_corruption=True)
        xls_file = pd.ExcelFile(workbook)

        # Regex to fetch date and campaign part
        match = re.search(r'(\d+)-\d+_campaign_statistics_(\d+)', xls)

        # Convert date string to YYYY-MM-DD format
        date = datetime.strptime(match.group(1), '%Y%m%d')
        formatted_date = date.strftime('%Y-%m-%d')

        # Convert Campaign id str into int
        campaign_id = int(match.group(2))

        # Fetch & Insert Scans per Code
        try:
            df_scans_per_code = xls_file.parse('scans per code')

            if not df_scans_per_code.empty:

                df_scans_per_code['created_at'] = formatted_date
                df_scans_per_code['campaign_id'] = campaign_id
                df_scans_per_code['campaign_name'] = campaigns[campaign_id]
                df_scans_per_code.rename(columns={'unique': 'unique_scans'}, inplace=True)

                df_scans_per_code.to_sql('qr_studio_scans_per_code', engine, schema='housten_final',
                                         if_exists='append', index=False)

        except ValueError:
            print(f"The file {xls} does not have 'scans per code' sheet!")

        # Fetch & Insert Cities
        try:
            df_cities = xls_file.parse('cities')

            if not df_cities.empty:

                df_cities['created_at'] = formatted_date
                df_cities['campaign_id'] = campaign_id
                df_cities['campaign_name'] = campaigns[campaign_id]
                df_cities = df_cities.drop('percent', axis=1)
                df_cities.rename(columns={'name': 'city'}, inplace=True)

                df_cities.to_sql('qr_studio_cities', engine, schema='housten_final',
                                 if_exists='append', index=False)

        except ValueError:
            print(f"The file {xls} does not have 'cities' sheet!")

    connection.close()


