import requests
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time
import os
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

#available to google drive
SCOPES = ['https://www.googleapis.com/auth/drive']


all_companies_data = []
data = []

ticket_url_base = "https://stooq.pl/t/?i=513&v=0&l="
max_attempts = 100

def scrape_wig_company_list(base_url=ticket_url_base):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # chrome driver inizialization
    driver = webdriver.Chrome(options=chrome_options)
    print("DEBUG: Chrome browser driver has been launched.")

    cookies_handled = False # New variable to track if cookies have been handled

    for i in range(1, max_attempts):
        full_url = f"{base_url}{i}"
        print(f"Checking URL: {full_url}")

        try:
            driver.get(full_url)
            print(f"DEBUG: Załadowano URL: {full_url}")

            # --- Cookies ---
            if not cookies_handled: # Only attempt to handle cookies if they haven't been handled yet
                try:
                    print("DEBUG: Trying to find and click the 'I agree' consent button...")
                    accept_button = WebDriverWait(driver, 20).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[aria-label="Zgadzam się"]'))
                    )
                    print("DEBUG: Cookie acceptance button found. Clicking...")
                    accept_button.click()
                    print("DEBUG: You successfully clicked the 'I agree' button.")

                    #wait till banner fade
                    WebDriverWait(driver, 10).until(
                        EC.invisibility_of_element_located((By.CLASS_NAME, "fc-consent-root"))
                    )
                    print("DEBUG: The cookie banner is gone.")
                    time.sleep(2)
                    cookies_handled = True # Set to True after successful handling

                except TimeoutException:
                    print("Warning: TimeoutException: Could not find/click on cookie banner (maybe it was missing or the selector was bad). Continuing.")
                    cookies_handled = True # Assume handled if not found, to avoid repeated attempts
                except Exception as consent_e:
                    print(f"Warning: Another error occurred while handling consent: {consent_e}. Continued.")
                    cookies_handled = True # Assume handled on error, to avoid repeated attempts

            html_content = driver.page_source
            soup = BeautifulSoup(html_content, 'html.parser')
            company_table = soup.find('table', id='fth1')
            if not company_table:
                print(f"INFO: Company table not found on page {full_url}. Stopping scraping.")
                break

            tbody = company_table.find('tbody')
            if not tbody:
                print("ERROR: Cannot find body in table.")
                break
            
            initial_companies_count = len(all_companies_data) # Store count before processing current page

            for row in tbody.find_all('tr'):
                cols = row.find_all('td')
                if len(cols) < 2:
                    continue # Skip rows that don't have enough columns

                ticket = None
                company_name = None

                # Extract Symbol from first column (cols[0])
                # Symbol is in <a> tag inside first <td>
                ticket_element = cols[0].find('a')
                if ticket_element:
                    ticket = ticket_element.get_text(strip=True)
                else:
                # Additional debugging if symbol is not found
                    pass

                # Extract Company Name from second column (cols[1])
                # Company Name is directly in second <td>
                company_name_element = cols[1]
                if company_name_element:
                    company_name = company_name_element.get_text(strip=True)
                else:
                    # Additional debugging if name is not found
                    pass

                # Add data to global list only if both elements are found
                if company_name and ticket:
                    all_companies_data.append({'Company Name': company_name, 'Ticket': ticket})
            
            # Check if any new companies were added from this page
            if len(all_companies_data) == initial_companies_count:
                print(f"INFO: No new company symbols found on page {full_url}. Stopping scraping.")
                break # Stop if no new symbols were found on the current page

        except Exception as e:
            print(f"ERROR: An unexpected error occurred while processing the URL. {full_url}: {e}")

    driver.quit()
    return pd.DataFrame(all_companies_data)

# function
df_tickets = scrape_wig_company_list()
if df_tickets is not None:
    print(df_tickets)
    print(f"Collected {len(df_tickets)} firms.")
else:
    print("Could not collect data about companies.")
    

    
def upload_dataframe_to_drive(df, filename, mime_type):
    creds = None
    try:
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    except FileNotFoundError:
        pass

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('drive', 'v3', credentials=creds)

    # 1. Convert DF to CSV in memory
    csv_data = df.to_csv(index=False)
    media = MediaIoBaseUpload(io.BytesIO(csv_data.encode('utf-8')), mimetype=mime_type, resumable=True)
    
    folder_id = "1ErDbOslxU61X6rZfT33VcFuz2G5nZ3ZS"

    # 2. We are looking to see if a file with this name already exists in this folder
    query = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])

    if files:
        # FILE EXISTS -> UPDATE
        file_id = files[0]['id']
        file = service.files().update(fileId=file_id, media_body=media).execute()
        print(f'File"{filename}" has beed updated (ID: {file_id}).')
    else:
        # FILE DOES NOT EXIST -> CREATE
        file_metadata = {'name': filename, 'parents': [folder_id]}
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f'File "{filename}" has been created (ID: {file.get("id")}).')

upload_dataframe_to_drive(df_tickets, 'gpw_tickets.csv', 'text/csv')