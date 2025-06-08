import argparse
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
from datetime import datetime
from datetime import timedelta
from selenium.webdriver.support import expected_conditions as EC
from itertools import count
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--country_choosen", required=True, help="Country name")
    args = parser.parse_args()

    country_choosen = args.country_choosen
    print(f"Running scraper for: {country_choosen}")

    Search = [
    "Big Data Engineer",
    "Data Engineer",
    "Data Platform Engineer",
    "ETL Developer",
    "Data Pipeline Developer",
    "Data Infrastructure Engineer",
    "Data Integration Engineer",
    "DataOps Engineer"
    ]   
    def jobstreet_scrape():
        while Search:
            # I set every search keyword will scrape again id the result is none.
            keywords = Search.pop(0)
            words = keywords
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument('--headless')  
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("start-maximized")
            driver = webdriver.Chrome(options=chrome_options)

            country_map = {
            'indonesia': 'https://www.jobstreet.co.id/en',
            'malaysia': 'https://www.jobstreet.com.my/en',
            'philippines': 'https://www.jobstreet.com.ph/en',
            'singapore': 'https://www.jobstreet.com.sg/en',
        }
            #country choosen from file name in Yaml.
            chosen_country = country_choosen
            driver.get(country_map[chosen_country])
            #To make sure not blocked by the website.
            print(driver.page_source)
            field = WebDriverWait(driver,10).until(EC.presence_of_element_located((By.CSS_SELECTOR,"input[placeholder='Enter keywords']")))
            field.send_keys(words)

            search = WebDriverWait(driver,10).until(EC.presence_of_element_located((By.CSS_SELECTOR,"button[type='submit']")))
            search.click()

            job_list = []
            now = datetime.now()

            search_url = driver.current_url + "?page="
            #Load page untill found one page with none scrape result 
            for page in count(start=1):
                driver.get(f"{search_url}{page}")
                found_job = False
                job_elements = driver.find_elements(By.CSS_SELECTOR, 'div[data-search-sol-meta]')
                #the others element will find element inside each job_id structure, so the data will allign to job_id.
                for element in job_elements:
                    job_card = element.find_elements(By.CSS_SELECTOR, 'article[data-job-id]')
                    for card in job_card:
                        found_job = True
                        job_id = card.get_attribute("data-job-id")
                        #Convert the post information into time with calculation from time now.
                        try:
                            find_time = element.find_element(By.CSS_SELECTOR, 'span[data-automation="jobListingDate"]').text
                            get_number = int(''.join(filter(str.isdigit, find_time)))
                            if "m" in find_time:
                                posted_time =  now - timedelta(minutes = get_number)
                            elif "h" in find_time:
                                posted_time =  now - timedelta(hours = get_number)
                            elif "d" in find_time:
                                posted_time =  now - timedelta(days = get_number)
                            else :
                                posted_time = find_time
                            posted_time = posted_time.strftime('%Y-%m-%d %H:%M:%S')
                        except:
                            posted_time = None

                        try:
                            role = element.find_element(By.CSS_SELECTOR, 'a[data-automation="jobTitle"]').text
                        except:
                            role = None

                        try:
                            company = element.find_element(By.CSS_SELECTOR, 'a[data-automation="jobCompany"]').text
                        except:
                            company = None

                        try:
                            location = element.find_element(By.CSS_SELECTOR, 'a[data-automation="jobLocation"]').text
                        except:
                            location = None    
                            
                        try:
                            url = "https://id.jobstreet.com/id/job/" + job_id
                        except:
                            url = None

                        recap = {
                            "Job_ID": job_id,
                            "Role": role,
                            "Company" : company,
                            "Location" : location,
                            "Publish_Time": posted_time,      
                            "URL" : url
                        }

                        job_list.append(recap)
                if not found_job:
                    break
            df = pd.DataFrame(job_list)
            print(df.head())
            print(words)
            print("Length of df:", len(df))
            if len(df) == 0:
                Search.append(keywords)
            else:
                try:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    file_path_json = f"/opt/airflow/dags/Source_Data_jobstreetscrape_{chosen_country}/jobstreetscrape_{words.replace(' ','')}_{chosen_country}_{timestamp}.json"
                    df.to_json(file_path_json, orient='records', lines=True)
                    file_path_csv = f"/opt/airflow/dags/Source_Data_jobstreetscrape_{chosen_country}/jobstreetscrape_{words.replace(' ','')}_{chosen_country}_{timestamp}.csv"
                    df.to_csv(file_path_csv, index = False)

                except Exception as e:
                    raise RuntimeError(f"Load Section Has Error {e}")
    jobstreet_scrape()
                    


if __name__ == "__main__":
    main()