
# Dynamic ELT Jobstreet Data using Airflow and GCP 

## 1. Objectives
- Build a modular and scalable ELT pipeline from jobstreet source.
- Automate job data collection from JobStreet with choosen keywords.
- Perform transformations and load data into Google Cloud BigQuery.

## 2. Tech Stacks
- 🐍 Python 3.10
- 🌬️ Apache Airflow (Dockerized)
- 📄 Parquet, Pandas
- 🛢️ Google BigQuery
- 🌐 Selenium for scraping
- 🐳 Docker + Docker Compose

# 3. Architecture Diagram
![Screenshot 2025-06-07 223027](https://github.com/user-attachments/assets/72afc5f6-986f-4a80-bfac-e1b009ff12a9)

# 4. Control Flow Graph
![Screenshot 2025-06-07 223307](https://github.com/user-attachments/assets/940d0e45-e8c8-4c0e-832a-5027832a603d)

## Pipeline Features
- Scraping module using Selenium (JobStreet)
- Branching DAGs for dynamic input format handling
- Incremental data loading logic
- Data validation tasks after loading
- Choose table based on date for ELT.
- Stored intermediate files in local staging as Parquet
# 5. How to Run
```bash
# 1. Clone the repo
git clone https://github.com/msidqyd/elt-jobstreet-gcp-pipeline.git
cd elt-jobstreet-gcp-pipeline

# 2. Start Airflow
docker compose --profile airflow up -d --build

# 3. Start Airflow
# Replace with your credential
Project = 'your_credential'                                             # At Main_DAG
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/dags/your_credential.json   # At .ENV

# 4. Access Airflow UI
http://localhost:8080
```
# 6. Example Screenshoot
#### Configurable Parameters
which the user could choose:
- source type CSV/JSON
- load type full/incremental
- table date, which table want to ingest
- also, run the scrape or skip the scrape
below is the example of new day, so run scrape and choose table date same time the data scraped
![Screenshot 2025-07-03 152334](https://github.com/user-attachments/assets/da1ca880-553e-461a-92f0-8c61fbd8014b)

### Airflow DAG Logs 
![Screenshot 2025-07-03 151756](https://github.com/user-attachments/assets/997fa145-f643-4ef4-a8b7-4cee943d0018)
![Screenshot 2025-07-03 163758](https://github.com/user-attachments/assets/f90be262-8c68-4a67-93fd-0e4c7c7ae9df)


### Records before incremental Load
![Screenshot 2025-07-03 144526](https://github.com/user-attachments/assets/136dd928-0b66-4522-b579-2349948758b7)
![Screenshot 2025-07-03 152259](https://github.com/user-attachments/assets/5a7612e9-1669-4fc6-b12b-c2d1d1d8751e)

### Records after incremental Load
![Screenshot 2025-07-03 151122](https://github.com/user-attachments/assets/ae41506d-b958-48f2-8a00-91a0d37ec353)
![Screenshot 2025-07-03 163821](https://github.com/user-attachments/assets/1f533f81-e9a9-451c-b929-a1cf62551499)


### Table before & after incremental
![Screenshot 2025-07-03 152001](https://github.com/user-attachments/assets/726addb7-6468-474f-aa6c-b8ab283de4db)
![Screenshot 2025-07-03 194211](https://github.com/user-attachments/assets/8de93252-f182-468d-bcbe-6d52f2c631b3)


### Scrape Result by Keywords
- Big Data Engineer
- Data Engineer
- Data Platform Engineer
- ETL Developer
- Data Pipeline Developer
- Data Infrastructure Engineer
- Data Integration Engineer
- DataOps Engineer

https://lookerstudio.google.com/reporting/3b4f56b0-1baf-49dc-a967-d5c8abf51880

https://lookerstudio.google.com/reporting/c4302f79-56c6-4a07-b943-4bd44a9d0d71

## Data Validation
- ✅ Row count checks
- ✅ Schema matching
- ✅ Null/missing value reports


## Feel free to explore and adapt this pipeline for your own use cases
