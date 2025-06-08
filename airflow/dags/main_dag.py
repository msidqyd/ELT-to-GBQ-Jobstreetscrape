import pytz
from datetime import datetime
import yaml
from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator, get_current_context
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash import BashOperator
from resources.scripts.Scrape_Jobstreet.extract_csv import extract_csv
from resources.scripts.Scrape_Jobstreet.extract_json import extract_json
from resources.scripts.Scrape_Jobstreet.load import load
from resources.scripts.Scrape_Jobstreet.transform import transformation
from resources.scripts.Scrape_Jobstreet.validation import validation_data

folder_staging = "/opt/airflow/dags/Staging_Data"
Project = 'citric-bee-460614-c9'

# Load source list
with open("dags/resources/dynamic-dag/list_source.yaml") as f:
    list_data_source = yaml.safe_load(f)

# Create dag with schedule at 21:15 everyday, also i add the config parameter for reducing manual intervention.
def create_elt_dag(source_data):
    @dag(
        dag_id=f"Main_ELT_GCP_{source_data}",
        schedule_interval="15 21 * * *",
        start_date=datetime(2025, 6, 7, tzinfo=pytz.timezone("Asia/Jakarta")),
        catchup=False,
        tags=["ELT_Scrape", "To_GBQ"],
        params={
            "source_type": "CSV",
            "load_type": Param("incremental", description="incremental/full", enum=["full", "incremental"]),
            "table_date": Param(datetime.today().strftime('%Y-%m-%d'), description="Choose Date Table for ELT To GBQ"),
            "run_scrape": Param("skip scrape", description="run scrape/skip scrape", enum=["run scrape", "skip scrape"])
        }
    )
    def elt_task():
        wait_scrape_task = EmptyOperator(task_id="wait_scrape_task", trigger_rule=TriggerRule.ONE_SUCCESS)
        wait_extract_task = EmptyOperator(task_id="wait_extract_task", trigger_rule=TriggerRule.ONE_SUCCESS)
        wait_load_task = EmptyOperator(task_id="wait_load_task")
        wait_transform_task = EmptyOperator(task_id="wait_transform_task")
        end_task = EmptyOperator(task_id="end_task")
        skip_scrape = EmptyOperator(task_id="skip_scrape")
        #Get country from data source name.
        country_choosen = source_data.split("_")[-1]

        #Branch operator for choosing want to run the scrape or not.
        def run_scrape():
            context = get_current_context()
            scrape_decide = context["params"]["run_scrape"]
            return "skip_scrape" if scrape_decide == "skip scrape" else "scrape"

        decide_scrape = BranchPythonOperator(
            task_id="run_scrape",
            python_callable=run_scrape
        )

        scrape = BashOperator(
            task_id="scrape",
            bash_command=f"python /opt/airflow/dags/resources/scripts/Scrape_Jobstreet/scrape.py --country_choosen {country_choosen}",
        )

        decide_scrape >> [scrape, skip_scrape]
        [scrape, skip_scrape] >> wait_scrape_task

        #Branch operator to choose the user want to extract the csv file or json file.
        def choose_branch_extract(**kwargs):
            source_type = kwargs["params"].get("source_type", "csv")
            return "extract_csv" if source_type.lower() == "csv" else "extract_json"

        branch = BranchPythonOperator(
            task_id="choose_branch",
            python_callable=choose_branch_extract
        )

        wait_scrape_task >> branch

        @task(task_id="extract_csv")
        def run_extract_csv():
            context = get_current_context()
            Project = 'citric-bee-460614-c9'
            params = context["params"]
            return extract_csv(source_data, params["table_date"], params["load_type"], Project)

        @task(task_id="extract_json")
        def run_extract_json():
            context = get_current_context()
            Project = 'citric-bee-460614-c9'
            params = context["params"]
            return extract_json(source_data, params["table_date"], params["load_type"], Project)

        @task(task_id="load_to_gbq")
        def run_load_to_gbq():
            context = get_current_context()
            params = context["params"]
            Project = 'citric-bee-460614-c9'
            return load(params["table_date"], params["load_type"], Project,source_data)

        @task(task_id="transform")
        def run_transform():
            Project = 'citric-bee-460614-c9'
            return transformation(source_data, Project)

        @task(task_id="validation_data")
        def run_validation():
            Project = 'citric-bee-460614-c9'
            validation_data(source_data, Project)

        extract_csv_task = run_extract_csv()
        extract_json_task = run_extract_json()
        load_task = run_load_to_gbq()
        transform_task = run_transform()
        validation_task = run_validation()
        
        branch >> extract_csv_task >> wait_extract_task
        branch >> extract_json_task >> wait_extract_task

        wait_extract_task >> load_task >> wait_load_task
        wait_load_task >> transform_task >> wait_transform_task
        wait_transform_task >> validation_task >> end_task

    return elt_task()

# Create DAGs dynamically from source list folder.
for source_data in list_data_source:
    dag_id = f"ELT_{source_data}"
    globals()[dag_id] = create_elt_dag(source_data)
