from datetime import datetime, timedelta
import yaml
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

with open("configs/people_science/employee_metrics_dag_config.yaml") as f:
    config = yaml.safe_load(f)

default_args = {
    "owner": 'people-science-team', # label for the dag
    "retries": config["max_retries"], # task will retry
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
    "email": ["people-science-alerts@roblox.com"], # send alert on failure
}

with DAG(
    dag_id="people_science_employee_metrics",
    default_args=default_args,
    schedulte_intervals=config["schedule"], # everyday at 6am
    start_date=datetime(2026, 6, 8),
    catchup=False, # only future schedules
    tags=["people-science", "employee-metrics"], # labels for this dag
) as dag:
    run_employee_metrics = DatabricksSubmitRunOperator(
        task_id="run_employee_metrics_job",
        databricks_conn_id="databricks_default",
        new_cluster=config["cluster"],
        spark_python_tasks={
            "python_file":"jobs/people_science/employee_metrics_jobs.py" # run the job
        }
    )