from airflow.models import DagBag

def test_dag_loads_without_errors():
    """check import errors"""
    dagbag = DagBag(include_examples=False)
    assert "people_science__employee_metrics" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_dag_has_correct_schedule():
    dagbag = DagBag(include_examples=False)
    dag = dagbag.dags["people_science__employee_metrics"]
    assert dag.schedule_interval == "0 6 * * *"


def test_dag_has_one_task():
    dagbag = DagBag(include_examples=False)
    dag = dagbag.dags["people_science__employee_metrics"]
    assert len(dag.tasks) == 1
    assert dag.tasks[0].task_id == "run_employee_metrics_job"