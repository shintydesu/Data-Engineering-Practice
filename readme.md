# People Science — Data Engineering Practice

This project is to practice and simulate building production style data pipelines for the People Science team. The goal is to practice the data engineering side of the data science internship David mentioned I might be able to work on. From designing tables, writing PySpark transformations, and scheduling pipelines so that data scientists have clean, reliable data to work with. 

This is for the data scientists that need pre-built, well structured tables they can query directly in notebooks rather than joining raw HRIS and performance data themselves every time. (This is just a simulation so it could be well different during the internship).

This project simulates building those tables end-to-end. From reading raw source data, applying business logic in PySpark, to writing a Delta table on a daily schedule.

---

## Repository structure

There are two separate repositories as David mentioned where pyspark repo (business logic) and dag repo which controls the scheduling.

```
pyspark-repo/ # Business logic
├── jobs/
│   └── people_science/
│       └── employee_metrics_job.py # This gets submitted to the clusters
├── transforms/
│   └── people_science/
│       └── employee_metrics_transform.py # Actual transformations and joins are here
├── schemas/
│   └── people_science/
│       └── employee_metrics_schema.py # Output table schema (column names + types)
├── configs/
│   └── people_science/
│       └── employee_metrics_config.yaml
├── utils/
│   ├── spark_utils.py # Spark sessions
│   └── schema_utils.py # schema enforcement on table
├── tests/
│   └── people_science/
│       └── test_employee_metrics.py # Unit tests for transform logic
└── requirements.txt

dag-repo/ # Orchestration / scheduling
├── dags/
│   └── people_science/
│       └── employee_metrics_dag.py # Airflow DAG — runs job.py on a schedule
├── configs/
│   └── people_science/
│       └── employee_metrics_dag_config.yaml
├── tests/
│   └── people_science/
│       └── test_employee_metrics_dag.py
└── requirements.txt
```
---

## The table being built

`people_science.analytics.employee_metrics`

A daily snapshot table at the employee grain, joining HRIS data with performance review scores. Designed to give data scientists a single table for workforce analytics without needing to touch raw upstream tables.