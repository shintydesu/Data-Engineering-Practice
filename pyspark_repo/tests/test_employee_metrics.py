import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from transforms.employee_metrics_transform import build_employee_metrics

@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.master("local").appName("test").getOrCreate()

def test_tenure_days_computed_correctl(spark):
    
    # Build a tiny mock table for test
    hris_data = [
        ("E001", "Alice", "Engineering", "L4", "2022-01-01", True),
        ("E002", "Bob", "Design", "L3", "2023-06-15", True),
        ("E003", "Carol", "Engineering", "L5", "2020-03-10", False),        
    ]

    hris_df = spark.createDataFrame(hris_data,
    ["employee_id", "full_name", "team", "level", "hire_date", "is_active"])

    perf_data = [
        ("E001", 4.2, "2024-06-30"),
        ("E002", 3.8, "2024-06-30"),        
    ]

    perf_df = spark.createDataFrame(perf_data,
        ["employee_id", "perf_score", "review_cycle"])
    
    result = build_employee_metrics(
        spark, hris_df, perf_df, snapshot_date="2024-06-30"
    )

    # test if these hold true
    assert result.count() == 2    

    alice = result.filter(result.employee_id == "E001").first()
    assert alice.tenure_days == 911

    assert alice.team_avg_perf_score == 4.2