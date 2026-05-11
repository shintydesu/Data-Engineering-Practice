'''
    This is to enforce the schema for the table
'''

from pyspark.sql.types import(
    StructType, StructField,
    StringType, IntegerType, FloatType, DateType, BooleanType
)

EMPLOYEE_METRICS_SCHEMA = StructType([
    StructField("employee_id", StringType(), nullable = False),
    StructField("full_name", StringType(), nullable=True),
    StructField("team", StringType(), nullable=True),
    StructField("level", StringType(), nullable=True),
    StructField('hire_date', DateType(), nullable=True),
    StructField("Tenure_days", IntegerType(), nullable=True),
    StructField("perf_score", FloatType(), nullable=True),
    StructField("team_avg_perf_score", FloatType(), nullable=True),
])