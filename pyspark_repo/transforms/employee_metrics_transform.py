from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F 
from pyspark.sql.window import Window

def build_employee_metrics(
    spark: SparkSession,
    hris_table: str,
    perf_table: str,
    snapshot_date: str,
) -> DataFrame:
    
    # Read tables (filter is like "where" in sql)
    hris = spark.table(hris_table).filter(
        F.col("is_active") == True
    )
    perf = spark.table(perf_table).filter(
        F.col("review_cycle") == snapshot_date
    )

    # Compute tenure days
    hris = hris.withcolumn(
        "tenure_days",
        F.datediff(F.lit(snapshot_date), F.col("hire_date"))
    )

    # Join the two tables
    df = hris.join(perf, on="employee_id", how="left")

    # Compute team level average performance with the headcount
    df = df.groupBy("year", "department", "recruiter").agg(
        F.count("employee_id").alias("headcount"),
        F.avg("perf_score").alias("avg_perf_score")
    )

    '''
    other ways to do it
    team_window = Window.partitionBy("team")
    df = df.withColumn(
        "team_avg_perf_score",
        F.round(F.avg('perf_score').over(team_window), 2)
    )
    '''

    df = df.select(
        "employee_id",
        "full_name",
        "team",
        "level",
        "hire_date",
        "tenure_days",
        "perf_score",
        "team_avg_perf_score",
        F.lit(snapshot_date).alias("snapshot_date"),
    )

    return df
    