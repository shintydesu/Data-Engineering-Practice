from pyspark.sql import SparkSession, DataFrame

def get_spark_session(app_name: str) -> SparkSession:

    return(
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

def write_delta_table(
    df: DataFrame,
    output_path: str,
    partition_by: str,
    mode: str = "overwrite",
) -> None:
    
    (
        df.write.format("delta")
        .format("delta")
        .mode(mode)
        .partitionBy(partition_by)
        .saveAsTable(output_path)
    )

    print(f"Wrote to {output_path} (mode={mode}, partition={partition_by})")