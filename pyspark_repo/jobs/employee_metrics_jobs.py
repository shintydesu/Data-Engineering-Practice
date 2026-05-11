import yaml
from utils.spark_utils import get_spark_session, write_delta_table
from utils.schema_utils import enforce_schema, validate_no_nulls
from schemas.employee_metrics_schema import EMPLOYEE_METRICS_SCHEMA
from transforms.employee_metrics_transform import build_employee_metrics

def main():
    with open("configs/employee_metrics_config.yaml") as f:
        config = yaml.safe_load(f)

    spark = get_spark_session("people_science_employee_metrics")

    df = build_employee_metrics(
        spark=spark,
        hris_table=config["input_tables"]["hris"],
        perf_table=config["input_tables"]["performamce"],
        snapshot_data=config["snapshot_date"]
    )

    df = enforce_schema(df, EMPLOYEE_METRICS_SCHEMA)

    validate_no_nulls(df, columns=["employee_id", "snapshot_date"])

    write_delta_table(
        df=df,
        output_path=config["output_table"],
        partition_by="snapshot_date",
        mode=config.get("write_mode", "overwrite")
    )

    print(f"Done. Wrote {df.count()} rows to {config['output_table']}")

if __name__ == "__main__":
    main()

