from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql import functions as F

def enforce_schema(df: DataFrame, schema: StructType) -> DataFrame:
    schema_fields = {field.name: field.dataType for field in schema.fields}
    missing = set(schema_fields.keys()) - set(df.columns)

    if missing:
        raise ValueError(f"Missing columns in DataFrame: {missing}")
    
    return df.select([
        F.col(name).cast(dtype).alias(name)
        for name, dtype in schema_fields.items()
    ])

def validate_no_nulls(df:DataFrame, columns: list) -> None:
    for col in columns:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            raise ValueError(
                f"Column '{col}' has {null_count} null values. \nExpected zero nulls for this field"
            )