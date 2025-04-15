from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col

def print_data_schema(df):
    df.printSchema()
    df.show(5)

def get_stats(df):
    num_columns = len(df.columns)
    print(f"Number of columns: {num_columns}")
    num_rows = df.count()
    print(f"Number of rows: {num_rows}")
    print_data_schema(df)


    numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, IntegerType)]
    if numeric_cols:
        print("\nDescriptive statistics for numeric columns:")
        df.select([col(c).cast("double") for c in numeric_cols]) \
            .summary("count", "mean", "stddev", "min", "max") \
            .show()
    else:
        print("\nNo numeric (integer/long) columns found.")

