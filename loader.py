from pyspark.sql.connect.session import SparkSession
from pyspark.sql.types import StructType


def load_data(session: SparkSession,file_path: str, schema: StructType):
    df = session.read.csv(file_path, header=True, sep="\t", schema=schema)
    return df