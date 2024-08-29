import importlib
from os import listdir
from os.path import isfile, join

from pyspark.shell import spark
from pyspark.sql import DataFrame
from pyspark.testing import assertDataFrameEqual


def view(name: str, target: DataFrame | str) -> DataFrame:
    df = target if isinstance(target, DataFrame) else spark.sql(target)
    df.createOrReplaceTempView(name)
    return df


def read_csv(name) -> DataFrame:
    return spark.read \
        .format("com.databricks.spark.csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(f"data/{name}.csv")


def run_solution(name):
    module = importlib.import_module(f'solutions.{name}')
    return getattr(module, 'solve')()


def list_solutions(path):
    files = [f for f in listdir(path) if isfile(join(path, f))]
    return [f.replace('.py', '') for f in files if 'template' not in f]


