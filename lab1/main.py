from pyspark.shell import spark

from lab1.common import run_solution

# print(run_solution('solution_9091_ivanovii'))

# df = spark.read('data/match.csv')


df = spark.read \
    .format("com.databricks.spark.csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"data/player.csv")

df.show()
