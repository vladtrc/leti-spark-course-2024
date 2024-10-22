from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, sha, lit, udf
from pyspark.sql.types import StringType, ArrayType

from lab2.common import SparkContextCommon, run_solution


def clean_data(inp: str) -> str:
    return "".join(inp.split())


clean_data_udf = udf(lambda inp: clean_data(inp), StringType())


#
# match
# lhs_author
# rhs_author
#
def solve(common: SparkContextCommon) -> DataFrame:
    inp = common.read_data()
    df = inp.withColumn('content', clean_data_udf(col('content')))
    df.show()
    res_sql = """
        select 
            cast(match as Double) as match, 
            cast(lhs_author as String) as lhs_author, 
            cast(rhs_author as String) as rhs_author
        from res
    """
    return common.spark.sql(res_sql)
