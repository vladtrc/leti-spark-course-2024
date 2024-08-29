from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common import read_csv


#
# Работа студента группы 9091 Иванова Ивана Ивановича
# Я все доделаю ЧЕСТНО
#

def solve() -> DataFrame:
    match: DataFrame = read_csv('match')
    player: DataFrame = read_csv('player')
    player_result: DataFrame = read_csv('player_result')

    kda = player_result.groupBy('player_id').agg(
        (F.mean('gold') / 1000).alias('avg_gold'),
        F.count('assist').alias('assist_cnt')
    )
    return kda
