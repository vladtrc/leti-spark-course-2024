from pyspark.sql import DataFrame
from pyspark.sql import functions as F, Window

from lab1.common import read_csv, view


#
# Работа студента группы 9091 Иванова Ивана Ивановича
# Я все доделаю ЧЕСТНО
#

def solve() -> DataFrame:
    match = read_csv('match')
    player = read_csv('player')
    player_result = read_csv('player_result')

    view("match", match)
    view("player", player)
    view("player_result", player_result)

    kda = player_result.groupBy('player_id').agg(
        (F.mean('gold') / 1000).alias('avg_gold'),
        F.count('assist').alias('assist_cnt')
    )
    kda.show()
    return None
