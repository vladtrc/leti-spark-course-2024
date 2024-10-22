from pyspark.shell import spark
from pyspark.sql import DataFrame

from common import read_csv, view


#
# Требования:
#
# N - номер (сортируем по количеству игр)
#
# name - имя
#
# pos - в каждой игре участвует 10 игроков (5 в каждой команде)
# в каждой команде игроки занимают "позицию" от 1 до 5.
# говорят, например, в этой игре он играет на ЧЕТВЕРТОЙ позиции
# вот в этой колонке требуется посчитать на какой позиции человек играл больше всего
#
# kda - среднее число kills/deaths/assists
#
# avg_gold - среднее количество gold
#
# winrate - процент побед игрока на протяжении всех его игр
#

def solve() -> DataFrame:
    match = read_csv('match')
    player = read_csv('player')
    player_result = read_csv('player_result')

    view("match", match)
    view("player", player)
    view("player_result", player_result)

    # very easy
    view("kda", """
        select 
            player_id, 
            cast(avg(gold / 1000) as int) as avg_gold_k,
            cast(avg(kill) as int) as avg_kills,
            cast(avg(death) as int) as avg_death,
            cast(avg(assist) as int) as avg_assists 
        from player_result 
        group by player_id
    """)

    # not hard
    view("winrate", """
        select
            player_id,
            int(100 * avg(cast(player_result.is_radiant = match.radiant_won as int))) as winrate,
            count(player_id) as number_of_matches
        from player_result 
            left join match on match.match_id = player_result.match_id 
        group by player_id 
    """)

    # still not hard but takes a while
    view("pos_cnt", """
        select 
            player_id, pos, count(pos) as pos_cnt
        from player_result
        group by player_id, pos
    """)

    view("max_pos_cnt", """
        select 
            player_id, 
            pos, 
            pos_cnt,
            max(pos_cnt) over (partition by player_id) as max_pos_cnt
        from pos_cnt
    """)

    view("pos", """
        select 
            player_id, 
            max(pos) as pos 
        from max_pos_cnt 
        where max_pos_cnt = pos_cnt
        group by player_id
    """)

    # aggregate and prettify
    view("res", """
        select 
            row_number() over (partition by 1 order by number_of_matches desc) AS N,
            name,
            pos.pos as pos,
            concat(avg_kills, '/', avg_death, '/', avg_assists) as kda,
            concat(avg_gold_k, 'k') as avg_gold,
            concat(winrate.winrate, '%/', number_of_matches) as winrate
        from player
            join kda on kda.player_id = player.player_id
            join winrate on winrate.player_id = player.player_id
            join pos on pos.player_id = player.player_id
        order by number_of_matches desc
    """)
    return spark.sql("select * from res")
