from functools import reduce

from assertpy import assert_that
from pyspark.sql import functions as F

from lab2.common import run_solution, list_solutions


def union_all(dfs):
    """
    Объединяет все DataFrame из списка в один DataFrame с помощью union.

    :param dfs: Список DataFrame для объединения
    :return: Один объединённый DataFrame
    """
    if not dfs:
        raise ValueError("The list of DataFrames is empty.")
    return reduce(lambda df1, df2: df1.union(df2), dfs)


def get_match_value(df, lhs_author, rhs_author):
    """
    Возвращает значение 'match' для указанных комбинаций lhs_author и rhs_author.

    :param df: DataFrame
    :param lhs_author: Значение столбца lhs_author
    :param rhs_author: Значение столбца rhs_author
    :return: Значение 'match'
    """
    result = (df.filter((F.col('lhs_author') == lhs_author) & (F.col('rhs_author') == rhs_author))
              .select('match').collect())
    if result:
        return result[0][0]
    else:
        raise ValueError(f"No match found for {lhs_author} and {rhs_author}")


all_dfs = []
for solution in list_solutions('lab2/solutions'):
    try:
        df = run_solution(solution)
        all_dfs += [df.withColumn('author', F.lit(solution))]
        assert_that(get_match_value(df,         'none',  'almost_none')).is_greater_than(0.95) #                  1.0|
        assert_that(get_match_value(df,'9090_ivanovii',     'template')).is_greater_than(0.85) #   0.9189842805320435|
        assert_that(get_match_value(df,     'template','9090_ivanovii')).is_greater_than(0.4)  #   0.6375838926174496|
        assert_that(get_match_value(df,'9091_ivanovii','9090_ivanovii')).is_greater_than(0.4)  #   0.5264900662251656|
        assert_that(get_match_value(df,'9091_ivanovii',     'template')).is_less_than(0.5)     #  0.38741721854304634|
        assert_that(get_match_value(df,  'almost_none','9091_ivanovii')).is_less_than(0.5)     #   0.3670886075949367|
        assert_that(get_match_value(df,  'almost_none',     'template')).is_less_than(0.5)     #  0.35443037974683544|
        assert_that(get_match_value(df,  'almost_none','9090_ivanovii')).is_less_than(0.5)     #  0.35443037974683544|
        assert_that(get_match_value(df,  'almost_none',         'none')).is_less_than(0.4)     #   0.3037974683544304|
        assert_that(get_match_value(df,'9090_ivanovii','9091_ivanovii')).is_less_than(0.2)     #  0.19226118500604594|
        assert_that(get_match_value(df,     'template','9091_ivanovii')).is_less_than(0.2)     #  0.09815436241610738|
        assert_that(get_match_value(df,'9091_ivanovii',  'almost_none')).is_less_than(0.2)     #  0.09602649006622517|
        assert_that(get_match_value(df,'9090_ivanovii',  'almost_none')).is_less_than(0.1)     #  0.03385731559854897|
        assert_that(get_match_value(df,     'template',  'almost_none')).is_less_than(0.1)     #  0.02348993288590604|
        assert_that(get_match_value(df,     'template',         'none')).is_close_to(0.0, 0.1) #                  0.0|
        assert_that(get_match_value(df,         'none','9091_ivanovii')).is_close_to(0.0, 0.1) #                  0.0|
        assert_that(get_match_value(df,'9091_ivanovii',         'none')).is_close_to(0.0, 0.1) #                  0.0|
        assert_that(get_match_value(df,         'none','9090_ivanovii')).is_close_to(0.0, 0.1) #                  0.0|
        assert_that(get_match_value(df,'9090_ivanovii',         'none')).is_close_to(0.0, 0.1) #                  0.0|
        assert_that(get_match_value(df,         'none',     'template')).is_close_to(0.0, 0.1) #                  0.0|
        assert_that(df.count()).is_equal_to(20)
        print(solution, 'ok')
    except Exception as e:
        print(solution, 'fail', e)

final_df = union_all(all_dfs)
final_df.orderBy(F.col('match').desc()).show()
