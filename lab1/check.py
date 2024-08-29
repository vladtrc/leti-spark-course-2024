from pyspark.testing import assertDataFrameEqual

from common import run_solution, list_solutions

template_df = run_solution('solution_template')
for solution in list_solutions('lab1/solutions'):
    try:
        df = run_solution(solution)
        # проще читать логи без этой колонки, с ней огромные diffы
        # assertDataFrameEqual(df.drop('N'), template_df.drop('N'))
        assertDataFrameEqual(df, template_df)
        print(solution, 'OK')
    except Exception as e:
        print(f'{solution} WRONG')
