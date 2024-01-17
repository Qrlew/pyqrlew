import pyqrlew as pq
import pyqrlew.tester as tester

DBNAME: str = 'pyqrlew-db'
USER: str = 'postgres'
PASSWORD: str = 'pyqrlew-db'
PORT: str = 5433

def test_sum(database, stochastic_tester, n_sim, distributions, name, row_privacy=True):
    tablename = 'table1' if row_privacy else 'table2'
    rp = 'row_privacy' if row_privacy else 'multi_rows'

    def write_inner_query(d, a, b):
        return f"SELECT {d} as x FROM testing.{tablename} WHERE {d} > {a} AND {d} < {b}"

    for (d, a, b) in distributions:
        inner_query = write_inner_query(d, a, b)
        query = f"SELECT SUM(x) AS my_sum FROM ({inner_query}) AS my_table"
        adj_query = lambda id: f"SELECT SUM(x) AS my_sum FROM ({inner_query} AND id != {id}) AS my_table"
        tester.run_test(database, stochastic_tester, query=query, adj_query_func=adj_query, name=f'{name}_{d}_{rp}', n_sim=n_sim)

def test_avg(database, stochastic_tester, n_sim, distributions, name, row_privacy=True):
    tablename = 'table1' if row_privacy else 'table2'
    rp = 'row_privacy' if row_privacy else 'multi_rows'

    def write_inner_query(d, a, b):
        return f"SELECT {d} as x FROM testing.{tablename} WHERE {d} > {a} AND {d} < {b}"

    for (d, a, b) in distributions:
        inner_query = write_inner_query(d, a, b)
        query = f"SELECT AVG(x) AS my_sum FROM ({inner_query}) AS my_table"
        adj_query = lambda id: f"SELECT AVG(x) AS my_sum FROM ({inner_query} AND id != {id}) AS my_table"
        tester.run_test(database, stochastic_tester, query=query, adj_query_func=adj_query, name=f'{name}_{d}_{rp}', n_sim=n_sim)

def test_count(database, stochastic_tester, n_sim, name, row_privacy=True):
    tablename = 'table1' if row_privacy else 'table2'
    rp = 'row_privacy' if row_privacy else 'multi_rows'

    query = f"SELECT COUNT(x) AS my_sum FROM (SELECT laplace AS x FROM testing.{tablename}) AS my_table"
    adj_query = lambda id: f"SELECT COUNT(x) AS my_sum FROM (SELECT laplace AS x FROM testing.{tablename} WHERE id != {id}) AS my_table"
    tester.run_test(database, stochastic_tester, query=query, adj_query_func=adj_query, name=f'{name}_{rp}', n_sim=n_sim)

def test_joins(database, stochastic_tester, n_sim, name):
    inner_query = "SELECT t1.halton + 2 * t2.halton as x FROM testing.table1 AS t1 JOIN testing.table2 AS t2 ON t1.id = t2.id WHERE t1.halton > -1 AND t1.halton < 1 AND t2.halton > -1 AND t2.halton < 1"
    query = f"SELECT SUM(x) AS my_sum FROM ({inner_query}) AS my_table"
    adj_query = lambda id: f"SELECT SUM(x) AS my_sum FROM ({inner_query} AND id != {id}) AS my_table"
    tester.run_test(database, stochastic_tester, query=query, adj_query_func=adj_query, name=name, n_sim=n_sim)

if __name__ == "__main__":
    database = tester.StochasticDatabase('testing', DBNAME, USER, PASSWORD, PORT)
    print('creating tables ...')
    size = 1000
    ## create table
    column_specs = [
        tester.ColumnSpec(name="laplace", distribution=tester.Distribution.LAPLACE, loc=0, scale=1),
        tester.ColumnSpec(name="uniform", distribution=tester.Distribution.UNIFORM, low=-1, high=1),
        tester.ColumnSpec(name="gaussian", distribution=tester.Distribution.GAUSSIAN, loc=0, scale=1),
        tester.ColumnSpec(name="halton", distribution=tester.Distribution.HALTON, base=2, low=-1, high=1),
    ]
    database.create_table("table1", size, column_specs, True)
    column_specs = [
        tester.ColumnSpec(name="laplace", distribution=tester.Distribution.LAPLACE, loc=0, scale=1),
        tester.ColumnSpec(name="uniform", distribution=tester.Distribution.UNIFORM, low=-1, high=1),
        tester.ColumnSpec(name="gaussian", distribution=tester.Distribution.GAUSSIAN, loc=0, scale=1),
        tester.ColumnSpec(name="halton", distribution=tester.Distribution.HALTON, base=2, low=-1, high=1),
    ]
    database.create_table("table2", size, column_specs, False)
    print('done')
    privacy_unit = [
        ("table1", [], "id"),
        ("table2", [], "id"),
    ]
    synthetic_data = [
        (["testing", "table1"], ["testing", "table1"]),
        (["testing", "table2"], ["testing", "table2"]),
    ]

    distributions = [
        # ('laplace', -2.5, 2.5),
        # ('gaussian', -2., 2.),
        # ('uniform', -1., 1.),
        ('halton', -1., 1.),
    ]
    row_privacy=False
    stochastic_tester = tester.StochasticTester(database, 1 / size, privacy_unit, synthetic_data)
    n_sim = 10000
    #test_count(database, stochastic_tester, n_sim, name="figures/count", row_privacy=row_privacy)
    test_sum(database, stochastic_tester, n_sim, distributions=distributions, name="figures/sum", row_privacy=row_privacy)
    # test_avg(database, stochastic_tester, n_sim, distributions=distributions, name="figures/avg", row_privacy=row_privacy)
    # test_joins(database, stochastic_tester, n_sim, name="figures/query_with_join")







