import pyqrlew as pq
import numpy as np
from typing import Callable
import matplotlib.pylab as plt

DBNAME: str = 'pyqrlew-db'
USER: str = 'postgres'
PASSWORD: str = 'pyqrlew-db'
PORT: str = 5433
N_SIM = 500


def run_test(stochastic_tester: pq.tester.StochasticTester, query: str, adj_query_func: Callable[[int], str], name: str, n_sim: int):
    print(f"Query = {query}")
    dp_results = stochastic_tester.utility_per_epsilon(query, epsilons=[1., 5., 10.], n_sim=n_sim)
    true_res = database.execute(query)[0][0]
    plt.figure(figsize=(6, 15))
    plt.subplot(3, 1, 1)
    pq.tester.plot_utility(dp_results=dp_results, true_res=true_res)

    epsilon = 1.
    ref_res = stochastic_tester.compute_results(query, epsilon=epsilon, n_sim=n_sim)
    adj_res = [
        stochastic_tester.compute_results(adj_query_func(i), epsilon=epsilon, n_sim=n_sim)
        for i in [np.random.uniform(0, 100) for _ in range(1)]
    ]
    plt.subplot(3, 1, 2)
    pq.tester.plot_adjacent_histograms(ref_res, adj_res[0], epsilon=epsilon, delta=stochastic_tester.delta)

    plt.subplot(3, 1, 3)
    pq.tester.plot_privacy_profile(ref_res, adj_res)

    plt.suptitle(query)
    plt.tight_layout()
    plt.savefig(name)
    print("================================")

def test_sum(stochastic_tester, n_sim):
    def write_inner_query(d, a, b):
        return f"SELECT {d} as x FROM testing.table1 WHERE {d} > {a} AND {d} < {b}"

    distributions = [
        ('laplace', -2.5, 2.5),
        ('gaussian', -2., 2.),
        ('uniform', -1., 1.),
        ('halton', -1., 1.),
    ]

    for (d, a, b) in distributions:
        inner_query = write_inner_query(d, a, b)
        query = f"SELECT SUM(x) AS my_sum FROM ({inner_query}) AS my_table"
        adj_query = lambda id: f"SELECT SUM(x) AS my_sum FROM ({inner_query} AND id != {id}) AS my_table"
        run_test(stochastic_tester, query=query, adj_query_func=adj_query, name=f'figures/sum_{d}.png', n_sim=n_sim)

def test_avg(stochastic_tester, n_sim):
    def write_inner_query(d, a, b):
        return f"SELECT {d} as x FROM testing.table1 WHERE {d} > {a} AND {d} < {b}"

    distributions = [
        ('laplace', -2.5, 2.5),
        ('gaussian', -2., 2.),
        ('uniform', -1., 1.),
        ('halton', -1., 1.),
    ]

    for (d, a, b) in distributions:
        inner_query = write_inner_query(d, a, b)
        query = f"SELECT AVG(x) AS my_sum FROM ({inner_query}) AS my_table"
        adj_query = lambda id: f"SELECT AVG(x) AS my_sum FROM ({inner_query} AND id != {id}) AS my_table"
        run_test(stochastic_tester, query=query, adj_query_func=adj_query, name=f'figures/avg_{d}.png', n_sim=n_sim)

def test_count(stochastic_tester, n_sim):
    query = f"SELECT COUNT(x) AS my_sum FROM (SELECT laplace AS x FROM testing.table1) AS my_table"
    adj_query = lambda id: f"SELECT COUNT(x) AS my_sum FROM (SELECT laplace AS x FROM testing.table1 WHERE id != {id}) AS my_table"
    run_test(stochastic_tester, query=query, adj_query_func=adj_query, name='figures/count.png', n_sim=n_sim)


if __name__ == "__main__":
    database = pq.tester.StochasticDatabase('testing', DBNAME, USER, PASSWORD, PORT)
    print('creating tables ...')
    size = 100
    ## create table
    column_specs = [
        pq.tester.ColumnSpec(name="laplace", distribution=pq.tester.Distribution.LAPLACE, loc=0, scale=1),
        pq.tester.ColumnSpec(name="uniform", distribution=pq.tester.Distribution.UNIFORM, low=-1, high=1),
        pq.tester.ColumnSpec(name="gaussian", distribution=pq.tester.Distribution.GAUSSIAN, loc=0, scale=1),
        pq.tester.ColumnSpec(name="halton", distribution=pq.tester.Distribution.HALTON, base=2, low=-1, high=1),
    ]
    database.create_table("table1", size, column_specs, True)
    column_specs = [
        pq.tester.ColumnSpec(name="laplace", distribution=pq.tester.Distribution.LAPLACE, loc=0, scale=1),
        pq.tester.ColumnSpec(name="uniform", distribution=pq.tester.Distribution.UNIFORM, low=-1, high=1),
        pq.tester.ColumnSpec(name="gaussian", distribution=pq.tester.Distribution.GAUSSIAN, loc=0, scale=1),
        pq.tester.ColumnSpec(name="halton", distribution=pq.tester.Distribution.HALTON, base=2, low=-1, high=1),
    ]
    database.create_table("table2", size, column_specs, False)
    print('done')
    protected_entity = [
        ("table1", [], "id"),
    ]
    synthetic_data = [
        (["testing", "table1"], ["testing", "table1"]),
    ]
    stochastic_tester = pq.tester.StochasticTester(database, 1 / size, protected_entity, synthetic_data)
    n_sim = 100
    test_count(stochastic_tester, n_sim)
    #test_sum(stochastic_tester, 1000)
    #test_avg(stochastic_tester, 1000)







