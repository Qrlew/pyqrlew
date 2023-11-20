import pyqrlew as pq
import numpy as np
from typing import List
import matplotlib.pylab as plt

DBNAME: str = 'pyqrlew-db'
USER: str = 'postgres'
PASSWORD: str = 'pyqrlew-db'
PORT: str = 5433
N_SIM = 500


class StochasticTester:
    def __init__(self, database, delta, privacy_unit, synthetic_data):
        self.database = database
        self.delta = delta
        self.privacy_unit = privacy_unit
        self.synthetic_data = synthetic_data

    def compute_dp_relation(self, query, epsilon):
        budget = {"epsilon": epsilon, "delta": self.delta}
        relation = self.database.dataset().sql(query)

        relation_with_privatequeries = relation.rewrite_with_differential_privacy(
            self.database.dataset(),
            self.synthetic_data,
            self.privacy_unit,
            budget
        )
        return relation_with_privatequeries.relation()

    def utility_per_epsilon(self, query: str, epsilons: List[float], n_sim: int):
        print('computing utility per epsilon ...')
        relations = {
            epsilon: self.compute_dp_relation(query, epsilon)
            for epsilon in epsilons
        }
        print('done')
        return {
            epsilon: [
                database.eval(dp_relation)[0][0]
                for _ in range(n_sim)
            ]
            for epsilon, dp_relation in relations.items()
        }

    def adjacent_databases(self, query: str, adj_query: str, epsilon: float, n_sim: int):
        print('test on adjacent databases ...')
        dp_relation = self.compute_dp_relation(query, epsilon)
        adj_dp_relation = self.compute_dp_relation(adj_query, epsilon)
        results = [
            database.eval(dp_relation)[0][0]
            for _ in range(n_sim)
        ]
        adj_results = [
            database.eval(adj_dp_relation)[0][0]
            for _ in range(n_sim)
        ]
        print('done')
        return results, adj_results


def plot_utility(dp_results: List[float], true_res: float):
    [
        plt.hist(results, bins=20, alpha=0.5, label = fr"$\varepsilon = {epsilon}$")
        for (epsilon, results) in dp_results.items()
    ]
    plt.axvline(true_res, color = 'red')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()


def plot_adjacent_histograms(array1: List[float], array2: List[float], epsilon:float, delta: float, bins:float=40, alpha=0.5, color1='blue', color2='orange'):
    xmin = min(min(array1), min(array2))
    xmax = max(max(array1), min(array2))
    bins = np.linspace(xmin, xmax, bins)
    values1, bins = np.histogram(array1, bins=bins)
    values1 = values1 / np.sum(values1)
    values2, _ = np.histogram(array2, bins=bins)
    values2 = values2 / np.sum(values2)
    x = bins[1:] + (bins[1] -bins[0])

    plt.bar(x, np.exp(epsilon) * values2 + delta, width=bins[1]-bins[0], alpha=alpha, color=color2, label=r'$e^{\varepsilon}P(f(\mathcal{D}_2)) + \delta$')
    plt.bar(x, values1, alpha=alpha, color=color1, width=bins[1]-bins[0], label=r'$P(f(\mathcal{D}_1))$')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()


def run_test(stochastic_tester: StochasticTester, query: str, adj_query: str, name: str, n_sim: int):
    print(f"Query = {query}")
    dp_results = stochastic_tester.utility_per_epsilon(query, epsilons=[1., 5., 10.], n_sim=n_sim)
    true_res = database.execute(query)[0][0]
    plt.figure(figsize=(12, 5))
    plt.subplot(1, 2, 1)
    plot_utility(dp_results=dp_results, true_res=true_res)

    epsilon = 1.
    res0, res1 = stochastic_tester.adjacent_databases(query, adj_query, epsilon=epsilon, n_sim=n_sim)
    plt.subplot(1, 2, 2)
    plot_adjacent_histograms(res0, res1, epsilon=epsilon, delta=stochastic_tester.delta)

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
        adj_query = f"SELECT SUM(x) AS my_sum FROM ({inner_query} AND id != 2) AS my_table"
        run_test(stochastic_tester, query=query, adj_query=adj_query, name=f'figures/sum_{d}.png', n_sim=n_sim)

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
        adj_query = f"SELECT AVG(x) AS my_sum FROM ({inner_query} AND id != 2) AS my_table"
        run_test(stochastic_tester, query=query, adj_query=adj_query, name=f'figures/avg_{d}.png', n_sim=n_sim)

def test_count(stochastic_tester, n_sim):
    query = f"SELECT COUNT(x) AS my_sum FROM (SELECT laplace AS x FROM testing.table1) AS my_table"
    adj_query = f"SELECT COUNT(x) AS my_sum FROM (SELECT laplace AS x FROM testing.table1 WHERE id != 2) AS my_table"
    run_test(stochastic_tester, query=query, adj_query=adj_query, name='figures/count.png', n_sim=n_sim)


if __name__ == "__main__":
    database = pq.stochatic_dataset.StochasticDatabase('testing', DBNAME, USER, PASSWORD, PORT)
    print('creating tables ...')
    ## create table
    column_specs = [
        pq.stochatic_dataset.ColumnSpec(name="laplace", distribution=pq.stochatic_dataset.Distribution.LAPLACE, loc=0, scale=1),
        pq.stochatic_dataset.ColumnSpec(name="uniform", distribution=pq.stochatic_dataset.Distribution.UNIFORM, low=-1, high=1),
        pq.stochatic_dataset.ColumnSpec(name="gaussian", distribution=pq.stochatic_dataset.Distribution.GAUSSIAN, loc=0, scale=1),
        pq.stochatic_dataset.ColumnSpec(name="halton", distribution=pq.stochatic_dataset.Distribution.HALTON, base=2, low=-1, high=1),
    ]
    database.create_table("table1", 100, column_specs, True)
    column_specs = [
        pq.stochatic_dataset.ColumnSpec(name="laplace", distribution=pq.stochatic_dataset.Distribution.LAPLACE, loc=0, scale=1),
        pq.stochatic_dataset.ColumnSpec(name="uniform", distribution=pq.stochatic_dataset.Distribution.UNIFORM, low=-1, high=1),
        pq.stochatic_dataset.ColumnSpec(name="gaussian", distribution=pq.stochatic_dataset.Distribution.GAUSSIAN, loc=0, scale=1),
        pq.stochatic_dataset.ColumnSpec(name="halton", distribution=pq.stochatic_dataset.Distribution.HALTON, base=2, low=-1, high=1),
    ]
    database.create_table("table2", 1000, column_specs, False)
    print('done')
    privacy_unit = [
        ("table1", [], "id"),
    ]
    synthetic_data = [
        (["testing", "table1"], ["testing", "table1"]),
    ]
    stochastic_tester = StochasticTester(database, 1 / 100 ** (3/2), privacy_unit, synthetic_data)
    test_sum(stochastic_tester, 1000)
    test_count(stochastic_tester, 1000)
    test_avg(stochastic_tester, 1000)







