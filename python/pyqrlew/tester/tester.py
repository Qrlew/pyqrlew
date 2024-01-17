from typing import List

class StochasticTester:
    def __init__(self, database, delta, protected_entity, synthetic_data):
        self.database = database
        self.delta = delta
        self.protected_entity = protected_entity
        self.synthetic_data = synthetic_data

    def compute_dp_relation(self, query, epsilon):
        budget = {"epsilon": epsilon, "delta": self.delta}
        relation = self.database.dataset().relation(query)

        relation_with_privatequeries = relation.rewrite_with_differential_privacy(
            self.database.dataset(),
            self.protected_entity,
            budget,
            self.synthetic_data,
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
                self.database.eval(dp_relation)[0][0]
                for _ in range(n_sim)
            ]
            for epsilon, dp_relation in relations.items()
        }

    def compute_results(self, query: str, epsilon: float, n_sim: int):
        dp_relation = self.compute_dp_relation(query, epsilon)
        return [
            self.database.eval(dp_relation)[0][0]
            for _ in range(n_sim)
        ]