import typing as t
import pyqrlew as qrl
import qrlew_datasets.database as db  # type: ignore   #module is installed, but missing library stubs or py.typed marker 
from pyqrlew.typing import PrivacyUnit, SyntheticData


class StochasticTester:
    def __init__(
            self,
            database: db.Database,
            delta: float,
            privacy_unit: PrivacyUnit,
            synthetic_data: SyntheticData
        ) -> None:
        self.database = database
        self.delta = delta
        self.privacy_unit = privacy_unit
        self.synthetic_data = synthetic_data

    def compute_dp_relation(self, query: str, epsilon: float) -> qrl.Relation:
        budget = {"epsilon": epsilon, "delta": self.delta}
        relation = self.database.dataset().relation(query)

        relation_with_privatequeries = relation.rewrite_with_differential_privacy(
            dataset=self.database.dataset(),
            privacy_unit=self.privacy_unit,
            epsilon_delta=budget,
            synthetic_data=self.synthetic_data,
        )
        return t.cast(qrl.Relation, relation_with_privatequeries.relation())

    def utility_per_epsilon(
            self,
            query: str,
            epsilons: t.List[float],
            n_sim: int
        ) -> t.Dict[float, list]:

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

    def compute_results(
            self,
            query: str,
            epsilon: float,
            n_sim: int
        ) -> list:
        dp_relation = self.compute_dp_relation(query, epsilon)
        return [
            self.database.eval(dp_relation)[0][0]
            for _ in range(n_sim)
        ]