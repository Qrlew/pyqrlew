import numpy as np
from typing import List, Callable, Union
import typing as t
import textwrap
from .stochatic_dataset import StochasticDatabase
from .tester import StochasticTester
import pandas as pd # type: ignore

# Optionally import matplotlib
try:
    import matplotlib.pylab as plt  # type: ignore  # module is installed, but missing library stubs or py.typed marker 
    plt.rcParams['text.usetex'] = True
except ImportError:
    plt = None

def divergence(array1: np.ndarray, array2: np.ndarray, epsilon: float) -> float:
    arr = [max(l1 - np.exp(epsilon)*l2, 0) for (l1, l2) in zip(array1, array2)]
    return float(np.sum(arr))

def privacy_profile(ref_results: np.ndarray, adj_results: List[np.ndarray], epsilon: float) -> float:
    divergences = [
        divergence(ref_results, adj_res, epsilon)
        for adj_res in adj_results
    ]
    return float(max(divergences))

def compute_distribution(my_list: list, bins: Union[int, List[int]]) -> np.ndarray:
    values, _ = np.histogram(my_list, bins=bins)
    values = values / np.sum(values)
    return values

def plot_utility(dp_results: dict, true_res: float) -> None:
    if plt is None:
        raise ModuleNotFoundError
    [
        plt.hist(results, bins=20, alpha=0.5, label = fr"$\varepsilon = {epsilon}$")
        for (epsilon, results) in dp_results.items()
    ]
    plt.axvline(true_res, color = 'red')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()

def plot_adjacent_histograms(array1: List[float], array2: List[float], epsilon:float, delta: float, nbins:float=20, alpha:float=0.5, color1:str='blue', color2:str='orange') -> None:
    if plt is None:
        raise ModuleNotFoundError
    xmin = max(min(array1), min(array2))
    xmax = min(max(array1), max(array2))
    bins = np.linspace(xmin, xmax, int(nbins))
    values1 = compute_distribution(array1, t.cast(List[int], bins))
    values2 = compute_distribution(array2, t.cast(List[int], bins))
    x = bins[1:] + (bins[1] -bins[0])

    plt.bar(x, np.exp(epsilon) * values2 + delta, width=bins[1]-bins[0], alpha=alpha, color=color2, label=r'$e^{\varepsilon}P(f(\mathcal{D}_2)) + \delta$')
    plt.bar(x, values1, alpha=alpha, color=color1, width=bins[1]-bins[0], label=r'$P(f(\mathcal{D}_1))$')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()

def plot_privacy_profile(array: List[float], arrays: List[List[float]], nbins:int=20, epsilons: List[float]=list(np.linspace(0, 3, 100))) -> None:
    if plt is None:
        raise ModuleNotFoundError
    xmin = max(min([min(a) for a in arrays]), min(array))
    xmax = min(max([max(a) for a in arrays]), max(array))
    bins = int(np.linspace(xmin, xmax, nbins))

    ref_distribution = compute_distribution(array, bins)
    adj_distributions = [
        compute_distribution(res, bins)
        for res in arrays
    ]
    pprofiles = [
        privacy_profile(ref_distribution, adj_distributions, epsilon) for epsilon in epsilons
    ]
    plt.plot(epsilons, pprofiles, '-', color='navy')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.xlabel(r'$\varepsilon$')
    plt.ylabel(r'$\delta$')

def save_data(filename: str, arr_ref: List[float], arr_adj: List[List[float]]) -> None:
    df = pd.DataFrame()
    df['ref'] = arr_ref
    for i, adj in enumerate(arr_adj):
        df[f'adj_{i}'] = adj
    df.to_csv(filename, index=False)
    print(f"file {filename} written.")

def run_test(database: StochasticDatabase, stochastic_tester: StochasticTester, query: str, adj_query_func: Callable[[int], str], name: str, n_sim: int)-> None:
    if plt is None:
        raise ModuleNotFoundError
    print(f"Query = {query}")
    dp_results = stochastic_tester.utility_per_epsilon(query, epsilons=[1., 5., 10.], n_sim=n_sim)
    true_res = database.execute(query)[0][0]
    plt.figure(figsize=(6, 15))
    plt.subplot(3, 1, 1)
    plot_utility(dp_results=dp_results, true_res=true_res)

    epsilon = 1.
    ref_res = stochastic_tester.compute_results(query, epsilon=epsilon, n_sim=n_sim)
    adj_res = [
        stochastic_tester.compute_results(
            adj_query_func(int(i)),
            epsilon=epsilon,
            n_sim=n_sim
        )
        for i in [np.random.uniform(0, 100) for _ in range(10)]
    ]
    save_data(f"{name}.txt", ref_res, adj_res)

    nbins = int(n_sim / 200)
    plt.subplot(3, 1, 2)
    plot_adjacent_histograms(ref_res, adj_res[0], epsilon=epsilon, delta=stochastic_tester.delta, nbins=nbins)

    plt.subplot(3, 1, 3)
    plot_privacy_profile(ref_res, adj_res, nbins=nbins)
    plt.axhline(stochastic_tester.delta, color='r')

    wrapped_query = textwrap.fill(query, width=30)  # Adjust the width as needed
    plt.suptitle(wrapped_query, fontsize=12)
    plt.tight_layout()
    plt.savefig(f"{name}.png")
    print("================================")
