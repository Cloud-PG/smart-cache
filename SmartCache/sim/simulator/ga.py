from multiprocessing import Pool
from random import randint, random, seed

import numpy as np
import pandas as pd
from tqdm import tqdm

# Set seed
seed(42)
np.random.seed(42)


def valid_individual(individual, dataframe, cache_size: float) -> bool:
    return indivudual_size(individual, dataframe) <= cache_size


def indivudual_size(individual, dataframe) -> float:
    cur_series = pd.Series(individual)
    cur_size = sum(dataframe[cur_series]['size'])
    return cur_size


def individual_fitness(individual, dataframe) -> float:
    cur_series = pd.Series(individual)
    cur_size = sum(dataframe[cur_series]['value'])
    return cur_size


def make_it_valid(individual, dataframe, cache_size: float):
    individual_size = indivudual_size(individual, dataframe)
    if individual_size > cache_size:
        nonzero = np.nonzero(individual)[0]
        np.random.shuffle(nonzero)
        sizes = dataframe.loc[nonzero]['size']
        to_false = []
        for cur_idx in nonzero.tolist():
            if individual_size <= cache_size:
                break
            to_false.append(cur_idx)
            individual_size -= sizes[cur_idx]
        if to_false:
            individual[to_false] = False
    return individual


def get_one_solution(gen_input):
    dataframe, cache_size = gen_input
    individual = np.random.randint(2, size=dataframe.shape[0], dtype=bool)
    individual = make_it_valid(individual, dataframe, cache_size)
    return individual


def get_best_configuration(dataframe, cache_size: float,
                           num_generations: int = 1000,
                           population_size: int = 42,
                           insert_best_greedy: bool = False):
    population = []
    pool = Pool()
    for _, individual in tqdm(enumerate(
        pool.imap(
            get_one_solution,
            [
                (dataframe, cache_size)
                for _ in range(population_size)
            ]
        )
    ), desc="Create Population",
            total=population_size, ascii=True):
        population.append(individual)

    pool.terminate()
    pool.join()

    if insert_best_greedy:
        print("[Create best individual with greedy method]")
        # Create 1 best individual with greedy method
        best_greedy = np.zeros(dataframe.shape[0], dtype=bool)
        cur_size = 0.
        cur_score = 0.

        for idx, cur_row in enumerate(dataframe.itertuples()):
            file_size = cur_row.size
            if cur_size + file_size <= cache_size:
                cur_size += file_size
                cur_score += cur_row.value
                best_greedy[idx] = True
            else:
                break

        population.append(best_greedy)

    best = evolve_with_genetic_algorithm(
        population, dataframe, cache_size, num_generations
    )

    return best


def cross(element, factor):
    if element >= factor:
        return 1
    return 0


V_CROSS = np.vectorize(cross)


def mutate(element, factor):
    if element <= factor:
        return 1
    return 0


V_MUTATE = np.vectorize(mutate)


def crossover(parent_a, parent_b) -> 'np.Array':
    """Perform and uniform corssover."""
    new_individual = np.zeros(len(parent_a)).astype(bool)
    uniform_crossover = np.random.rand(len(parent_a))
    cross_selection = V_CROSS(uniform_crossover, 0.9).astype(bool)
    new_individual[cross_selection] = parent_a[cross_selection]
    cross_selection = ~cross_selection
    new_individual[cross_selection] = parent_b[cross_selection]
    return new_individual


def mutation(individual) -> 'np.Array':
    """Bit Flip mutation."""
    flip_bits = np.random.rand(len(individual))
    mutant_selection = V_MUTATE(flip_bits, 0.01).astype(bool)
    individual[mutant_selection] = ~individual[mutant_selection]
    return individual


def generation(gen_input):
    best, individual, dataframe, cache_size = gen_input
    child_0 = crossover(best, individual)
    child_1 = ~child_0

    child_0 = mutation(child_0)
    child_0 = make_it_valid(
        child_0, dataframe, cache_size)
    child_0_fitness = individual_fitness(child_0, dataframe)

    child_1 = mutation(child_1)
    child_1 = make_it_valid(
        child_1, dataframe, cache_size)
    child_1_fitness = individual_fitness(child_1, dataframe)

    return (child_0, child_0_fitness, child_1, child_1_fitness)


def roulette_wheel(fitness: list, extractions: int = 1):
    cur_fitness = np.array(fitness)
    fitness_sum = np.sum(cur_fitness)
    probabilities = cur_fitness / fitness_sum
    probabilities = probabilities.tolist()

    for _ in range(extractions):
        candidates = []
        while(True):
            idx = randint(0, len(probabilities) - 1)
            cur_probability = probabilities[idx]
            if random() <= cur_probability:
                candidates.append(idx)
            if len(candidates) == 2:
                break
        yield candidates


def evolve_with_genetic_algorithm(population, dataframe,
                                  cache_size: float,
                                  num_generations: int
                                  ):
    cur_population = [elm for elm in population]

    cur_fitness = []
    for indivudual in cur_population:
        cur_fitness.append(individual_fitness(indivudual, dataframe))

    for _ in tqdm(
        range(num_generations),
        desc="Evolution", ascii=True,
        position=0
    ):
        idx_best = np.argmax(cur_fitness)

        childrens = []
        childrens_fitness = []

        pool = Pool(processes=4, maxtasksperchild=1)

        for child_0, child_0_fitness, child_1, child_1_fitness in tqdm(
            pool.imap(
                generation,
                [
                    (
                        cur_population[candidates[0]],
                        cur_population[candidates[1]],
                        dataframe,
                        cache_size
                    )
                    for candidates
                    in roulette_wheel(cur_fitness, len(population))
                ]
            ),
                desc=f"Make new generation [Best: {cur_fitness[idx_best]:0.0f}][Mean: {np.mean(cur_fitness):0.0f}][Var: {np.var(cur_fitness):0.0f}]",
                ascii=True, position=1, leave=False,
                total=len(cur_population),
        ):
            childrens += [child_0, child_1]
            childrens_fitness += [child_0_fitness, child_1_fitness]

        pool.terminate()
        pool.join()

        new_population = cur_population + childrens
        new_fitness = cur_fitness + childrens_fitness

        for idx, real_idx in enumerate(reversed(np.argsort(new_fitness).tolist())):
            if idx < len(population):
                cur_population[idx] = new_population[real_idx]
                cur_fitness[idx] = new_fitness[real_idx]
            else:
                break

    idx_best = np.argmax(cur_fitness)
    return cur_population[idx_best]


def compare_greedy_solution(dataframe: pd.DataFrame, cache_size: float,
                            greedy_selection: pd.Series):
    ga_size = sum(dataframe[dataframe['class']]['size'].to_list())
    ga_score = sum(dataframe[dataframe['class']]['value'].to_list())

    gr_size = sum(dataframe[greedy_selection]['size'].to_list())
    gr_score = sum(dataframe[greedy_selection]['value'].to_list())

    print("---[Results]---")
    print(f"[Size: \t{gr_size:0.2f}][Score: \t{gr_score:0.2f}][Greedy]")
    print(f"[Size: \t{ga_size:0.2f}][Score: \t{ga_score:0.2f}][GA]")
