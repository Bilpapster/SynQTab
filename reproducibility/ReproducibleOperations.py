from typing import List
import numpy as np


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class _RandomSeedOperations:
    def __init__(self, random_seed: int | float):
        self.random_seed = random_seed
        np.random.seed(random_seed)


class ReproducibleOperations(_RandomSeedOperations, metaclass=Singleton):

    def sample_from(
        self,
        elements: List,
        how_many: int | float,
        at_least: int = 1,
        sampling_with_replacement: bool = False,
    ) -> List:
        """Samples `how_many` items from `elements`. Internally uses numpy. Reproducibility is ensured as long as you stick to
        functions of this class throughout the application for all operations that require randomness.

        Args:
            elements (List): the elements to sample from
            how_many (int | float): the desired number of items to sample
            at_least (int, optional): number of items to be sampled at least. Overrides `how_many` in case `how_many` < `at_least`.
            Defaults to 1.
            sampling_with_replacement (bool, optional): Whether replacement should be applied during sampling. Defaults to False.

        Returns:
            List: the sampled items
        """
        return np.random.choice(
            a=elements,
            size=max(int(how_many), at_least),
            replace=sampling_with_replacement,
        )
