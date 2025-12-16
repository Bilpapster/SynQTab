from abc import ABC, abstractmethod
import pandas as pd


class Evaluator(ABC):
    @abstractmethod
    def evaluate(self, data_1:pd.DataFrame, data_2:pd.DataFrame):
        pass