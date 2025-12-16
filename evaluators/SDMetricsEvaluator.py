from abc import abstractmethod
import pandas as pd
from evaluators.Evaluator import Evaluator


class SDMetricsEvaluator(Evaluator):
    @abstractmethod
    def evaluate(self, data_1: pd.DataFrame, data_2: pd.DataFrame, metadata: dict) -> dict:
        pass