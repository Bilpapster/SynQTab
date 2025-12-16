import pandas as pd
from sdmetrics.single_table import LogisticDetection
from evaluators.SDMetricsEvaluator import SDMetricsEvaluator


class LogisticEvaluation(SDMetricsEvaluator):
    """
    Evaluator that uses the LogisticDetection metric from SDMetrics to calculate how difficult it is to tell apart the
    real data from the synthetic data.

    Use Logistic Regression and train the model on the union of real and synthetic data.
    The model will predict whether each row is real or synthetic.

    The final score is based on the average ROC AUC score across all the cross validation splits.
    """
    def evaluate(self, data_1:pd.DataFrame, data_2:pd.DataFrame, metadata: dict) -> dict:
        metric = LogisticDetection()
        score = metric.compute(data_1, data_2, metadata)
        return {'logistic_detection': score}