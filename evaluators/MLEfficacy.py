from evaluators.SDMetricsEvaluator import SDMetricsEvaluator
import pandas as pd


class MLEfficacy(SDMetricsEvaluator):
    def _find_classification_type(self, data: pd.DataFrame, target_column: str) -> str:
        unique_values = data[target_column].nunique()
        if unique_values == 2:
            return "binary"
        else:
            return "multiclass"

    def evaluate(self, real_data:pd.DataFrame, synthetic_data:pd.DataFrame, metadata: dict, problem_type:str, target_column:str) -> dict:
        if problem_type=="regression":
            from sdmetrics.single_table import LinearRegression

            scores = LinearRegression.compute(
                test_data=real_data,
                train_data=synthetic_data,
                target=target_column,
                metadata=metadata
            )

            return scores
        elif problem_type=="classification":
            type = self._find_classification_type(synthetic_data, target_column)
            if type=="binary":
                from sdmetrics.single_table import BinaryAdaBoostClassifier

                scores = BinaryAdaBoostClassifier.compute(
                    test_data=real_data,
                    train_data=synthetic_data,
                    target=target_column,
                    metadata=metadata
                )

                return scores
            else:
                from sdmetrics.single_table import MulticlassDecisionTreeClassifier

                scores = MulticlassDecisionTreeClassifier.compute(
                    test_data=real_data,
                    train_data=synthetic_data,
                    target=target_column,
                    metadata=metadata
                )

                return scores
        return None