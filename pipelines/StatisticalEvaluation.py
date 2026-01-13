import os
from evaluators.QualityEvaluator import StatisticalEvaluator
from pipelines.Pipeline import Pipeline
from datasets.Dataset import Dataset


class StatisticalEvaluation(Pipeline):
    def __init__(self):
        super().__init__()

    def run(self):
        list_path = os.path.join(os.path.dirname(__file__), "..", "tabarena_list.txt")

        if not os.path.exists(list_path):
            print(f"File not found: {list_path}")
            return

        with open(list_path, "r") as f:
            dataset_list = [line.strip() for line in f if line.strip()]

        for dataset_name in dataset_list:
            self._evaluate_single_dataset(dataset_name)

    def _evaluate_single_dataset(self, dataset_name):
        print(f"Evaluating dataset: {dataset_name}")
        try:
            dataset_config = Dataset(dataset_name)

            prior = dataset_config.fetch_prior_dataset(max_rows=1000)
            # clean_synthetic = dataset_config.fetch_clean_synthetic_dataset()
            metadata = dataset_config.create_sdmetrics_metadata()

            evaluator = StatisticalEvaluator()
            # Assuming evaluate takes (real_data, synthetic_data, metadata)
            # Currently passing prior as both real and synthetic for testing/baseline
            evaluator.evaluate(prior, prior, metadata)

            evaluator.write_pickle(dataset_name)
            evaluator.write_json(dataset_name)
            print(f"Completed evaluation for dataset: {dataset_name}\n")
        except Exception as e:
            print(f"Failed to evaluate dataset {dataset_name}: {e}\n")


if __name__ == "__main__":
    evaluation_pipeline = StatisticalEvaluation()
    evaluation_pipeline.run()
