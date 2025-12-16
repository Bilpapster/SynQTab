import os
from evaluators.StatisticalEvaluator import StatisticalEvaluator
from pipelines.Pipeline import Pipeline
from datasets.Dataset import Dataset

class StatisticalEvaluation():
    def __init__(self, ):
        super().__init__()

    def run(self):
        list_path = "../tabarena_list.txt"

        if not os.path.exists(list_path):
            print(f"File not found: {list_path}")
            return

        with open(list_path, "r") as f:
            dataset_list = [line.strip() for line in f if line.strip()]

        for dataset_name in dataset_list:
            print(f"Evaluating dataset: {dataset_name}")
            dataset_config = Dataset(dataset_name)

            prior = dataset_config.fetch_prior_dataset(max_rows=1000)
            # clean_synthetic = dataset_config.fetch_clean_synthetic_dataset()
            metadata = dataset_config.create_sdmetrics_metadata()
            evaluator = StatisticalEvaluator()
            evaluator.evaluate(prior, prior, metadata)
            evaluator.write_pickle()
            evaluator.write_json()
            print(f"Completed evaluation for dataset: {dataset_name}\n")

eval = StatisticalEvaluation()
eval.run()