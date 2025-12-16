import json
from sdmetrics.reports.single_table import QualityReport
import pandas as pd
from evaluators.SDMetricsEvaluator import SDMetricsEvaluator


class StatisticalEvaluator(SDMetricsEvaluator):
    """
    Evaluator that uses SDMetrics QualityReport to assess the statistical
    quality of data_2 compared to data_1. data_1 can be the real data or the synthetic created from clean data
    and data_2 is the synthetic data or the data created from the polluted data.
    """

    def __init__(self, experiment_name: str = "Statistical Evaluation"):
        self.sd_metrics_report = None
        self.json_report = None
        self.experiment_name = experiment_name

    def evaluate(self, data_1: pd.DataFrame, data_2: pd.DataFrame, metadata:dict) -> dict:
        """
        Evaluates the synthetic data using the SDMetrics QualityReport.
        This generates an aggregate score and detailed property scores for
        Column Shapes and Column Pair Trends.
        """
        report = {}

        try:
            # Initialize the SDMetrics QualityReport
            quality_report = QualityReport()

            # Generate the report
            quality_report.generate(data_1, data_2, metadata=metadata, verbose=False)
            self.sd_metrics_report = quality_report

            # Get the overall quality score
            report['Overall_Quality_Score'] = quality_report.get_score()

            # Get detailed property scores (Column Shapes, Column Pair Trends)
            properties = quality_report.get_properties()
            for _, row in properties.iterrows():
                prop_name = row['Property']
                clean_prop_name = prop_name.replace(' ', '_')
                report[f'{clean_prop_name}_Score'] = row['Score']

                # Get details for this specific property and convert to dict/list for JSON serialization
                details = quality_report.get_details(property_name=prop_name)
                report[f'{clean_prop_name}_Details'] = details.to_dict(orient='records')

        except Exception as e:
            report['QualityReport_error'] = str(e)

        return quality_report

    def write_pickle(self):
        """
        Writes the quality report to a pickle file for later visualization.
        """
        import os
        directory = "../experiments/sd_reports/"
        os.makedirs(directory, exist_ok=True)
        self.sd_metrics_report.save(filepath=f"{directory}{self.experiment_name}_statistical_eval.pkl")

    def write_json(self):
        """
        Writes the evaluation report to a JSON file.
        """
        import os
        directory = "../experiments/sd_jsons/"
        os.makedirs(directory, exist_ok=True)
        with open(f"{directory}{self.experiment_name}_statistical_eval.json", 'w') as f:
            json.dump(self.json_report, f, indent=4)