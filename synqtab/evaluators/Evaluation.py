from typing import Any, Dict, Optional, Self

from synqtab.data.Dataset import Dataset
from synqtab.enums import (
    DataPerfectness, DataErrorType,
    EvaluationMethod, GeneratorModel, EvaluationTarget
)
from synqtab.experiments.Experiment import Experiment
from synqtab.evaluators.Evaluator import Evaluator
from synqtab.utils import get_logger


LOG = get_logger(__file__)


class Evaluation():
    
    _delimiter: str = '#'
    _NULL: str = 'NULL'

    def __init__(
        self,
        *evaluation_targets: EvaluationTarget,
        experiment: Experiment,
        evaluation_method: EvaluationMethod,
        params: Optional[Dict[str, Any]] = None,
    ):
        from synqtab.mappings import EVALUATION_METHOD_TO_EVALUATION_CLASS
        
        self.evaluation_targets = evaluation_targets
        self.experiment = experiment
        self.evaluation_method = evaluation_method
        self.evaluator = EVALUATION_METHOD_TO_EVALUATION_CLASS.get(self.evaluation_method)(params=params)
        self.params = params if params is not None else dict()
        
        self._should_compute = (not self._exists_in_postgres())
    
    def _run(self):
        from synqtab.data import PostgresClient, MinioClient
        from synqtab.enums import ProblemType, DataPerfectness, EvaluationInput, EvaluationTarget, MinioBucket, EvaluationOutput
        from synqtab.mappings.mappings import EVALUATION_METHOD_TO_EVALUATION_CLASS
        from synqtab.reproducibility import ReproducibleOperations
        from synqtab.utils import timed_computation
        
        evaluation_full_name = str(self) + '/' + str(self.experiment)
        LOG.info(f"Entering the _run() function of Evaluation {evaluation_full_name}")
        
        real_perfect_df = self.experiment.dataset._fetch_real_perfect_dataframe()
        target_column_name = self.experiment.dataset.target_feature
        target = real_perfect_df[target_column_name]
        problem_type = ProblemType(self.experiment.dataset.problem_type)
        sdmetrics_metadata = self.experiment.dataset.get_sdmetrics_single_table_metadata()
        training_df, validation_df = ReproducibleOperations.train_test_split(
            real_perfect_df, test_size=0.5, stratify=target, problem_type=problem_type)
        
        # use the class with the least frequency as minority class. If it is a regression problem, this
        # EvaluationInput key is not used downstream. So, this implementation targets only classification datasets.
        minority_class = validation_df[target_column_name].value_counts(sort=True, ascending=True).index[0]
        
        evaluation_target_dfs = []
        for evaluation_target in self.evaluation_targets:
            data = None
            match evaluation_target:
                case EvaluationTarget.R:
                    LOG.info("Getting perfect R data from dataset + train test split")
                    data = training_df

                case EvaluationTarget.RH:
                    if self.experiment.data_perfectness == DataPerfectness.PERFECT:
                        raise ValueError(f"Cannot create real corrupted data from a perfect experiment object.")
                    
                    LOG.info("Getting imperfect data as perfect + corruption")
                    data_error_instance = self.experiment.data_error.get_class()(row_fraction=self.experiment.data_error_rate)
                    data, corrupted_rows, corrupted_cols = data_error_instance.corrupt(
                        data=training_df,
                        categorical_columns=self.experiment.dataset.categorcal_features,
                        target_column=self.experiment.dataset.target_feature,
                    )
                    if self.experiment.data_perfectness == DataPerfectness.SEMIPERFECT:
                        data.drop(corrupted_rows)

                case EvaluationTarget.S:
                    perfect_counterpart_experiment = self.experiment.perfect_counterpart()
                    LOG.info("Getting S data from Synthetic bucket " + perfect_counterpart_experiment.minio_path())
                    data = MinioClient.read_parquet_from_bucket(
                        bucket_name=MinioBucket.SYNTHETIC,
                        object_name=perfect_counterpart_experiment.minio_path(),
                    )

                case EvaluationTarget.SH:
                    data = MinioClient.read_parquet_from_bucket(
                        bucket_name=MinioBucket.SYNTHETIC,
                        object_name=self.experiment.minio_path(),
                    )
                    
                case _ as not_implemented_evaluation_target:
                    raise NotImplementedError(
                        f"Unknown evaluation target type. Got {not_implemented_evaluation_target}. " +
                        f"Valid options: {[str(option) for option in EvaluationTarget]}."
                    )
            
            for column in data.columns:
                if column in self.experiment.dataset.categorcal_features:
                    data[column] = data[column].astype('category')
                    
            for column in validation_df:
                if column in self.experiment.dataset.categorcal_features:
                    validation_df[column] = validation_df[column].astype('category')
            
            evaluation_target_dfs.append(data)
        
                  
        params = {
            str(EvaluationInput.PROBLEM_TYPE): str(problem_type),
            str(EvaluationInput.METADATA): sdmetrics_metadata,
            str(EvaluationInput.REAL_VALIDATION_DATA): validation_df,
            str(EvaluationInput.NOTES): True,
            str(EvaluationInput.PREDICTION_COLUMN_NAME): target_column_name,
            str(EvaluationInput.KNOWN_COLUMN_NAMES): self.params.get(str(EvaluationInput.KNOWN_COLUMN_NAMES), list(training_df.columns)),
            str(EvaluationInput.SENSITIVE_COLUMN_NAMES): self.params.get(str(EvaluationInput.SENSITIVE_COLUMN_NAMES), []),
            str(EvaluationInput.REAL_TRAINING_DATA): evaluation_target_dfs[0], # used by dual evaluators
            str(EvaluationInput.DATA): evaluation_target_dfs[0],               # used by singular evaluators
            str(EvaluationInput.SYNTHETIC_DATA): evaluation_target_dfs[1] if len(evaluation_target_dfs) > 1 else None,
            str(EvaluationInput.MINORITY_CLASS_LABEL): minority_class,
        }
        
        evaluator_instance = EVALUATION_METHOD_TO_EVALUATION_CLASS.get(self.evaluation_method)(params)
        
        evaluation_output, elapsed_time = timed_computation(
            computation=evaluator_instance.evaluate,
            params=dict(),
        )
        
        import json
        PostgresClient.write_evaluation_result(
            evaluation_id=str(self),
            experiment_id=str(self.experiment),
            first_target=str(self.evaluation_targets[0]),
            second_target=str(self.evaluation_targets[1]) if len(self.evaluation_targets) > 1 else None,
            result=evaluation_output.get(EvaluationOutput.RESULT),
            execution_time=elapsed_time,
            notes=json.dumps(evaluation_output.get(EvaluationOutput.NOTES))
        )

        
    def _is_valid(self) -> bool: 
        return self.evaluator.is_compatible_with(self.experiment.dataset)
        
    def publish_task_if_valid(self) -> bool:
        if not self._is_valid():
            return False
        
        from synqtab.data import MinioClient
        from synqtab.enums import MinioBucket, MinioFolder
        
        task_dict = {
            'evaluation_id': str(self),
            'experiment_id': str(self.experiment),
            'params': self.params,
        }
        bucket_name = MinioBucket.TASKS
        file_name = MinioFolder.create_prefix(
            str(self.experiment), str(self)
        )
        MinioClient.upload_json_to_bucket(
            data=task_dict,
            bucket_name=bucket_name,
            file_name=file_name,
            folder=None # we handle folders in the file name for consistency
        )
        return True
    
    # IMPORTANT: Keep this method aligned with the from_str_and_experiment() method!
    def _get_evaluation_id_parts(self):
        from synqtab.reproducibility.ReproducibleOperations import ReproducibleOperations
        return [
            str(self.evaluation_method), # Evaluator short name, e.g., 'IFO' for Isolation Forest Evaluator
            str(self.evaluation_targets[0]), # Type of the first evaluation target, e.g, 'R' for real data, or 'SH' for imperfect synthetic
            str(self.evaluation_targets[1]) if len(self.evaluation_targets) > 1 else self._NULL, # Type of the second evaluation target if it exists, else standardized NULL placeholder
        ]
    
    # IMPORTANT: Keep this method aligned with the _get_evaluation_id_parts() method!
    @classmethod
    def from_str_and_experiment(cls, evaluation_id: str, experiment: Experiment) -> Self:
                
        evaluation_id_parts = evaluation_id.split(cls._delimiter)
        evaluator_shortname = evaluation_id_parts[0]
        
        evaluation_targets = [EvaluationTarget(str(evaluation_id_parts[1]))]
        if evaluation_id_parts[2] != cls._NULL:
            evaluation_targets.append(EvaluationTarget(str(evaluation_id_parts[2])))
            
        return Evaluation(
            *evaluation_targets,
            evaluation_method=EvaluationMethod(evaluator_shortname),
            experiment=experiment,
        )
    
    def __str__(self):
        experiment_id_parts = self._get_evaluation_id_parts()
        return self._delimiter.join(experiment_id_parts)
    
    def run(self, force: bool=False) -> Self:
        # Skip evaluation if it already exists in the database
        if not self._should_compute and not force:
            from synqtab.data import PostgresClient
            LOG.info(f"Evaluating {str(self)}/{str(self.experiment)} will be skipped because it already exists in Postgres.")
            PostgresClient.write_skipped_computation(
                computation_id=str(self) + '/' + str(self.experiment),
                reason=f"Already exists in Postgres.")
            return self
        
        # Skip evaluation if it is the perfect baseline of a non-perfect experiment
        # The perfect baseline is only computed once, for the first data error rate
        # For example, the R-S evaluations of OUT10, OUT20, and OUT40 on the same dataset are the same; we compute them only once
        from synqtab.environment.experiment import ERROR_RATES
        first_data_error_rate = ERROR_RATES[0]
        if self.experiment.data_error is not None: # if it is not a "perfect" experiment
            is_baseline_evaluation: bool = True
            for evaluation_target in self.evaluation_targets:
                if evaluation_target not in {EvaluationTarget.R, EvaluationTarget.S}:
                    is_baseline_evaluation = False
                    break
            
            # if it is a baseline "perfect" evaluation of a non-perfect experiment, compute only for one error rate
            if is_baseline_evaluation and self.experiment.data_error_rate != first_data_error_rate: 
                from synqtab.data import PostgresClient
                LOG.info(f"Evaluating {str(self)}/{str(self.experiment)} will be skipped to avoid re-computing the baseline.")
                PostgresClient.write_skipped_computation(
                    computation_id=str(self) + '/' + str(self.experiment),
                    reason=f"The perfect baseline is only computed for error rate: {first_data_error_rate}.")
                return self
        
        self._run()
        return self

    def _exists_in_postgres(self) -> bool:
        # TODO FIND A WAY TO HANDLE GRACEFULLY THE PERFECT DATA SCENARIO - NO NEED TO RECOMPUTE IN ALL CASES, ONLY ONCE
        from synqtab.data import PostgresClient
        
        return PostgresClient.evaluation_exists(str(self), str(self.experiment))
