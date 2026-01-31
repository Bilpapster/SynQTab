# from abc import ABC, abstractmethod
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
        self.params = params
        
        self._should_compute = (not self._exists_in_postgres())
        
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
        return self
        # if not self._should_compute and not force:
        #     from synqtab.data import PostgresClient
        #     LOG.info(f"Running experiment {str(self)} will be skipped because it already exists in Postgres.")
        #     PostgresClient.write_skipped_computation(computation_id=str(self), reason="Already exists in Postgres.")
        #     return self
        
        # self._run()
        # return self

    def _exists_in_postgres(self) -> bool:
        # TODO FIND A WAY TO HANDLE GRACEFULLY THE PERFECT DATA SCENARIO - NO NEED TO RECOMPUTE IN ALL CASES, ONLY ONCE
        from synqtab.data import PostgresClient
        
        return PostgresClient.evaluation_exists(str(self), str(self.experiment))
