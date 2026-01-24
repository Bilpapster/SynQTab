from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Self

from synqtab.data.Dataset import Dataset
from synqtab.enums.data import DataPerfectness, DataErrorType
from synqtab.enums.evaluators import EvaluationMethod
from synqtab.enums.generators import GeneratorModel
from synqtab.errors.DataError import DataError
from synqtab.utils.logging_utils import get_logger


LOG = get_logger(__file__)


class Experiment(ABC):
    
    _delimiter: str = '#'
    _NULL: str = 'NULL'
    translator: str

    def __init__(
        self,
        dataset: Dataset,
        generator: GeneratorModel,
        data_error: Optional[DataError] = None,
        data_error_rate: Optional[float] = None,
        data_perfectness: DataPerfectness = DataPerfectness.PERFECT,
        evaluators: Optional[list[EvaluationMethod]] = None,
        options: Optional[Dict[Any, Any]] = None,
    ):
        self.dataset = dataset
        self.generator = generator
        self.data_error = data_error
        self.data_error_rate = data_error_rate
        self.data_perfectness = data_perfectness
        self.evaluators = evaluators
        self.options = options
        
        self._should_compute = self._already_exists()
        
    @classmethod
    @abstractmethod
    def short_name(cls):
        pass
    
    # IMPORTANT: Keep this method aligned with the from_str() method!
    def _get_experiment_id_parts(self):
        from synqtab.reproducibility.ReproducibleOperations import ReproducibleOperations
        return [
            self.short_name(),  # Experiment shortname, e.g., 'NOR' for Normal Experiment
            self.dataset.dataset_name,  # Dataset name, e.g., 'anneal'
            str(ReproducibleOperations.get_current_random_seed()),  # Random seed
            self.data_perfectness.short_name(), # Data perfectness level, e.g., 'PERF' for perfect
            self.data_error.value if self.data_error else self._NULL,    # Data error type, e.g., 'OUT' for outliers
            str(self.data_error_rate * 100) if self.data_error_rate else self._NULL, # Data error rate multiplied by 100, e.g., 0.2 -> 20
            self.generator.value,   # Generator type, e.g., 'tabpfn' 
        ]
    
    # IMPORTANT: Keep this method aligned with the _get_experiment_parts() method!
    @classmethod
    def from_str(cls, experiment_id: str) -> tuple[Self, int]:
        from synqtab.mappings.mappings import EXPERIMENT_TYPE_TO_EXPERIMENT_CLASS
        from synqtab.data.Dataset import Dataset
        from synqtab.enums.data import DataPerfectness
        from synqtab.enums.generators import GeneratorModel
        
        experiment_id_parts = experiment_id.split(cls._delimiter)
        experiment_short_name = experiment_id_parts[0]
        dataset = Dataset(experiment_id_parts[1])
        random_seed = int(experiment_id_parts[2])
        data_perfectness = DataPerfectness(experiment_id_parts[3])
        data_error = None if experiment_id_parts[4] == cls._NULL else DataErrorType(experiment_id_parts[4])
        data_error_rate = None if experiment_id_parts[5] == cls._NULL else float(experiment_id_parts[5] / 100)
        generator = GeneratorModel(experiment_id_parts[6])
        
        for experiment_type, experiment_class in EXPERIMENT_TYPE_TO_EXPERIMENT_CLASS.items():
            if experiment_short_name == experiment_class.short_name():
                LOG.info(f"Experiment {experiment_id} found to be of class {experiment_class.__name__}")
                return experiment_class(
                    dataset=dataset,
                    generator=generator,
                    data_error=data_error,
                    data_error_rate=data_error_rate,
                    data_perfectness=data_perfectness,
                ), random_seed # RETURNS TUPLE: Experiment class + random seed!
    
    def __str__(self):
        from synqtab.reproducibility.ReproducibleOperations import ReproducibleOperations
        
        experiment_id_parts = self._get_experiment_id_parts()
        return self._delimiter.join(experiment_id_parts)
    
    def to_minio_path(self):
        from synqtab.enums.minio import MinioFolder, MinioBucket
        from synqtab.reproducibility.ReproducibleOperations import ReproducibleOperations
        
        bucket = MinioBucket.SYNTHETIC
        prefix = MinioFolder.create_prefix(
            MinioFolder.DATA,
            f"experiment={self.short_name}",
            f"dataset={self.dataset.dataset_name}",
            f"random_seed={ReproducibleOperations.get_current_random_seed}",
            f"perfectness={self.data_perfectness}",
            f""
            f"generator={}"
        )
        
        return MinioFolder.create_prefix(
            
        )
        
    def run(self) -> Self:
        return self
    
    def persist(self) -> Self:
        return self
    
    def populate_tasks(self) -> Self:
        return self

    def _already_exists(self) -> bool:
        experiment_id = self.__str__()
    