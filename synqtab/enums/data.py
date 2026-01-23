from enum import Enum

from synqtab.errors.DataError import DataError

class Metadata(Enum):
    NAME = 'name'
    PROBLEM_TYPE = 'problem_type'
    TARGET_FEATURE = 'target_feature'
    CATEGORICAL_FEATURES = 'categorical_features'


class ProblemType(Enum):
    CLASSIFICATION = 'classification'
    REGRESSION = 'regression'


class DataPerfectness(Enum):
    PERFECT = 'perfect'
    IMPERFECT = 'imperfect'
    SEMIPERFECT = 'semiperfect'
    
    def short_name(self) -> str:
        """First 3 letters in capital for imperfect; first 4 letters in capital for
        anything else.

        Returns:
            str: The shortname of the DataPerfectness object.
        """
        if self == DataPerfectness.IMPERFECT:
            return self.value.upper()[:3]
        return self.value.upper()[:4]
    

class DataErrorType(Enum):
    CATEGORICAL_SHIFT = 'SFT'
    GAUSSIAN_NOISE = 'NOI'
    PLACEHOLDER = 'PLC'
    NEAR_DUPLICATE = 'DUP'
    OUTLIER = 'OUT'
    INCONSISTENCY = 'INC'
    LABEL_ERROR = 'LER'
    
    def get_class(self) -> DataError:
        
        match(self):
            case DataErrorType.CATEGORICAL_SHIFT:
                from synqtab.errors.CategoricalShift import CategoricalShift
                return CategoricalShift
            case DataErrorType.GAUSSIAN_NOISE:
                from synqtab.errors.GaussianNoise import GaussianNoise
                return GaussianNoise
            case DataErrorType.INCONSISTENCY:
                from synqtab.errors.Inconsistency import Inconsistency
                return Inconsistency
            case DataErrorType.NEAR_DUPLICATE:
                from synqtab.errors.NearDuplicateRow import NearDuplicateRow
                return NearDuplicateRow
            case DataErrorType.OUTLIER:
                from synqtab.errors.Outlier import Outliers
                return Outliers
            case DataErrorType.PLACEHOLDER:
                from synqtab.errors.Placeholder import Placeholder
                return Placeholder
                
                
                
                
        
        from synqtab.errors.Placeholder import Placeholder
        
        DATA_ERROR_TYPE_TO_DATA_ERROR_CLASS = {
            DataErrorType.GAUSSIAN_NOISE: GaussianNoise,
            DataErrorType.INCONSISTENCY: Inconsistency,
            DataErrorType.LABEL_ERROR: None, # TODO IMPLEMENT LABEL ERROR
            DataErrorType.NEAR_DUPLICATE: NearDuplicateRow,
            DataErrorType.OUTLIER: Outliers,
            DataErrorType.PLACEHOLDER: Placeholder,
        }
