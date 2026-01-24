from enum import Enum


class ExperimentType(Enum):
    NORMAL = 'NOR'
    PRIVACY = 'PRI'
    AUGMENTATION = 'AUG'
    REBALANCING = 'REB'
