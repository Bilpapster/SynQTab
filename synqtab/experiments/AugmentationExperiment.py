from synqtab.experiments.Experiment import Experiment
from synqtab.enums.experiments import ExperimentType

class AugmentationExperiment(Experiment):
    
    @classmethod
    def short_name(cls):
        return ExperimentType.AUGMENTATION.value