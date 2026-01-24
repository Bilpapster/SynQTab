from synqtab.experiments.Experiment import Experiment
from synqtab.enums.experiments import ExperimentType

class NormalExperiment(Experiment):
    
    @classmethod
    def short_name(cls):
        return ExperimentType.NORMAL.value
