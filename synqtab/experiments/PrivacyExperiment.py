from synqtab.experiments.Experiment import Experiment
from synqtab.enums.experiments import ExperimentType

class PrivacyExperiment(Experiment):
    pass

    @classmethod
    def short_name(cls):
        return ExperimentType.PRIVACY.value