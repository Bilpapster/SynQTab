from synqtab.experiments.Experiment import Experiment
from synqtab.enums.experiments import ExperimentType

class RebalancingExperiment(Experiment):

    @classmethod
    def short_name(cls):
        return ExperimentType.REBALANCING.value