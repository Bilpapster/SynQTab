from tabpfn_extensions import TabPFNClassifier, TabPFNRegressor, TabPFNUnsupervisedModel
from configs import TabPFNSettings
from generators.Generator import Generator
import torch
import tabpfn_extensions.tabebm.tabebm as tabebm

class TabEBM(Generator):
    """
    TabPFN synthetic data generator.
    """
    def __init__(self, settings: TabPFNSettings):
        super().__init__()
        self.settings = settings.to_dict()
        self.generator = None

    def generate(self, X_initial, y_initial):
        tabebm.seed_everything(42)
        self.generator = tabebm.TabEBM()  # Using TabEBM for synthetic data generation

        samples_per_class = self.generator.generate(
            X_initial,
            y_initial,
            num_samples=self.settings["n_samples"]
        )

        return samples_per_class