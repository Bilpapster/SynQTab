import warnings
warnings.filterwarnings("ignore") # mitigates synthcity's annoying verbosity

import copy
from pprint import pp
import random

from synqtab.data.clients.MinioClient import MinioClient
from synqtab.data.Dataset import Dataset
from synqtab.enums.data import DataErrorType, DataPerfectness
from synqtab.enums.evaluators import QUALITY_EVALUATORS, ML_FOCUSED_EVALUATORS
from synqtab.enums.generators import GENERIC_MODELS
from synqtab.enums.minio import MinioBucket, MinioFolder
from synqtab.experiments.Experiment import Experiment
from synqtab.experiments.NormalExperiment import NormalExperiment
from synqtab.environment.experiment import RANDOM_SEEDS, ERROR_RATES
from synqtab.reproducibility.ReproducibleOperations import ReproducibleOperations
from synqtab.utils.logging_utils import get_logger


LOG = get_logger(__file__)


random_seeds = copy.deepcopy(RANDOM_SEEDS); random.shuffle(random_seeds)
pp(f"{random_seeds=}")

dataset_names = MinioClient.list_files_in_bucket_by_file_extension(
    bucket_name=MinioBucket.REAL.value,
    file_extension='parquet',
    prefix=MinioFolder.create_prefix(MinioFolder.PERFECT, MinioFolder.DATA),
); random.shuffle(dataset_names)
pp(f"{dataset_names=}", compact=True); print()

models = copy.deepcopy(GENERIC_MODELS); random.shuffle(models)
pp(f"{models=}", compact=True); print()

errors = [error for error in DataErrorType]; random.shuffle(errors)
pp(f"{errors=}", compact=True); print()

error_rates = copy.deepcopy(ERROR_RATES); random.shuffle(error_rates)
pp(f"{error_rates=}")

data_perfectness_levels = [DataPerfectness.IMPERFECT, DataPerfectness.SEMIPERFECT]
random.shuffle(data_perfectness_levels)
pp(f"{data_perfectness_levels=}", compact=True); print()

evaluators = copy.deepcopy(QUALITY_EVALUATORS + ML_FOCUSED_EVALUATORS); random.shuffle(evaluators)
pp(f"{evaluators=}", compact=True); print()


from synqtab.errors.LabelError import LabelError
from synqtab.errors.GaussianNoise import GaussianNoise

import pandas as pd

ReproducibleOperations.set_random_seed(2)
data = {
  "calories": range(1, 101),
  "duration": range(201, 301),
  "target": ReproducibleOperations.sample_from([0, 1, 2, 3, 4], how_many=100, sampling_with_replacement=True)
}

print(len(data.get('calories')))
print(len(data.get('duration')))
print(len(data.get('target')))

# #load data into a DataFrame object:
# df = pd.DataFrame(data)

# label_error = LabelError(0.9).corrupt(df, target_feature="target")
# print(label_error[1], label_error[2])
# print(label_error[0].head(10))

# noisy_df, corrupted_rows, corrupted_cols = GaussianNoise(0.4).corrupt(df)
# print(corrupted_rows, corrupted_cols)
# print(noisy_df.head(10))

# exit(0)


# First, generate all perfect synthetic data (S)
for random_seed in random_seeds:
    ReproducibleOperations.set_random_seed(random_seed)
    for dataset_name in dataset_names:
        dataset = Dataset(dataset_name)
        for model in models:
            try:
                normal_experiment = NormalExperiment(
                    dataset=dataset,
                    generator=model,
                    data_error=None,
                    data_error_rate=None,
                    data_perfectness=DataPerfectness.PERFECT, # only perfect data at first
                    evaluators=None,
                )
                normal_experiment.run().persist()
                print(normal_experiment)
                experiment, seed = Experiment.from_str(str(normal_experiment))
                print(experiment, seed)
                exit(0)
            except Exception as e:
                LOG.error(
                    f"The experiment failed but I will continue to the next one. Error: {e}",
                    extra={'experiment_id': str(normal_experiment)}
                )
                continue


# Then, generate all imperfect (S_hat) and semi-perfect (S_semi) and populate evaluation tasks
for random_seed in random_seeds:
    ReproducibleOperations.set_random_seed(random_seed)
    for dataset_name in dataset_names:
        dataset = Dataset(dataset_name)
        for model in models:
            for error in errors:
                for error_rate in error_rates:
                    for perfectness_level in data_perfectness_levels:
                        try:
                            normal_experiment = NormalExperiment(
                                dataset=dataset,
                                generator=model,
                                data_error=error,
                                data_error_rate=error_rate,
                                data_perfectness=perfectness_level,
                                evaluators=evaluators,
                            )
                            normal_experiment.run().persist().populate_tasks()
                            print(normal_experiment)
                            
                        except Exception as e:
                            LOG.error(
                                f"The experiment failed but I will continue to the next one. Error: {e}",
                                extra={'experiment_id': str(normal_experiment)}
                            )
                            continue
