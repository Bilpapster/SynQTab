import pandas as pd

from pollution.Polluter import Polluter
from datasets.Dataset import Dataset
from evaluators.StatisticalEvaluator import StatisticalEvaluator
from generators.TabPFN import TabPFN
from generators.Generator import Task
from pprint import pprint

tabpfg_settings = {
    'n_sgld_steps': 10,
    'n_samples': 1000,
    'balance_classes': False
}

data_config = Dataset('Amazon_employee_access', max_rows=1000)
data = data_config._fetch_original_from_local()

# Choose a pollution type
polluter = Polluter.CSCAR  # or SCAR, CSCAR, NONE

# Apply pollution
corrupted_data, corrupted_rows, corrupted_columns = polluter.corrupt(
    data=data,
    random_seed=42,
    row_percent=20,      # corrupt 20% of rows
    column_percent=50    # corrupt 50% of columns
)

print(corrupted_data)