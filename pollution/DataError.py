from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

from pollution.DataErrorApplicability import DataErrorApplicability
from reproducibility.ReproducibleOperations import ReproducibleOperations


class DataError(ABC):

    def __init__(
        self,
        random_seed: int | float,
        row_fraction: float,
        column_fraction: float,
        options: Optional[Dict[Any, Any]] = None,
    ):
        self.random_seed = random_seed
        self.row_fraction = row_fraction
        self.column_fraction = column_fraction
        self.options = options
        self.validate_instance_attributes()
        self.initialize_other_attributes()

    @abstractmethod
    def data_error_applicability(self) -> DataErrorApplicability:
        pass

    def validate_instance_attributes(self) -> None:
        self.validate_random_seed()
        self.validate_row_fraction()
        self.validate_column_fraction()
        self.validate_options()

    def initialize_other_attributes(self):
        self.reproducible_operations = ReproducibleOperations(self.random_seed)
        self.categorical_columns = []
        self.numeric_columns = []
        self.rows_to_corrupt = []
        self.columns_to_corrupt = []
        self.corrupted_data = None

    def validate_random_seed(self) -> None:
        pass

    def validate_row_fraction(self) -> None:
        if not 0 <= self.row_fraction <= 1:
            raise ValueError(
                f"Row fraction must be in the range [0, 1]. Got {self.row_fraction}."
            )

    def validate_column_fraction(self) -> None:
        if not 0 <= self.column_fraction <= 1:
            raise ValueError(
                f"Column fraction must be in the range [0, 1]. Got {self.column_fraction}."
            )

    def validate_options(self) -> None:
        pass

    def find_numerical_categorical_columns(self) -> None:
        self.categorical_columns = [
            column
            for column in self.corrupted_data.columns
            if isinstance(self.corrupted_data[column].dtype, pd.CategoricalDtype)
        ]
        self.numeric_columns = [
            column
            for column in self.corrupted_data.columns
            if pd.api.types.is_numeric_dtype(self.corrupted_data[column].dtype)
        ]

    def identify_rows_to_corrupt(self, data: pd.DataFrame) -> None:
        # the Numpy's random seed is set in the class constructor - crucial for repoducibility
        self.rows_to_corrupt = self.reproducible_operations.sample_from(
            elements=data.index.to_list(), how_many=self.row_fraction * data.shape[0]
        )

    def identify_columns_to_corrupt(self) -> None:
        match self.dataErrorApplicability():
            case DataErrorApplicability.CATEGORICAL_ONLY:
                self.columns_to_corrupt = self.reproducible_operations.sample_from(
                    elements=self.categorical_columns,
                    how_many=self.column_fraction * (len(self.numeric_columns + self.categorical_columns)),
                )
                
            case DataErrorApplicability.NUMERIC_ONLY:
                self.columns_to_corrupt = self.reproducible_operations.sample_from(
                    elements=self.numeric_columns,
                    how_many=self.column_fraction * (len(self.numeric_columns + self.categorical_columns)),
                )
                
            case DataErrorApplicability.ANY_COLUMN:
                self.columns_to_corrupt = self.reproducible_operations.sample_from(
                    elements=self.numeric_columns + self.categorical_columns,
                    how_many=self.column_fraction * (len(self.numeric_columns + self.categorical_columns)),
                )
                
            case _ as not_implemented_category:
                raise NotImplementedError(
                    f"Unknown data error applicability type. Got {not_implemented_category}. \
                        Valid options: {[option.value for option in DataErrorApplicability]}."
                )

    def randomly_select_from(
        self, a: List, size: int | float, at_least: int = 1, replace=False
    ) -> List:
        return np.random.choice(a=a, size=max(int(size), at_least), replace=replace)

    def corrupt(self, data: pd.DataFrame) -> Tuple[pd.DataFrame, List, List]:
        # ALWAYS call `super().corrupt(data)` as first line when overriding in subclasses
        self.corrupted_data = data.copy(deep=True)
        self.find_numerical_categorical_columns()
        self.identify_rows_to_corrupt(data)
        self.identify_columns_to_corrupt()
        
        # ALWAYS call `return super().corruption_result_output_tuple()` as last line when overriding
        
    def corruption_result_output_tuple(self) -> Tuple[pd.DataFrame, List, List]:
        return self.corrupted_data, self.rows_to_corrupt, self.columns_to_corrupt
