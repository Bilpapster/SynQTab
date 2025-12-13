from pollution import DataError
import pandas as pd
from typing import List, Tuple

from pollution.DataErrorApplicability import DataErrorApplicability


class CategoricalShift(DataError):

    def data_error_applicability(self) -> DataErrorApplicability:
        return DataErrorApplicability.CATEGORICAL_ONLY

    def corrupt(self, data: pd.DataFrame) -> Tuple[pd.DataFrame, List, List]:
        super().corrupt(data)
        # TODO
        return super().corruption_result_output_tuple()
