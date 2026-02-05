from synqtab.errors import DataError


class Placeholder(DataError):

    CATEGORICAL_MISSING_VALUE = "UNKNOWN"  # TODO: Discuss internally
    NUMERIC_MISSING_VALUE = -1  # TODO: Discuss internally
    # See https://www.sciencedirect.com/science/article/pii/S0306437925000341 -> page 4
    # Specifically, last paragraph of 3.2 and footnote 2 on the same page.

    def data_error_applicability(self):
        from synqtab.errors import DataErrorApplicability
        
        return DataErrorApplicability.ANY_COLUMN
    
    def full_name(self):
        return "Placeholders"
    
    def short_name(self):
        from synqtab.enums import DataErrorType
        
        return DataErrorType.PLACEHOLDER

    # Based on Jenga's implementation
    # https://github.com/schelterlabs/jenga/blob/a8bd74a588176e64183432a0124553c774adb20d/src/jenga/corruptions/generic.py#L26
    def _apply_corruption(self, data_to_corrupt, rows_to_corrupt, columns_to_corrupt, **kwargs):
        for column_to_corrupt in columns_to_corrupt:
            if column_to_corrupt in self.numeric_columns:
                data_to_corrupt.loc[rows_to_corrupt, column_to_corrupt] = self.NUMERIC_MISSING_VALUE
                continue
            
            #else if column to corrupt is categorical, transform to object, pollute and transform back to category
            data_to_corrupt[column_to_corrupt] = data_to_corrupt[column_to_corrupt].astype('object')
            data_to_corrupt.loc[rows_to_corrupt, column_to_corrupt] = self.CATEGORICAL_MISSING_VALUE
            data_to_corrupt[column_to_corrupt] = data_to_corrupt[column_to_corrupt].astype('category')
            

        return data_to_corrupt
