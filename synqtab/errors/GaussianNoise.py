from synqtab.errors.DataError import DataError


# Based on https://github.com/schelterlabs/jenga/blob/a8bd74a588176e64183432a0124553c774adb20d/src/jenga/corruptions/numerical.py#L9
class GaussianNoise(DataError):

    # TODO: Discuss Internally
    SCALING_MIN = 1
    SCALING_MAX = 5
    NORMAL_MEAN = 0

    def data_error_applicability(self):
        from synqtab.errors import DataErrorApplicability
        
        return DataErrorApplicability.NUMERIC_ONLY
    
    def full_name(self):
        return "Gaussian Noise"
    
    def short_name(self):
        from synqtab.enums import DataErrorType
        
        return str(DataErrorType.GAUSSIAN_NOISE)

    def _apply_corruption(self, data_to_corrupt, rows_to_corrupt, columns_to_corrupt, **kwargs):
        from numpy import std
        from synqtab.reproducibility import ReproducibleOperations
        
        for column_to_corrupt in columns_to_corrupt:            
            stddev = std(data_to_corrupt[column_to_corrupt])
            scale = ReproducibleOperations.uniform(
                low=self.SCALING_MIN, high=self.SCALING_MAX
            )
            noise = ReproducibleOperations.normal(
                loc=self.NORMAL_MEAN, scale=scale * stddev, size=len(rows_to_corrupt)
            )
            
            # if the column is declared as 'int*' in pandas (e.g,, 'int64'), then corrupt realistically 
            # by casting the noise to int. This simulates that the noise is produced from the source
            # but the actual values of the dataframe are casted to int so they are not distinguishable
            if 'int' in str(data_to_corrupt.dtypes[column_to_corrupt]):
                noise = [max(int(noise_factor), 1) for noise_factor in noise]
                        
            data_to_corrupt.loc[rows_to_corrupt, column_to_corrupt] += noise

        return data_to_corrupt
