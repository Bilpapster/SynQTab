import tempfile
import os
import pandas as pd
import desbordante as db
from evaluators.Evaluator import Evaluator


class FunctionalDependencies(Evaluator):
    def __init__(self):
        pass

    def evaluate(self, data_1: pd.DataFrame) -> dict:
        # Create a temporary file
        temp_fd, temp_path = tempfile.mkstemp(suffix='.csv', text=True)

        try:
            # Write DataFrame to temporary CSV
            data_1.to_csv(temp_path, index=False)
            print(f"Temporary CSV file created at: {temp_path}")

            # Load data from temporary file
            algo = db.fd.algorithms.Default()

            algo.load_data(table=(temp_path, ',', True))
            print("Data loaded into FD discovery algorithm.")

            algo.execute()
            print("Executed FD discovery algorithm.")

            # Collect functional dependencies
            fds = [str(fd) for fd in algo.get_fds()]

            return {
                "count": len(fds),
                "functional_dependencies": fds
            }

        finally:
            # Clean up: close and delete temporary file
            os.close(temp_fd)
            os.remove(temp_path)

            # Remove log file if it exists
            if os.path.exists('myeasylog.log'):
                os.remove('myeasylog.log')