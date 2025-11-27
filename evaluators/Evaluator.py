from abc import ABC

class Evaluator(ABC):
    def __init__(self, clean_X, clean_y, dirty_X, dirty_y):
        """
        Initialize the evaluator with synthetic datasets coming from clean and dirty datasets.
        :param clean_X: Synthetic features from clean dataset
        :param clean_y: Synthetic labels from clean dataset
        :param dirty_X: Synthetic features from dirty dataset
        :param dirty_y: Synthetic labels from dirty dataset
        """
        self.clean_X = clean_X
        self.clean_y = clean_y
        self.dirty_X = dirty_X
        self.dirty_y = dirty_y

    def evaluate(self) -> dict:
        """
        Evaluate the impact of Data Quality on synthetic datasets coming from clean and dirty datasets.
        :return:
        """
        raise NotImplementedError("Subclasses must implement this method")