import abc
from abc import ABC
from enum import Enum

class Task(Enum):
    """
    Enum for different types of machine learning tasks.
    """
    CLASSIFICATION = "classification"
    REGRESSION = "regression"

class Generator(ABC):
    def __init__(self):
        super().__init__()

    def generate(self, X_initial, y_initial, task: str):
        """Train the generator model."""
        pass