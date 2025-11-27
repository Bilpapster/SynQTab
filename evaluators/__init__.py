"""
Evaluators package for SynQTab project.
Contains classes for evaluating synthetic data quality and statistical properties.
"""

from .Evaluator import Evaluator
from .StatisticalEvaluator import StatisticalEvaluator

__all__ = [
    'Evaluator',
    'StatisticalEvaluator'
]