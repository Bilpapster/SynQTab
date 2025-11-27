import numpy as np
import pandas as pd
from scipy.stats import chi2_contingency, ks_2samp, mannwhitneyu
from evaluators.Evaluator import Evaluator


class StatisticalEvaluator(Evaluator):
    def __init__(self, clean_X, clean_y, dirty_X, dirty_y, categorical_features=None):
        super().__init__(clean_X, clean_y, dirty_X, dirty_y)
        self.clean_X = pd.DataFrame(clean_X) if not isinstance(clean_X, pd.DataFrame) else clean_X
        self.dirty_X = pd.DataFrame(dirty_X) if not isinstance(dirty_X, pd.DataFrame) else dirty_X
        self.clean_y = np.array(clean_y)
        self.dirty_y = np.array(dirty_y)
        self.categorical_features = categorical_features or []

    def compute_label_distribution(self) -> dict:
        """
        Compute counts and proportions for each class in clean_y and dirty_y.
        Works for binary or multiclass labels.
        """
        clean_prop = pd.Series(self.clean_y).value_counts(normalize=True).to_dict()
        dirty_prop = pd.Series(self.dirty_y).value_counts(normalize=True).to_dict()

        return {
            "clean_proportions": clean_prop,
            "dirty_proportions": dirty_prop
        }

    def compare_label_distribution(self) -> dict:
        """
        Perform chi-square test on the contingency table of class counts.
        Returns chi-square statistic, p-value, observed and expected frequencies.
        """
        classes = np.unique(np.concatenate([self.clean_y, self.dirty_y]))
        table = []
        for arr in [self.clean_y, self.dirty_y]:
            counts = [np.sum(arr == c) for c in classes]
            table.append(counts)
        table = np.array(table)

        chi2, p, dof, expected = chi2_contingency(table)

        return {
            "chi2_statistic": chi2,
            "p_value": p,
            "degrees_of_freedom": dof,
            "observed_frequencies": table.tolist(),
            "expected_frequencies": expected.tolist()
        }

    def compare_categorical_features(self) -> dict:
        """
        Compare categorical features using chi-square tests.
        """
        results = {}

        for feature in self.categorical_features:
            if feature in self.clean_X.columns and feature in self.dirty_X.columns:
                clean_vals = self.clean_X[feature].dropna()
                dirty_vals = self.dirty_X[feature].dropna()

                # Get all unique values
                all_values = np.unique(np.concatenate([clean_vals, dirty_vals]))

                # Build contingency table
                clean_counts = [np.sum(clean_vals == val) for val in all_values]
                dirty_counts = [np.sum(dirty_vals == val) for val in all_values]

                table = np.array([clean_counts, dirty_counts])

                try:
                    chi2, p, dof, expected = chi2_contingency(table)
                    results[feature] = {
                        "chi2_statistic": chi2,
                        "p_value": p,
                        "degrees_of_freedom": dof,
                        "clean_distribution": dict(zip(all_values, clean_counts)),
                        "dirty_distribution": dict(zip(all_values, dirty_counts))
                    }
                except ValueError:
                    results[feature] = {"error": "Insufficient data for chi-square test"}

        return results

    def compare_numerical_features(self) -> dict:
        """
        Compare numerical features using Kolmogorov-Smirnov and Mann-Whitney U tests.
        """
        results = {}
        numerical_features = [col for col in self.clean_X.columns
                              if col not in self.categorical_features]

        for feature in numerical_features:
            if feature in self.clean_X.columns and feature in self.dirty_X.columns:
                clean_vals = self.clean_X[feature].dropna()
                dirty_vals = self.dirty_X[feature].dropna()

                # Kolmogorov-Smirnov test (distribution comparison)
                ks_stat, ks_p = ks_2samp(clean_vals, dirty_vals)

                # Descriptive statistics
                clean_stats = {
                    "mean": float(clean_vals.mean()),
                    "std": float(clean_vals.std()),
                    "median": float(clean_vals.median()),
                    "min": float(clean_vals.min()),
                    "max": float(clean_vals.max())
                }

                dirty_stats = {
                    "mean": float(dirty_vals.mean()),
                    "std": float(dirty_vals.std()),
                    "median": float(dirty_vals.median()),
                    "min": float(dirty_vals.min()),
                    "max": float(dirty_vals.max())
                }

                results[feature] = {
                    "ks_test": {"statistic": ks_stat, "p_value": ks_p},
                    "clean_stats": clean_stats,
                    "dirty_stats": dirty_stats
                }

        return results

    def evaluate(self) -> dict:
        """
        Evaluate statistical similarity between clean and dirty datasets.
        Returns comprehensive comparison results.
        """
        return {
            "label_comparison": {
                "distribution": self.compute_label_distribution(),
                "chi_square_test": self.compare_label_distribution()
            },
            "categorical_features": self.compare_categorical_features(),
            "numerical_features": self.compare_numerical_features()
        }