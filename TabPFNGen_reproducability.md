# Reproducibility Modifications in TabPFN Library

This document summarizes the changes needed in the TabPFN library to ensure deterministic results by setting a fixed random seed.

---

## 1. `__init__` Function

In the init function add:

```python
torch.manual_seed(42)
```