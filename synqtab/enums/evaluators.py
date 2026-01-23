from enum import Enum


class EvaluationMethod(Enum):
    DCR = 'DCR'
    DFD = 'DFD'
    DPR = 'DPR'
    HFD = 'HFD'
    IFO = 'IFO'
    LOF = 'LOF'
    APR = 'APR'
    ARC = 'ARC'
    AR2 = 'AR2'
    EFF = 'EFF'
    QLT = 'QLT'


SINGULAR_EVALUATORS: list[EvaluationMethod] = [
    EvaluationMethod.DFD,
    EvaluationMethod.HFD,
    EvaluationMethod.IFO,
    EvaluationMethod.LOF,
]


DUAL_EVALUATORS: list[EvaluationMethod] = [
    EvaluationMethod.DCR,
    EvaluationMethod.DPR,
    EvaluationMethod.APR,
    EvaluationMethod.ARC,
    EvaluationMethod.AR2,
    EvaluationMethod.EFF,
    EvaluationMethod.QLT,
]


QUALITY_EVALUATORS: list[EvaluationMethod] = [
    EvaluationMethod.DFD,
    EvaluationMethod.HFD,
    EvaluationMethod.IFO,
    EvaluationMethod.LOF,
    EvaluationMethod.QLT,
]


ML_FOCUSED_EVALUATORS: list[EvaluationMethod] = [
    EvaluationMethod.APR,
    EvaluationMethod.ARC,
    EvaluationMethod.AR2,
    EvaluationMethod.EFF,
]


PRIVACY_EVALUATORS: list[EvaluationMethod] = [
    EvaluationMethod.DCR,
    EvaluationMethod.DPR,
]