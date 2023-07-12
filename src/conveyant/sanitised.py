# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Sanitised function wrappers
"""
import dataclasses
from typing import Callable, Mapping, Optional, Sequence


@dataclasses.dataclass
class SanitisedFunctionWrapper:
    f: Callable
    def __str__(self):
        return self.f.__name__

    def __repr__(self):
        return self.__str__()

    def __call__(self, *pparams, **params):
        return self.f(*pparams, **params)


class SanitisedPartialApplication:
    def __init__(self, f: Callable, *pparams: Sequence, **params: Mapping):
        self.f = f
        self.pparams = pparams
        self.params = params

    def __str__(self):
        pparams = ", ".join([str(p) for p in self.pparams])
        params = ", ".join([f"{k}={v}" for k, v in self.params.items()])
        if pparams and params:
            all_params = ", ".join([pparams, params])
        elif pparams:
            all_params = pparams
        elif params:
            all_params = params
        return f"{self.f.__name__}({all_params})"
    
    def __repr__(self):
        return self.__str__()
    
    def __call__(self, *pparams, **params):
        return self.f(*self.pparams, *pparams, **self.params, **params)


@dataclasses.dataclass
class PipelineArgument:
    def __init__(self, *pparams, **params) -> None:
        self.pparams = pparams
        self.params = params


@dataclasses.dataclass
class PipelineStage:
    f: callable
    args: PipelineArgument = dataclasses.field(
        default_factory=PipelineArgument)
    split: bool = False

    def __post_init__(self):
        self.f = SanitisedFunctionWrapper(self.f)

    def __call__(self, *pparams, **params):
        return self.f(*self.args.pparams, **self.args.params)(
            *pparams, **params
        )
