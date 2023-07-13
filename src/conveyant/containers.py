# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Functional containers and sanitised wrappers for safe pickling
"""
import dataclasses
import inspect
from typing import Callable, Mapping, Sequence


@dataclasses.dataclass(frozen=True)
class Primitive:
    """
    Primitive function wrapper.

    Forces all arguments to be keyword arguments and forces the wrapped
    function to return a dictionary. Furthermore, any arguments that are
    not specified in the signature of the wrapped function are optionally
    passed directly into the output.
    """

    f: Callable
    name: str
    output: Sequence[str]
    forward_unused: bool = False

    def __call__(self, **params):
        extra_params = {
            k: v for k, v in params.items()
            if k not in inspect.signature(self.f).parameters
        }
        valid_params = {
            k: v for k, v in params.items()
            if k in inspect.signature(self.f).parameters
        }
        out = self.f(**valid_params)
        if self.output is None:
            if not isinstance(out, dict):
                raise TypeError(
                    f'Primitive {self.name} has output spec `None`, so the '
                    f'wrapped function must return a dictionary. Instead, '
                    f'got {out}.'
                )
        elif len(self.output) == 1:
            out = {self.output[0]: out}
        else:
            out = {k: v for k, v in zip(self.output, out)}
        if self.forward_unused:
            return {**extra_params, **out}
        else:
            return out

    def __str__(self):
        return f"Primitive({self.name})"

    def __repr__(self):
        return str(self)


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
