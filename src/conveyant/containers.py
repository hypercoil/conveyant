# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Functional containers and sanitised wrappers for safe pickling
"""
import dataclasses
import inspect
from typing import Callable, Mapping, Optional, Sequence

from .compositors import reversed_args_compositor

# TODO: The system for __allowed__ arguments is incredibly brittle and
#       fails to appropriately mirror/propagate across nested containers. This
#       should be made more robust.


def CONTAINER_TYPES():
    return (
        SanitisedPartialApplication,
        SanitisedFunctionWrapper,
        Composition,
    )


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
            k: v
            for k, v in params.items()
            if k not in inspect.signature(self.f).parameters
        }
        valid_params = {
            k: v
            for k, v in params.items()
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
        elif len(self.output) == 0:
            out = {}
        elif len(self.output) == 1:
            out = {self.output[0]: out}
        else:
            out = {k: v for k, v in zip(self.output, out)}
        if self.forward_unused:
            return {**extra_params, **out}
        else:
            return out

    def __str__(self):
        return f'Primitive({self.name})'

    def __repr__(self):
        return str(self)


@dataclasses.dataclass
class SanitisedFunctionWrapper:
    f: Callable
    __allowed__: Optional[Sequence[str]] = dataclasses.field(
        default_factory=tuple
    )

    def bind(self, *pparams: Sequence, **params: Mapping):
        if self.__allowed__ is not None:
            params = {
                k: v for k, v in params.items()
                if k in self.__allowed__
            }
        if len(params) == 0:
            return self
        return SanitisedPartialApplication(
            self.f,
            *pparams,
            **params,
            __allowed__=self.__allowed__,
        )

    def add_allowed(self, __allowed__):
        return SanitisedFunctionWrapper(
            self.f,
            __allowed__=tuple(set(__allowed__ + self.__allowed__)),
        )

    def __str__(self):
        if isinstance(self.f, Primitive):
            return str(self.f)
        try:
            return self.f.__name__
        except AttributeError:
            return f'wrapped {type(self.f).__name__}'

    def __repr__(self):
        return self.__str__()

    def __call__(self, *pparams, **params):
        return self.f(*pparams, **params)

    def __eq__(self, other):
        return self.f == other


class SanitisedPartialApplication:
    def __init__(
        self,
        f: Callable,
        *pparams: Sequence,
        __allowed__: Optional[Sequence[str]] = (),
        **params: Mapping,
    ):
        self.f = f
        self.pparams = pparams
        self.params = params
        self.__allowed__ = __allowed__

    def bind(self, *pparams: Sequence, **params: Mapping):
        if self.__allowed__ is not None:
            params = {
                k: v for k, v in params.items()
                if k in self.__allowed__
            }
        if len(params) == 0:
            return self
        return SanitisedPartialApplication(
            self.f,
            *self.pparams,
            *pparams,
            **self.params,
            **params,
            __allowed__=self.__allowed__,
        )

    def add_allowed(self, __allowed__):
        return SanitisedPartialApplication(
            self.f,
            *self.pparams,
            **self.params,
            __allowed__=tuple(set(__allowed__ + self.__allowed__)),
        )

    def __str__(self):
        pparams = ', '.join([str(p) for p in self.pparams])
        params = ', '.join([f'{k}={v}' for k, v in self.params.items()])
        if pparams and params:
            all_params = ', '.join([pparams, params])
        elif pparams:
            all_params = pparams
        elif params:
            all_params = params
        if isinstance(self.f, Primitive):
            return f'{self.f}({all_params})'
        try:
            return f'{self.f.__name__}({all_params})'
        except AttributeError:
            return f'(wrapped {type(self.f).__name__})({all_params})'

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
        default_factory=PipelineArgument
    )
    split: bool = False

    def __post_init__(self):
        self.f = SanitisedFunctionWrapper(self.f)

    def __call__(self, *pparams, **params):
        return self.f(*self.args.pparams, **self.args.params)(
            *pparams, **params
        )


@dataclasses.dataclass
class Composition:
    compositor: callable
    outer: callable
    inner: callable
    # 'curried' is a bit of a misnomer, but it's the best I can think of
    curried_fn: str = 'outer'
    curried_params: PipelineArgument = dataclasses.field(
        default_factory=PipelineArgument
    )
    __allowed__: Optional[Sequence[str]] = dataclasses.field(
        default_factory=tuple
    )

    def __post_init__(self):
        if self.compositor == reversed_args_compositor:
            self.curried_fn = 'inner'
        __allowed_inner__ = (
            self.__allowed__ if self.curried_fn == 'outer' else ()
        )
        __allowed_outer__ = (
            self.__allowed__ if self.curried_fn == 'inner' else ()
        )
        if not isinstance(self.compositor, CONTAINER_TYPES()):
            self.compositor = SanitisedFunctionWrapper(self.compositor)
        if not isinstance(self.outer, CONTAINER_TYPES()):
            self.outer = SanitisedFunctionWrapper(
                self.outer, __allowed__=__allowed_outer__
            )
        else:
            self.outer = self.outer.add_allowed(__allowed_outer__)
        if not isinstance(self.inner, CONTAINER_TYPES()):
            self.inner = SanitisedFunctionWrapper(
                self.inner, __allowed__=__allowed_inner__
            )
        else:
            self.inner = self.inner.add_allowed(__allowed_inner__)
        if self.curried_fn == 'inner':
            self.__allowed__ = self.outer.__allowed__
        elif self.curried_fn == 'outer':
            self.__allowed__ = self.inner.__allowed__

    def bind_curried(self, **params):
        return Composition(
            self.compositor,
            self.outer,
            self.inner,
            curried_params=PipelineArgument(**params),
        )

    def bind(self, **params):
        if self.__allowed__ is not None:
            params = {
                k: v for k, v in params.items()
                if k in self.__allowed__
            }
        if len(params) == 0:
            return self
        if self.curried_fn == 'inner':
            inner = self.inner
            try:
                outer = self.outer.bind(**params)
            except AttributeError:
                outer = SanitisedPartialApplication(
                    self.outer,
                    __allowed__=self.__allowed__,
                    **params,
                )
        elif self.curried_fn == 'outer':
            try:
                inner = self.inner.bind(**params)
            except AttributeError:
                inner = SanitisedPartialApplication(
                    self.inner,
                    __allowed__=self.__allowed__,
                    **params,
                )
            outer = self.outer
        return Composition(
            self.compositor,
            outer,
            inner,
            curried_params=self.curried_params,
            __allowed__=self.__allowed__,
        )

    def add_allowed(self, __allowed__):
        return Composition(
            self.compositor,
            self.outer,
            self.inner,
            curried_params=self.curried_params,
            __allowed__=tuple(set(__allowed__ + self.__allowed__)),
        )

    def __call__(self, **params):
        return self.compositor(self.outer, self.inner)(
            **self.curried_params.params
        )(**params)
