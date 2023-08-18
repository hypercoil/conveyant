# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Functional containers and sanitised wrappers for safe pickling
"""
import dataclasses
import inspect
from typing import Any, Callable, Mapping, Optional, Sequence, Tuple

from .compositors import reversed_args_compositor
from .emulate import splice_on

# TODO: The system for __allowed__ arguments is incredibly brittle and
#       fails to appropriately mirror/propagate across nested containers. This
#       should be made more robust.


def CONTAINER_TYPES():
    return (
        FunctionWrapper,
        PartialApplication,
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
    splice_on_call: bool = True

    def __post_init__(self):
        if self.splice_on_call:
            object.__setattr__(
                self, '__call__',
                splice_on(self.f, allow_variadic=True)(self.__call__),
            )
            object.__setattr__(
                self, '__signature__', self.__call__.__signature__
            )
        else:
            @splice_on(self.f, allow_variadic=True)
            def _wrapped(**params):
                return self.f(**params)
            object.__setattr__(self, '__signature__', _wrapped.__signature__)
            del _wrapped

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


@dataclasses.dataclass(frozen=True)
class CallableContainer:
    """
    Container for sanitising and manipulating callables.

    This is a frozen dataclass that is used to wrap callables in a way that
    allows for safe pickling. It also allows for the manipulation of the
    wrapped callable's arguments, delayed binding of arguments,
    conditionalisation of arguments, partial application, and composition of
    callables.

    This class (and those that inherit from it) should be used only with
    great care. Depending on how they are constructed and composed, they
    might lose some expected functionality. For example, nesting containers
    without appropriately copying the ``__allowed__`` and ``__conditions__``
    attributes will result in the loss of those attributes. Similarly,
    composing containers without appropriately copying the ``__allowed__``
    and ``__conditions__`` attributes will result in the loss of those
    attributes.

    Parameters
    ----------
    f : Callable
        The callable to be wrapped.
    __allowed__ : tuple or None (default: None)
        A sequence of strings representing the names of the arguments that
        are allowed to be passed to the wrapped function under delayed
        binding. If None, all arguments are allowed. To explicitly allow no
        arguments, pass an empty tuple.
    __conditions__ : mapping
        A mapping from tuples of the form (argument name, argument value) to
        sequences of tuples of the form (argument name, argument value). If an
        argument is passed to the wrapped function and its name and value are
        in the keys of this mapping, then all argument in the value of the
        mapping will also be passed to the wrapped function.
    __priority__ : str (default: ``'eci'``)
        A string representing the priority of the arguments passed to the
        wrapped function. The string should be a permutation of the letters
        ``'e'``, ``'c'``, and ``'i'``, where ``'e'`` represents environment,
        ``'c'`` represents condition, and ``'i'`` represents internal. The
        priority string determines how argument values are assigned in the
        event of a name collision. For example, if the priority string is
        ``'eci'``, then environment arguments will have maximum priority,
        followed by condition arguments, followed by internal arguments.
        * ``'e'`` : environment / explicit arguments. These are arguments
            explicitly passed from the environment or from the user.
        * ``'c'`` : condition arguments. These are arguments that are
            conditionally passed to the wrapped function based on the
            values of other arguments according to the ruleset defined in
            ``__conditions__``.
        * ``'i'`` : internal arguments. These are arguments that are passed to
            the wrapped function by the wrapper, if it is a partial
            application or a composition.
    """

    f: Callable
    pparams: Sequence = dataclasses.field(default_factory=tuple)
    params: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    __allowed__: Optional[Sequence[str]] = dataclasses.field(
        default_factory=tuple
    )
    __conditions__: Mapping[
        Tuple[str, Any], Tuple[str, Any]
    ] = dataclasses.field(default_factory=dict)
    __priority__: str = 'eci'  # environment, condition, internal

    def __init__(
        self,
        f: Callable,
        *pparams: Sequence,
        __allowed__: Optional[Sequence[str]] = (),
        __conditions__: Mapping[
            Tuple[str, Any],
            Sequence[Tuple[str, Any]],
        ] = {},
        __priority__: str = 'eci',
        **params: Mapping,
    ):
        object.__setattr__(self, 'f', f)
        object.__setattr__(self, 'pparams', pparams)
        object.__setattr__(self, 'params', params)
        object.__setattr__(self, '__allowed__', __allowed__)
        object.__setattr__(self, '__conditions__', __conditions__)
        object.__setattr__(self, '__priority__', __priority__)

    def __post_init__(self):
        object.__setattr__(self, '__call__', splice_on(self.f)(self.__call__))
        object.__setattr__(self, '__signature__', self.__call__.__signature__)

    def bind(self, *pparams: Sequence, **params: Mapping):
        if self.__allowed__ is not None:
            params = {
                k: v for k, v in params.items()
                if k in self.__allowed__
            }
        if len(params) == 0:
            return self
        i_params = self.params
        e_params = params
        if self.get_priority('i') < self.get_priority('e'):
            params = {**e_params, **i_params}
        else:
            params = {**i_params, **e_params}
        return PartialApplication(
            self.f,
            *self.pparams,
            *pparams,
            **params,
            __allowed__=self.__allowed__,
            __conditions__=self.__conditions__,
            __priority__=self.__priority__,
        )

    def add_allowed(
        self,
        __allowed__: Sequence[str],
    ) -> 'CallableContainer':
        return self.__class__(
            self.f,
            *self.pparams,
            **self.params,
            __allowed__=tuple(set(__allowed__).union(self.__allowed__)),
            __conditions__=self.__conditions__,
            __priority__=self.__priority__,
        )

    def add_conditions(
        self,
        __conditions__: Mapping[
            Tuple[str, Any],
            Sequence[Tuple[str, Any]],
        ],
    ) -> 'CallableContainer':
        return self.__class__(
            self.f,
            *self.pparams,
            **self.params,
            __allowed__=self.__allowed__,
            __conditions__={**self.__conditions__, **__conditions__},
            __priority__=self.__priority__,
        )

    def set_priority(
        self,
        __priority__: str,
    ) -> 'CallableContainer':
        return self.__class__(
            self.f,
            *self.pparams,
            **self.params,
            __allowed__=self.__allowed__,
            __conditions__=self.__conditions__,
            __priority__=__priority__,
        )

    def get_priority(self, query: str) -> int:
        return self.__priority__.index(query)

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
        e_params = params
        c_params = {}
        i_params = self.params
        if self.__conditions__:
            if self.get_priority('i') < self.get_priority('e'):
                params = {**e_params, **i_params}
            else:
                params = {**i_params, **e_params}
            for k, v in params.items():
                if (k, v) in self.__conditions__:
                    for new_k, new_v in self.__conditions__[(k, v)]:
                        c_params[new_k] = new_v
        params_metadict = {
            'e': e_params,
            'c': c_params,
            'i': i_params,
        }
        ordered = sorted(['e', 'c', 'i'], reverse=True, key=self.get_priority)
        all_params = {}
        for param_key in ordered:
            param_group = params_metadict[param_key]
            all_params = {**all_params, **param_group}
        return self.f(*self.pparams, *pparams, **all_params)

    def __eq__(self, other):
        return self.f == other


class FunctionWrapper(CallableContainer):
    def __init__(
        self,
        f: Callable,
        __allowed__: Optional[Sequence[str]] = (),
        __conditions__: Mapping[
            Tuple[str, Any],
            Sequence[Tuple[str, Any]],
        ] = {},
        __priority__: str = 'eci',
    ):
        return super().__init__(
            f,
            __allowed__=__allowed__,
            __conditions__=__conditions__,
            __priority__=__priority__,
        )


class PartialApplication(CallableContainer):
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
        self.f = FunctionWrapper(self.f)

    def __call__(self, *pparams, **params):
        return self.f(*self.args.pparams, **self.args.params)(
            *pparams, **params
        )


# TODO: If in practice we're not using this, then we should remove it.
#       Otherwise, we should make it more robust. Currently, the priority
#       string does nothing.
@dataclasses.dataclass(frozen=True)
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
    __conditions__: Mapping[
        Tuple[str, Any], Sequence[Tuple[str, Any]]
    ] = dataclasses.field(default_factory=dict)
    __priority__: str = 'eci'

    def __post_init__(self):
        if self.compositor == reversed_args_compositor:
            object.__setattr__(self, 'curried_fn', 'inner')
        if self.curried_fn == 'inner':
            __allowed_outer__ = self.__allowed__
            __conditions_outer__ = self.__conditions__
            __allowed_inner__ = ()
            __conditions_inner__ = {}
        elif self.curried_fn == 'outer':
            __allowed_outer__ = ()
            __conditions_outer__ = {}
            __allowed_inner__ = self.__allowed__
            __conditions_inner__ = self.__conditions__

        if not isinstance(self.compositor, CONTAINER_TYPES()):
            object.__setattr__(
                self, 'compositor', FunctionWrapper(self.compositor)
            )
        if not isinstance(self.outer, CONTAINER_TYPES()):
            object.__setattr__(
                self,
                'outer',
                FunctionWrapper(
                    self.outer,
                    __allowed__=__allowed_outer__,
                    __conditions__=__conditions_outer__,
                    __priority__=self.__priority__,
                ),
            )
        else:
            object.__setattr__(
                self, 'outer', self.outer.add_allowed(__allowed_outer__)
            )
            object.__setattr__(
                self, 'outer', self.outer.add_conditions(__conditions_outer__)
            )

        if not isinstance(self.inner, CONTAINER_TYPES()):
            object.__setattr__(
                self,
                'inner',
                FunctionWrapper(
                    self.inner,
                    __allowed__=__allowed_inner__,
                    __conditions__=__conditions_inner__,
                    __priority__=self.__priority__,
                )
            )
        else:
            object.__setattr__(
                self, 'inner', self.inner.add_allowed(__allowed_inner__)
            )
            object.__setattr__(
                self, 'inner', self.inner.add_conditions(__conditions_inner__)
            )

        if self.curried_fn == 'inner':
            object.__setattr__(self, '__allowed__', self.outer.__allowed__)
        elif self.curried_fn == 'outer':
            object.__setattr__(self, '__allowed__', self.inner.__allowed__)

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
                outer = PartialApplication(
                    self.outer,
                    __allowed__=self.__allowed__,
                    __conditions__=self.__conditions__,
                    __priority__=self.__priority__,
                    **params,
                )
        elif self.curried_fn == 'outer':
            try:
                inner = self.inner.bind(**params)
            except AttributeError:
                inner = PartialApplication(
                    self.inner,
                    __allowed__=self.__allowed__,
                    __conditions__=self.__conditions__,
                    __priority__=self.__priority__,
                    **params,
                )
            outer = self.outer
        return Composition(
            self.compositor,
            outer,
            inner,
            curried_params=self.curried_params,
            __allowed__=self.__allowed__,
            __conditions__=self.__conditions__,
            __priority__=self.__priority__,
        )

    def add_allowed(self, __allowed__):
        return self.__class__(
            self.compositor,
            self.outer,
            self.inner,
            curried_params=self.curried_params,
            __allowed__=tuple(set(__allowed__ + self.__allowed__)),
            __conditions__=self.__conditions__,
            __priority__=self.__priority__,
        )

    def add_conditions(
        self,
        __conditions__: Mapping[Tuple[str, Any], Tuple[str, Any]],
    ) -> 'CallableContainer':
        return self.__class__(
            self.compositor,
            self.outer,
            self.inner,
            curried_params=self.curried_params,
            __allowed__=self.__allowed__,
            __conditions__={**self.__conditions__, **__conditions__},
            __priority__=self.__priority__,
        )

    def set_priority(
        self,
        __priority__: str,
    ) -> 'CallableContainer':
        return self.__class__(
            self.compositor,
            self.outer,
            self.inner,
            curried_params=self.curried_params,
            __allowed__=self.__allowed__,
            __conditions__=self.__conditions__,
            __priority__=__priority__,
        )

    def __call__(self, **params):
        return self.compositor(self.outer, self.inner)(
            **self.curried_params.params
        )(**params)
