# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Assignment emulator
~~~~~~~~~~~~~~~~~~~
Emulate assignment of keyword arguments to function parameters.
"""
import inspect
from functools import WRAPPER_ASSIGNMENTS, wraps
from typing import Any, Mapping, Optional, Sequence, Tuple, Type


def emulate_assignment(
    strict: bool = True,
    allow_variadic: bool = False,
) -> callable:
    def _emulate_assignment(f: callable) -> callable:
        @wraps(f, assigned=WRAPPER_ASSIGNMENTS + ('__kwdefaults__',))
        def wrapped(**params):
            argument = {}
            parameters = inspect.signature(f).parameters
            for k, v in parameters.items():
                if v.kind == v.VAR_KEYWORD:
                    continue
                argument[k] = params.pop(k, v.default)
                if argument[k] is inspect._empty:
                    raise TypeError(
                        f'{f.__name__}() missing required argument {k!r}'
                    )
            if len(params) > 0 and strict and not allow_variadic:
                raise TypeError(
                    f'{f.__name__}() got an unexpected keyword argument '
                    f'{list(params.keys())[0]!r}'
                )
            elif allow_variadic:
                argument = {**params, **argument}
            return f(**argument)
        return wrapped
    return _emulate_assignment


def splice_on(
    g: callable,
    occlusion: Sequence[str] = (),
    expansion: Optional[Mapping[str, Tuple[Type, Any]]] = None,
    allow_variadic: bool = False,
    strict_emulation: bool = True,
) -> callable:
    """
    Splice the decorated/wrapped function's parameters on another function
    `g`, occluding parameters in `occlusion`.

    All arguments are forced to be keyword arguments.
    """
    def _splice_on(f: callable) -> callable:
        @wraps(g, assigned=WRAPPER_ASSIGNMENTS + ('__kwdefaults__',))
        def h(**params):
            return f(**params)
        f_params = inspect.signature(f).parameters
        g_params = inspect.signature(g).parameters
        h_params = [
            p for p in f_params.values()
            if p.kind != p.VAR_KEYWORD
        ]
        if expansion is not None:
            for k, (t, v) in expansion.items():
                h_params.append(
                    inspect.Parameter(
                        name=k,
                        kind=inspect.Parameter.KEYWORD_ONLY,
                        default=inspect.Parameter.empty
                        if v is inspect.Parameter.empty
                        else v,
                        annotation=t,
                    )
                )
        h_params.extend(
            p for p in g_params.values()
            if p.name not in occlusion
            and p.kind != p.VAR_KEYWORD
        )
        h_params = [p.replace(kind=p.KEYWORD_ONLY) for p in h_params]
        if allow_variadic:
            try:
                h_params.append(
                    next(
                        p for p in g_params.values()
                        if p.kind == p.VAR_KEYWORD
                    )
                )
            except StopIteration:
                pass
        h_params_unique = []
        param_names = set()
        for p in h_params:
            if p.name not in param_names:
                h_params_unique.append(p)
                param_names.add(p.name)
        h.__signature__ = inspect.signature(h).replace(
            parameters=h_params_unique
        )
        return emulate_assignment(
            strict=strict_emulation,
            allow_variadic=allow_variadic,
        )(h)
    return _splice_on
