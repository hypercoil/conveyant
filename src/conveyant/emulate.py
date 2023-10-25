# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Assignment emulator
~~~~~~~~~~~~~~~~~~~
Emulate assignment of keyword arguments to function parameters.
"""
import inspect
from functools import WRAPPER_ASSIGNMENTS, WRAPPER_UPDATES
from functools import wraps as wraps_orig
from textwrap import indent
from typing import Any, Mapping, Optional, Sequence, Tuple, Type


def wraps(
    wrapped: callable,
    assigned: Optional[Sequence[str]] = None,
    updated: Optional[Sequence[str]] = None,
) -> callable:
    """
    A version of functools.wraps that preserves the metadata and signature of
    the wrapped function.
    """
    if assigned is None:
        assigned = WRAPPER_ASSIGNMENTS
    if updated is None:
        updated = WRAPPER_UPDATES
    wrapper = wraps_orig(
        wrapped,
        assigned=assigned,
        updated=updated,
    )
    if hasattr(wrapped, '__signature__'):
        wrapper.__signature__ = inspect.signature(wrapped)
    if hasattr(wrapped, '__meta__'):
        wrapper.__meta__ = wrapped.__meta__
    return wrapper


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
    kwonly_only: bool = False,
    strict_emulation: bool = True,
    doc_subs: Optional[Mapping[str, Tuple[str, Mapping[str, str]]]] = None,
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
            and (not kwonly_only or p.kind == p.KEYWORD_ONLY)
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
            and (not kwonly_only or p.kind == p.KEYWORD_ONLY)
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
        if doc_subs is not None:
            metadata = getattr(h, '__meta__', {})
            doc_metadata = metadata.get('__doc__', {})
            doc_metadata['subs'] = {**doc_metadata.get('subs', {}), **doc_subs}
            metadata['__doc__'] = doc_metadata
            h.__meta__ = metadata
        return emulate_assignment(
            strict=strict_emulation,
            allow_variadic=allow_variadic,
        )(h)
    return _splice_on


def splice_docstring(
    f: callable,
    template: Mapping[str, Mapping[str, str]],
    base_str: Optional[str] = None,
    returns: Optional[str] = None,
    indentation: Optional[str] = None,
    missingdoc: Optional[str] = None,
) -> callable:
    """
    Splice the docstring of `f` with `template`, using the function's
    signature to infer parameters.
    """
    @wraps(f)
    def g(*args, **kwargs):
        return f(*args, **kwargs)
    parameters = inspect.signature(f).parameters
    if missingdoc is None:
        missingdoc = '<No description>'
    doc_template = (
        getattr(f, '__meta__', {}).get('__doc__', {}).get('desc', None)
        or base_str
        or f.__doc__
        or missingdoc
    )
    if indentation is None:
        indentation = '    '
    doc_template += '\n\nParameters\n----------\n'
    doc_vars = {}
    subs = getattr(f, '__meta__', {}).get('__doc__', {}).get('subs', {})
    for k in parameters:
        k_query, v_format = subs.get(k, (k, {}))
        v = template.get(k_query, {})
        doc_vars[f'{k}_type'] = v.get(
            'type', parameters[k].annotation.__name__
        )
        doc_vars[f'{k}_desc'] = indent(
            v.get('desc', missingdoc), # parameters[k].annotation.__doc__),
            indentation,
        ).format(**v_format)
        default = v.get('default', parameters[k].default)
        if default is inspect.Parameter.empty:
            doc_vars[f'{k}_default'] = ''
        else:
            doc_vars[f'{k}_default'] = f'(default: ``{default}``)'
        doc_template += (
            f'{k} : {{{k}_type}} {{{k}_default}}\n'
            f'{{{k}_desc}}\n'
        )

    if returns is not None:
        doc_template += '\nReturns\n-------\n'
        doc_template += f'{returns}\n'

    g.__doc__ = doc_template.format(**doc_vars)
    return g
