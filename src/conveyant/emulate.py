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


def emulate_assignment(strict: bool = True) -> callable:
    def _emulate_assignment(f: callable) -> callable:
        @wraps(f, assigned=WRAPPER_ASSIGNMENTS + ('__kwdefaults__',))
        def wrapped(**params):
            argument = {}
            parameters = inspect.signature(f).parameters
            for k, v in parameters.items():
                argument[k] = params.pop(k, v.default)
            if len(params) > 0 and strict:
                raise TypeError(
                    f'{f.__name__}() got an unexpected keyword argument '
                    f'{list(params.keys())[0]!r}'
                )
            return f(**argument)
        return wrapped
    return _emulate_assignment
