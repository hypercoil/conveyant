# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Elementary replication
"""
from itertools import chain, cycle, product
from math import prod
from typing import Any, Literal, Optional, Sequence, Union

from .config import aggregator_types


def _flatten(xs):
    if not isinstance(xs, aggregator_types):
        xs = (xs,)
    for x in xs:
        if isinstance(x, aggregator_types):
            yield from _flatten(x)
        else:
            yield x


def _flatten_to_depth(xs, depth):
    if depth is None:
        depth = float('inf')
    if not isinstance(xs, aggregator_types):
        xs = (xs,)
    for x in xs:
        if isinstance(x, aggregator_types) and depth > 0:
            yield from _flatten_to_depth(x, depth - 1)
        else:
            yield x


def _nominal_length(
    var: Any,
    maximum_aggregation_depth: Optional[int] = None,
) -> int:
    if isinstance(var, aggregator_types):
        if maximum_aggregation_depth is None:
            maximum_aggregation_depth = float('inf')
        if maximum_aggregation_depth > 0:
            return sum(
                _nominal_length(
                    v,
                    maximum_aggregation_depth=maximum_aggregation_depth - 1,
                )
                for v in var
            )
        else:
            return 1
    else:
        return 1


def _get_n_replicates(
    spec: Union[Sequence[Union[Sequence, str]], str],
    params: dict,
    weave_type: Literal['maximal', 'minimal', 'strict'] = 'maximal',
    maximum_aggregation_depth: Optional[int] = None,
) -> int:
    if isinstance(spec, str):
        return _nominal_length(
            var=params[spec],
            maximum_aggregation_depth=maximum_aggregation_depth,
        )
    else:
        gen = (
            _get_n_replicates(
                spec=s,
                params=params,
                weave_type=weave_type,
                maximum_aggregation_depth=maximum_aggregation_depth,
            )
            for s in spec
        )
    if isinstance(spec, list):
        return prod(gen)
    elif isinstance(spec, tuple):
        if weave_type == 'maximal':
            return max(gen)
        elif weave_type == 'minimal':
            return min(gen)
        elif weave_type == 'strict':
            gen = list(gen)
            assert len(set(gen)) == 1
            return gen[0]
        else:
            raise ValueError(f'Unrecognized weave_type: {weave_type}')
    else:
        raise ValueError(f'Unrecognized spec type: {type(spec)}')


def cycle_to_length(
    var: str,
    params: dict,
    length: int,
    maximum_aggregation_depth: Optional[int] = None,
) -> int:
    nl = _nominal_length(
        var=params[var],
        maximum_aggregation_depth=maximum_aggregation_depth,
    )
    src = list(_flatten_to_depth(params[var], maximum_aggregation_depth))
    val = src * (length // nl)
    val += src[: length % nl]
    return val


def _tuple_spec(
    spec: Sequence,
    params: dict,
    maximum_aggregation_depth: Optional[int] = None,
    weave_type: Literal['maximal', 'minimal', 'strict'] = 'maximal',
) -> Sequence:
    children = [
        _replicate(
            spec=e,
            params=params,
            maximum_aggregation_depth=maximum_aggregation_depth,
            weave_type=weave_type,
        )
        for e in spec
    ]
    children = list(_flatten_to_depth(children, depth=1))
    if weave_type == 'minimal':
        return list(zip(*zip(*children)))
    elif weave_type == 'strict':
        return list(zip(*zip(*children, strict=True)))
    elif weave_type == 'maximal':
        ll = [len(e) for e in children]
        argmax = ll.index(max(ll))
        _children = [
            cycle(children[i]) if i != argmax else children[i]
            for i in range(len(children))
        ]
        return list(zip(*zip(*_children)))


def _replicate(
    spec: Union[Sequence, str],
    params: dict,
    maximum_aggregation_depth: Optional[int] = None,
    weave_type: Literal['maximal', 'minimal', 'strict'] = 'maximal',
) -> Sequence:
    if isinstance(spec, str):
        return [
            list(
                _flatten_to_depth(
                    params[spec],
                    maximum_aggregation_depth,
                )
            )
        ]
    elif isinstance(spec, list):
        children = [
            _replicate(
                spec=e,
                params=params,
                maximum_aggregation_depth=maximum_aggregation_depth,
                weave_type=weave_type,
            )
            for e in spec
        ]
        # God save us all
        transposed = list(product(*[list(zip(*v)) for v in children]))
        return list(zip(*[chain(*v) for v in transposed]))
    elif isinstance(spec, tuple):
        return _tuple_spec(
            spec=spec,
            params=params,
            maximum_aggregation_depth=maximum_aggregation_depth,
            weave_type=weave_type,
        )


def replicate(
    spec: Union[Sequence[Union[Sequence, str]], str],
    weave_type: Literal['maximal', 'minimal', 'strict'] = 'maximal',
    n_replicates: Optional[int] = None,
    maximum_aggregation_depth: Optional[int] = None,
    broadcast_out_of_spec: bool = False,
) -> callable:
    if list not in aggregator_types:
        raise ValueError(
            f'aggregator_types must contain list: {aggregator_types}'
        )
    def transformer(**params):
        _n_replicates = n_replicates
        if n_replicates is None:
            if not spec:
                _n_replicates = max(
                    _nominal_length(
                        var=params[k],
                        maximum_aggregation_depth=maximum_aggregation_depth,
                    )
                    for k in params.keys()
                )
            else:
                _n_replicates = _get_n_replicates(
                    spec=spec,
                    params=params,
                    weave_type=weave_type,
                    maximum_aggregation_depth=maximum_aggregation_depth,
                )
        spec_flat = list(_flatten(spec))
        repl_vals = _replicate(
            spec=spec,
            params=params,
            maximum_aggregation_depth=maximum_aggregation_depth,
            weave_type=weave_type,
        )
        repl_params = {k: v for k, v in zip(spec_flat, repl_vals)}
        for k in repl_params.keys():
            repl_params[k] = cycle_to_length(
                var=k,
                params=repl_params,
                length=_n_replicates,
                maximum_aggregation_depth=maximum_aggregation_depth,
            )
        if broadcast_out_of_spec:
            for k in params.keys():
                if k not in spec_flat:
                    repl_params[k] = cycle_to_length(
                        var=k,
                        params=params,
                        length=_n_replicates,
                        maximum_aggregation_depth=maximum_aggregation_depth,
                    )
        else:
            for k, v in params.items():
                if k not in spec_flat:
                    repl_params[k] = [v]

        repl_params = {
            k: list(v) if type(v) in aggregator_types else [v]
            for k, v in repl_params.items()
        }

        return repl_params
    return transformer
