# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Functional compositors
~~~~~~~~~~~~~~~~~~~~~~
Composition operators.
"""
from itertools import chain
from typing import Literal, Mapping, Optional, Sequence

from .replicate import replicate


def _seq_to_dict(
    seq: Sequence[Mapping],
    merge_type: Optional[Literal['union', 'intersection']] = None,
) -> Mapping[str, Sequence]:
    if merge_type is None:
        keys = seq[0].keys()
    else:
        keys = [set(s.keys()) for s in seq]
        if merge_type == 'union':
            keys = set.union(*keys)
        elif merge_type == 'intersection':
            keys = set.intersection(*keys)
    if merge_type == 'union':
        NULLSTR = '__ignore__'
        dct = {k: tuple(r.get(k, NULLSTR) for r in seq) for k in keys}
        dct = {k: tuple(v for v in dct[k] if v is not NULLSTR) for k in keys}
    else:
        dct = {k: tuple(r[k] for r in seq) for k in keys}
    for k in dct:
        try:
            # We don't want this path for just any iterable -- in particular,
            # definitely not for np.ndarray, pd.DataFrame, strings, etc.
            assert isinstance(dct[k][0], tuple) or isinstance(dct[k][0], list)
            dct[k] = tuple(chain(*dct[k]))
        except (TypeError, AssertionError, IndexError):
            pass
    return dct


def _dict_to_seq(
    dct: Mapping[str, Sequence],
) -> Sequence[Mapping]:
    keys = dct.keys()
    seq = tuple(
        dict(zip(keys, v))
        for v in zip(*dct.values())
    )
    return seq


def direct_compositor(
    f_outer: callable,
    f_inner: callable,
) -> callable:
    def transformed_f_outer(**f_outer_params):
        def transformed_f_inner(**f_inner_params):
            return f_outer(**{**f_outer_params, **f_inner(**f_inner_params)})
        return transformed_f_inner
    return transformed_f_outer


def reversed_args_compositor(
    f_outer: callable,
    f_inner: callable,
) -> callable:
    def transformed_f_inner(**f_inner_params):
        def transformed_f_outer(**f_outer_params):
            return f_outer(**{**f_outer_params, **f_inner(**f_inner_params)})
        return transformed_f_outer
    return transformed_f_inner


def close_imapping_compositor(
    inner_mapping: Optional[Mapping] = None,
    outer_mapping: Optional[Mapping] = None,
    map_spec: Optional[Sequence[str]] = None,
    n_replicates: Optional[int] = None,
    weave_type: Literal['maximal', 'minimal', 'strict'] = 'maximal',
    maximum_aggregation_depth: Optional[int] = None,
    broadcast_out_of_spec: bool = False,
    merge_type: Optional[Literal['union', 'intersection']] = 'union',
) -> callable:
    map_spec = map_spec or []
    map_spec_transformer = replicate(
        spec=map_spec,
        weave_type=weave_type,
        n_replicates=n_replicates,
        maximum_aggregation_depth=maximum_aggregation_depth,
        broadcast_out_of_spec=broadcast_out_of_spec,
    )
    def imapping_compositor(
        f_outer: callable,
        f_inner: callable,
    ) -> callable:
        def transformed_f_outer(**f_outer_params):
            def transformed_f_inner(**f_inner_params):
                ret = []
                _inner_mapping = inner_mapping or {}
                _outer_mapping = outer_mapping or {}
                params_mapped = map_spec_transformer(
                    **{
                        **f_outer_params,
                        **f_inner_params,
                        **_inner_mapping,
                        **_outer_mapping,
                    }
                )
                f_inner_params_mapped = {
                    k: v
                    for k, v in params_mapped.items()
                    if (k in f_inner_params or k in _inner_mapping)
                }
                f_outer_params_mapped = {
                    k: v
                    for k, v in params_mapped.items()
                    if (k in f_outer_params or k in _outer_mapping)
                }
                _n_replicates = max(len((v)) for v in params_mapped.values())
                inner_params_hash_dict = {}
                for i in range(_n_replicates):
                    f_inner_params_mapped_i = {
                        k: v[i % len(v)]
                        for k, v in f_inner_params_mapped.items()
                    }
                    # TODO: This is ... not a great hash
                    inner_params_hash = hash(str(f_inner_params_mapped_i))
                    if inner_params_hash in inner_params_hash_dict:
                        inner_i_result = (
                            inner_params_hash_dict[inner_params_hash]
                        )
                    else:
                        inner_i_result = f_inner(**f_inner_params_mapped_i)
                        inner_params_hash_dict[inner_params_hash] = (
                            inner_i_result
                        )
                    f_outer_params_mapped_i = {
                        k: v[i % len(v)]
                        for k, v in f_outer_params_mapped.items()
                    }
                    ret.append(
                        f_outer(
                            **{
                                **inner_i_result,
                                **f_outer_params_mapped_i,
                            }
                        )
                    )
                return _seq_to_dict(ret, merge_type=merge_type)
            return transformed_f_inner
        return transformed_f_outer
    return imapping_compositor


def close_omapping_compositor(
    mapping: Optional[Mapping] = None,
    map_spec: Optional[Sequence[str]] = None,
    n_replicates: Optional[int] = None,
    weave_type: Literal['maximal', 'minimal', 'strict'] = 'maximal',
    maximum_aggregation_depth: Optional[int] = None,
    broadcast_out_of_spec: bool = False,
    merge_type: Optional[Literal['union', 'intersection']] = 'union',
) -> callable:
    # TODO: distinguish between "mapping" (over outputs) compositors and
    # "replicating" (over inputs) compositors in docstring.
    map_spec = map_spec or []
    map_spec_transformer = replicate(
        spec=map_spec,
        weave_type=weave_type,
        n_replicates=n_replicates,
        maximum_aggregation_depth=maximum_aggregation_depth,
        broadcast_out_of_spec=broadcast_out_of_spec,
    )
    def omapping_compositor(
        f_outer: callable,
        f_inner: callable,
    ) -> callable:
        def transformed_f_outer(**f_outer_params):
            def transformed_f_inner(**f_inner_params):
                ret = []
                _mapping = mapping or {}
                out = f_inner(**f_inner_params)
                f_outer_params_mapped = map_spec_transformer(
                    **{**f_outer_params, **out, **_mapping}
                )
                try:
                    out = _dict_to_seq(out)
                except TypeError:
                    # We really shouldn't enter this branch, since the
                    # compositor does nothing in this case
                    out = [out]
                if mapping or n_replicates:
                    _n_replicates = n_replicates or len(
                        next(iter(mapping.values()))
                    )
                    assert len(out) == _n_replicates, (
                        f'The length of the output of the inner function '
                        f'({len(out)}) must be equal to the length of the '
                        f'mapped values ({_n_replicates})'
                    )
                for i, o in enumerate(out):
                    f_outer_params_i = {
                        **{
                            k: f_outer_params_mapped[k][i]
                            if len(f_outer_params_mapped[k]) > 1
                            else f_outer_params_mapped[k][0]
                            for k in f_outer_params_mapped
                        },
                        **{k: v[i] for k, v in _mapping.items()},
                        **o,
                    }
                    ret.append(f_outer(**f_outer_params_i))
                return _seq_to_dict(ret, merge_type=merge_type)
            return transformed_f_inner
        return transformed_f_outer
    return omapping_compositor


# def delayed_compositor(
#     f_outer: callable,
#     f_inner: callable,
# ) -> callable:
#     def transformed_f_outer(**f_outer_params):
#         def transformed_f_inner(**f_inner_params):
#             return f_outer, f_outer_params, f_inner, f_inner_params
#         return transformed_f_inner
#     return transformed_f_outer


def delayed_outer_compositor(
    f_outer: callable,
    f_inner: callable,
) -> callable:
    def transformed_f_outer(**f_outer_params):
        def transformed_f_inner(**f_inner_params):
            out = f_inner(**f_inner_params)
            return out, f_outer, f_outer_params
        return transformed_f_inner
    return transformed_f_outer
