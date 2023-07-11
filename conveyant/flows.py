# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Functional control flows
~~~~~~~~~~~~~~~~~~~~~~~~
Simple functional transformations for configuring control flows of functions.
"""
from itertools import chain
from typing import Any, Literal, Mapping, Optional, Sequence

from .compositors import (
    _seq_to_dict,
    close_imapping_compositor,
    close_omapping_compositor,
    delayed_outer_compositor,
    direct_compositor,
)
from .replicate import replicate


def null_prim(**params):
    return params


def null_transform(
    f: callable,
    compositor: callable = direct_compositor,
) -> callable:
    return f


def null_stage() -> callable:
    return null_transform


def inject_params() -> callable:
    def transform(
        f: callable,
        compositor: callable = direct_compositor,
    ) -> callable:
        def transformer_f(**params):
            return params

        def f_transformed(**params):
            return compositor(f, transformer_f)()(**params)
        return f_transformed
    return transform


def ichain(*pparams) -> callable:
    def transform(
        f: callable,
        compositor: callable = direct_compositor,
    ) -> callable:
        for p in reversed(pparams):
            f = p(f, compositor=compositor)
        return f
    return transform


def ochain(*pparams) -> callable:
    def transform(
        f: callable,
        compositor: callable = direct_compositor,
    ) -> callable:
        for p in pparams:
            f = p(f, compositor=compositor)
        return f
    return transform


def iochain(
    f: callable,
    ichain: Optional[callable] = None,
    ochain: Optional[callable] = None,
    compositor: callable = direct_compositor,
) -> callable:
    if ichain is not None:
        f = ichain(f, compositor=compositor)
    if ochain is not None:
        f = ochain(f, compositor=compositor)
    return f


def split_chain(
    *chains: Sequence[callable],
    map_spec: Optional[Sequence[str]] = None,
    weave_type: Literal['maximal', 'minimal', 'strict'] = 'maximal',
    maximum_aggregation_depth: Optional[int] = None,
    broadcast_out_of_spec: bool = False,
    merge_type: Optional[Literal['union', 'intersection']] = 'union',
) -> callable:
    map_spec = map_spec or []
    map_spec_transformer = replicate(
        spec=map_spec,
        weave_type=weave_type,
        n_replicates=len(chains),
        maximum_aggregation_depth=maximum_aggregation_depth,
        broadcast_out_of_spec=broadcast_out_of_spec,
    )
    def transform(
        f: callable,
        compositor: callable = direct_compositor,
    ) -> callable:
        fs_transformed = tuple(c(f, compositor=compositor) for c in chains)
        try:
            fs_transformed = tuple(chain(*fs_transformed))
        except TypeError:
            pass

        def f_transformed(**params: Mapping):
            mapping = map_spec_transformer(**params)
            ret = tuple(
                fs_transformed[i](
                    **{
                        **params,
                        **{
                            k: mapping[k][i]
                            if len(mapping[k]) > 1
                            else mapping[k][0]
                            for k in mapping
                        },
                    }
                )
                for i in range(len(fs_transformed))
            )
            return _seq_to_dict(ret, merge_type=merge_type)

        return f_transformed
    return transform


def imapping_composition(
    transform: callable,
    map_spec: Optional[Sequence[str]] = None,
    inner_mapping: Optional[Mapping[str, Sequence]] = None,
    outer_mapping: Optional[Mapping[str, Sequence]] = None,
    n_replicates: Optional[int] = None,
) -> callable:
    mapping_compositor = close_imapping_compositor(
        map_spec=map_spec,
        inner_mapping=inner_mapping,
        outer_mapping=outer_mapping,
        n_replicates=n_replicates,
    )
    def transform_(
        f: callable,
        compositor: Optional[callable] = None,
    ) -> callable:
        # We override any compositor passed to the transform function
        # with the mapping compositor.
        return transform(f, compositor=mapping_compositor)
    return transform_


def omapping_composition(
    transform: callable,
    map_spec: Optional[Sequence[str]] = None,
    mapping: Optional[Mapping[str, Sequence]] = None,
    n_replicates: Optional[int] = None,
) -> callable:
    mapping_compositor = close_omapping_compositor(
        map_spec=map_spec,
        mapping=mapping,
        n_replicates=n_replicates,
    )
    def transform_(
        f: callable,
        compositor: Optional[callable] = None,
    ) -> callable:
        # We override any compositor passed to the transform function
        # with the mapping compositor.
        return transform(f, compositor=mapping_compositor)
    return transform_


def imap(
    transform: Optional[callable] = None,
    *,
    mapping: Optional[Mapping[str, Sequence]] = None,
    map_spec: Optional[Sequence[str]] = None,
    n_replicates: Optional[int] = None,
) -> callable:
    transform = transform or inject_params()
    mapping = mapping or {}
    map_spec = map_spec or tuple(mapping.keys())
    return imapping_composition(
        transform=transform,
        outer_mapping=mapping,
        map_spec=map_spec,
        n_replicates=n_replicates,
    )


def omap(
    transform: Optional[callable] = None,
    *,
    mapping: Optional[Mapping[str, Sequence]] = None,
    map_spec: Optional[Sequence[str]] = None,
    n_replicates: Optional[int] = None,
) -> callable:
    transform = transform or inject_params()
    mapping = mapping or {}
    map_spec = map_spec or tuple(mapping.keys())
    return omapping_composition(
        transform=transform,
        mapping=mapping,
        map_spec=map_spec,
        n_replicates=n_replicates,
    )


def join(
    joining_f: callable,
    join_vars: Optional[Sequence[str]] = None,
) -> callable:
    def split_chain(*chains: Sequence[callable]) -> callable:
        def transform(
            f: callable,
            compositor: Optional[callable] = None,
        ) -> callable:
            fs = [
                chain(f, compositor=delayed_outer_compositor)
                for chain in chains
            ]

            def join_fs(**params):
                out = [f(**params) for f in fs]
                out = tuple(zip(*out))
                f_outer = out[1][0]
                f_outer_params = out[2][0]
                out = _seq_to_dict(out[0], merge_type='union')
                jvars = join_vars or tuple(out.keys())

                for k, v in out.items():
                    if k not in jvars:
                        out[k] = v[0]
                        continue
                    out[k] = joining_f(v)
                return f_outer(**{**f_outer_params, **out})

            return join_fs
        return transform
    return split_chain


def joindata(
    join_vars: Optional[Sequence[str]] = None,
    how: Literal['outer', 'inner'] = 'outer',
    fill_value: Any = None,
) -> callable:
    def joining_f(arg):
        out = arg[0].join(arg[1:], how=how)
        if fill_value is not None:
            out = out.fillna(fill_value)
        return out

    return join(joining_f, join_vars)
