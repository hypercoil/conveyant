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

from .replicate import replicate


def _seq_to_dict(
    seq: Sequence[Mapping],
    #chain_vars: Sequence[str] = ("plotter",),
    merge_type: Optional[Literal["union", "intersection"]] = None,
) -> Mapping[str, Sequence]:
    if merge_type is None:
        keys = seq[0].keys()
    else:
        keys = [set(s.keys()) for s in seq]
        if merge_type == "union":
            keys = set.union(*keys)
        elif merge_type == "intersection":
            keys = set.intersection(*keys)
    if merge_type == "union":
        NULLSTR = "__ignore__"
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
    # for k in chain_vars:
    #     if k not in dct:
    #         continue
    #     try:
    #         dct[k] = tuple(chain(*dct[k]))
    #     except TypeError:
    #         pass
    return dct


def _dict_to_seq(
    dct: Mapping[str, Sequence],
) -> Sequence[Mapping]:
    keys = dct.keys()
    seq = tuple(
        dict(zip(dct.keys(), v))
        for v in zip(*dct.values())
    )
    return seq


def direct_compositor(
    f_outer: callable,
    f_inner: callable,
    unpack_dict: bool = True,
) -> callable:
    def transformed_f_outer(**f_outer_params):
        def transformed_f_inner(**f_inner_params):
            if unpack_dict:
                return f_outer(
                    **{**f_outer_params, **f_inner(**f_inner_params)}
                )
            return f_outer(f_inner(**f_inner_params), **f_outer_params)
        return transformed_f_inner
    return transformed_f_outer


def null_transform(
    f: callable,
    compositor: callable = direct_compositor,
) -> callable:
    return f


def split_chain(
    *chains: Sequence[callable],
    map_spec: Optional[Sequence[str]] = None,
    weave_type: Literal['maximal', 'minimal', 'strict'] = 'maximal',
    maximum_aggregation_depth: Optional[int] = None,
    broadcast_out_of_spec: bool = False,
    merge_type: Optional[Literal["union", "intersection"]] = "union",
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
        compositor: callable = direct_compositor
    ) -> callable:
        fs_transformed = tuple(c(f, compositor=compositor) for c in chains)
        try:
            fs_transformed = tuple(chain(*fs_transformed))
        except TypeError:
            pass

        def f_transformed(**params: Mapping):
            mapping = map_spec_transformer(**params)
            ret = tuple(
                fs_transformed[i](**{
                    **params, **{**params, **{
                        k: mapping[k][i]
                        if len(mapping[k]) > 1 else mapping[k][0]
                        for k in mapping
                    }}
                })
                for i in range(len(fs_transformed))
            )
            return _seq_to_dict(ret, merge_type=merge_type)

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


def joindata(
    join_vars: Optional[Sequence[str]] = None,
    how: Literal["outer", "inner"] = "outer",
    fill_value: Any = None,
) -> callable:
    def joining_f(arg):
        out = arg[0].join(arg[1:], how=how)
        if fill_value is not None:
            out = out.fillna(fill_value)
        return out

    return join(joining_f, join_vars)


# def map_over_sequence(
#     xfm: callable,
#     mapping: Optional[Mapping[str, Sequence]] = None,
#     n_replicates: Optional[int] = None,
# ) -> callable:
#     mapping_transform = close_mapping_transform(
#         mapping=mapping,
#         n_replicates=n_replicates,
#     )
#     def transform(f: callable) -> callable:
#         return xfm(f, mapping_transform)
#     return transform


# def replicate_and_map(
#     xfm: callable,
#     mapping: Mapping[str, Sequence],
#     default_params: Literal["inner", "outer"] = "inner",
# ) -> callable:
#     mapping_transform = close_replicating_transform(
#         mapping,
#         default_params=default_params,
#     )
#     def transform(f: callable) -> callable:
#         return xfm(f, mapping_transform)
#     return transform


# def replicate(
#     mapping: Mapping[str, Sequence] = {},
#     map_over: Sequence[str] = (),
#     additional_params: Optional[Mapping[str, Any]] = {"copy_actors": True},
# ) -> callable:
#     if mapping:
#         n_vals = len(next(iter(mapping.values())))
#         for vals in mapping.values():
#             assert len(vals) == n_vals, (
#                 "All values must have the same length. Perhaps you intended to "
#                 "nest replications?"
#             )
#     def transform(f: callable) -> callable:
#         def f_transformed(**params: Mapping):
#             _additional_params = additional_params or {}
#             if map_over is not None:
#                 #TODO: assert equal lengths
#                 n_vals = len(params[map_over[0]])
#             mapped_params = {k: v for k, v in params.items() if k in map_over}
#             other_params = {k: v for k, v in params.items() if k not in map_over}
#             mapped_params = {**mapped_params, **mapping}
#             ret = []
#             for i in range(n_vals):
#                 nxt = f(**{
#                     **other_params,
#                     **{k: mapped_params[k][i] for k in mapped_params},
#                     **_additional_params,
#                 })
#                 ret += [nxt]
#             return _seq_to_dict(ret)

#         return f_transformed
#     return transform


def close_replicating_transform(
    mapping: Mapping,
    default_params: Literal["inner", "outer"] = "inner",
) -> callable:
    n_replicates = len(next(iter(mapping.values())))
    for v in mapping.values():
        assert len(v) == n_replicates, (
            "All mapped values must have the same length")
    def replicating_transform(
        f_outer: callable,
        f_inner: callable,
        unpack_dict: bool = False,
    ) -> callable:
        def transformed_f_outer(**f_outer_params):
            def transformed_f_inner(**f_inner_params):
                ret = []
                for i in range(n_replicates):
                    if default_params == "inner":
                        mapped_params_inner = {
                            k: v[i] for k, v in mapping.items()
                            if k not in f_outer_params
                        }
                        mapped_params_outer = {
                            k: v[i] for k, v in mapping.items()
                            if k in f_outer_params
                        }
                    elif default_params == "outer":
                        mapped_params_inner = {
                            k: v[i] for k, v in mapping.items()
                            if k in f_inner_params
                        }
                        mapped_params_outer = {
                            k: v[i] for k, v in mapping.items()
                            if k not in f_inner_params
                        }
                    f_inner_params_i = {
                        **f_inner_params,
                        **mapped_params_inner
                    }
                    f_outer_params_i = {
                        **f_outer_params,
                        **mapped_params_outer
                    }
                    if unpack_dict:
                        ret.append(
                            f_outer(**{**f_inner(**f_inner_params_i), **f_outer_params_i})
                        )
                    else:
                        ret.append(
                            f_outer(f_inner(**f_inner_params_i)[i], **f_outer_params_i)
                        )
                return _seq_to_dict(ret)
            return transformed_f_inner
        return transformed_f_outer
    return replicating_transform


def close_mapping_transform(
    mapping: Optional[Mapping] = None,
    n_replicates: Optional[int] = None,
) -> callable:
    if n_replicates is None:
        n_replicates = len(next(iter(mapping.values())))
    if mapping is None:
        mapping = {}
    for v in mapping.values():
        assert len(v) == n_replicates, (
            "All mapped values must have the same length")
    def mapping_transform(
        f_outer: callable,
        f_inner: callable,
        unpack_dict: bool = True,
    ) -> callable:
        def transformed_f_outer(**f_outer_params):
            def transformed_f_inner(**f_inner_params):
                ret = []
                out = f_inner(**f_inner_params)
                out = _dict_to_seq(out)
                assert len(out) == n_replicates, (
                    "The length of the output of the inner function must be "
                    "equal to the length of the mapped values")
                for i, o in enumerate(out):
                    if mapping:
                        mapped_params = {k: v[i] for k, v in mapping.items()}
                    if unpack_dict:
                        f_outer_params_i = {
                            **f_outer_params,
                            **mapped_params,
                            **o,
                        }
                        ret.append(f_outer(**f_outer_params_i))
                    else:
                        f_outer_params_i = {
                            **f_outer_params,
                            **mapped_params,
                        }
                        ret.append(f_outer(o, **f_outer_params_i))
                return _seq_to_dict(ret)
            return transformed_f_inner
        return transformed_f_outer
    return mapping_transform


def delayed_outer_transform(
    f_outer: callable,
    f_inner: callable,
    unpack_dict: Any = None,
) -> callable:
    def transformed_f_outer(**f_outer_params):
        def transformed_f_inner(**f_inner_params):
            out = f_inner(**f_inner_params)
            return out, f_outer, f_outer_params
        return transformed_f_inner
    return transformed_f_outer


def join(
    joining_f: callable,
    join_vars: Optional[Sequence[str]] = None,
    unpack_dict: bool = True,
) -> callable:
    def split_chain(*chains: Sequence[callable]) -> callable:
        def transform(f: callable) -> callable:
            fs = [chain(f, delayed_outer_transform) for chain in chains]

            def join_fs(**params):
                out = [f(**params) for f in fs]
                out = tuple(zip(*out))
                f_outer = out[1][0]
                f_outer_params = out[2][0]
                out = _seq_to_dict(out[0], merge_type="union")
                jvars = join_vars or tuple(out.keys())

                for k, v in out.items():
                    if k not in jvars:
                        out[k] = v[0]
                        continue
                    out[k] = joining_f(v)
                if unpack_dict:
                    return f_outer(**{**f_outer_params, **out})
                return f_outer(out, **f_outer_params)

            return join_fs
        return transform
    return split_chain
