# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Unit tests
"""
import pytest


from conveyant import (
    ichain,
    ochain,
    iochain,
    split_chain,
    joindata,
    direct_transform,
    null_transform,
    close_mapping_transform,
    join,
    replicate,
)


def oper(name, w, x, y, z):
    # print(f'{name}: {w}, {x}, {y}, {z}')
    # print({name: (2 * w - x * z) / y})
    return {name: (2 * w - x * z) / y}


def increment_args(incr):
    def transform(f, xfm=direct_transform):
        def transformer_f(**numeric_params):
            return {k: v + incr for k, v in numeric_params.items()}

        def f_transformed(**params):
            numeric_params = {
                k: v for k, v in params.items()
                if isinstance(v, (int, float))
            }
            other_params = {
                k: v for k, v in params.items()
                if k not in numeric_params
            }
            return xfm(f, transformer_f)(**other_params)(**numeric_params)

        return f_transformed
    return transform


def negate_args():
    def transform(f, xfm=direct_transform):
        def transformer_f(**numeric_params):
            return {k: -v for k, v in numeric_params.items()}

        def f_transformed(**params):
            numeric_params = {
                k: v for k, v in params.items()
                if isinstance(v, (int, float))
            }
            other_params = {
                k: v for k, v in params.items()
                if k not in numeric_params
            }
            return xfm(f, transformer_f)(**other_params)(**numeric_params)

        return f_transformed
    return transform


def name_output(name):
    def transform(f, xfm=direct_transform):
        def transformer_f():
            return {'name': name}

        def f_transformed(**params):
            return xfm(f, transformer_f)(**params)()
        return f_transformed
    return transform


def rename_output(old_name, new_name):
    def transform(f, xfm=direct_transform):
        def transformer_f(**params):
            return {new_name: params.pop(old_name), **params}

        def f_transformed(**params):
            return xfm(transformer_f, f)()(**params)
        return f_transformed
    return transform


def test_replicate():
    params = {
        'a': [1, 2, 3],
        'b': [4, 5, 6],
        'c': [7, 8, 9],
        'd': [10, 11, 12],
    }
    spec = ['a', 'b']
    transformer = replicate(
        spec=spec,
        weave_type='maximal',
        maximum_aggregation_depth=None,
        broadcast_out_of_spec=False,
    )
    params_out = transformer(**params)
    assert params_out['a'] == [1, 1, 1, 2, 2, 2, 3, 3, 3]
    assert params_out['b'] == [4, 5, 6, 4, 5, 6, 4, 5, 6]
    assert params_out['c'] == [7, 8, 9]
    assert params_out['d'] == [10, 11, 12]

    transformer = replicate(
        spec=spec,
        weave_type='maximal',
        maximum_aggregation_depth=None,
        broadcast_out_of_spec=True,
    )
    params_out = transformer(**params)
    assert params_out['a'] == [1, 1, 1, 2, 2, 2, 3, 3, 3]
    assert params_out['b'] == [4, 5, 6, 4, 5, 6, 4, 5, 6]
    assert params_out['c'] == [7, 8, 9, 7, 8, 9, 7, 8, 9]
    assert params_out['d'] == [10, 11, 12, 10, 11, 12, 10, 11, 12]

    transformer = replicate(
        spec=spec,
        weave_type='maximal',
        maximum_aggregation_depth=None,
        broadcast_out_of_spec=True,
        n_replicates=12,
    )
    params_out = transformer(**params)
    assert params_out['a'] == [1, 1, 1, 2, 2, 2, 3, 3, 3, 1, 1, 1]
    assert params_out['b'] == [4, 5, 6, 4, 5, 6, 4, 5, 6, 4, 5, 6]
    assert params_out['c'] == [7, 8, 9, 7, 8, 9, 7, 8, 9, 7, 8, 9]
    assert params_out['d'] == [10, 11, 12, 10, 11, 12, 10, 11, 12, 10, 11, 12]

    params = {'a': [0, 1, 2], 'b': [3, 4], 'c': [2, 5]}
    spec = (['a', 'b'], 'c')
    transformer = replicate(
        spec=spec,
        weave_type='minimal',
        maximum_aggregation_depth=None,
        broadcast_out_of_spec=False,
    )
    params_out = transformer(**params)
    assert params_out['a'] == [0, 0]
    assert params_out['b'] == [3, 4]
    assert params_out['c'] == [2, 5]

    spec = ['a', 'b', 'c']
    transformer = replicate(
        spec=spec,
        weave_type='maximal',
        maximum_aggregation_depth=None,
        broadcast_out_of_spec=True,
    )
    params_out = transformer(**params)
    assert params_out['a'] == [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2]
    assert params_out['b'] == [3, 3, 4, 4, 3, 3, 4, 4, 3, 3, 4, 4]
    assert params_out['c'] == [2, 5, 2, 5, 2, 5, 2, 5, 2, 5, 2, 5]

    spec = ['a', ('b', 'c')]
    transformer = replicate(
        spec=spec,
        weave_type='strict',
        maximum_aggregation_depth=None,
        broadcast_out_of_spec=True,
    )
    params_out = transformer(**params)
    assert params_out['a'] == [0, 0, 1, 1, 2, 2]
    assert params_out['b'] == [3, 4, 3, 4, 3, 4]
    assert params_out['c'] == [2, 5, 2, 5, 2, 5]

    params = {
        'a': ['cat', 'dog'],
        'b': [0, 1, 2],
        'c': [3, 4, 3],
        'd': 'fish', 
        'e': ['whales', 'dolphins', 'porpoises'],
        'f': (99, 77, 55),
    }
    spec = ['a', ('b', 'c'), 'd', ('e', 'f')]
    transformer = replicate(
        spec=spec,
        weave_type='strict',
        maximum_aggregation_depth=None,
        broadcast_out_of_spec=True,
    )
    params_out = transformer(**params)
    for v in params_out.values():
        assert len(v) == 18

    transformer = replicate(
        spec=[],
        weave_type='strict',
        maximum_aggregation_depth=None,
        broadcast_out_of_spec=True,
    )
    params_out = transformer(**params)
    for v in params_out.values():
        assert len(v) == 3


def test_direct_transform():
    w, x, y, z = 1, 2, 3, 4
    name = 'test'
    out = oper(name=name, w=w, x=x, y=y, z=z)
    assert out[name] == -2

    transformed_oper = increment_args(incr=1)(oper, xfm=direct_transform)
    out = transformed_oper(name=name, w=w, x=x, y=y, z=z)
    assert out[name] == -11 / 4


def test_direct_chains():
    w, x, y, z = 1, 2, 3, 4
    i_chain = ichain(
        increment_args(incr=1),
        name_output('test'),
    )
    o_chain = ochain(
        rename_output('test', 'test2'),
    )
    io_chain = iochain(oper, i_chain, o_chain)
    out = io_chain(w=w, x=x, y=y, z=z)
    assert out['test2'] == -11 / 4


def test_splitting_chains():
    # wp, xp, yp, zp = 1, 2, 3, 4
    # wn, xn, yn, zn = -1, -2, -3, -4
    w, x, y, z = 1, 2, 3, 4
    i_chain = ichain(
        split_chain(
            ichain(
                increment_args(incr=1),
                name_output('test'),
            ),
            ichain(
                negate_args(),
                name_output('testn'),
            ),
        )
    )
    o_chain = ochain(
        rename_output('test', 'test2'),
        rename_output('testn', 'testn2'),
    )
    io_chain = iochain(oper, i_chain, o_chain)
    out = io_chain(w=w, x=x, y=y, z=z)
    assert out['test2'][0] == -11 / 4
    assert out['testn2'][0] == 10 / 3

    i_chain = ichain(
        name_output('test'),
        split_chain(
            ichain(
                increment_args(incr=1),
            ),
            ichain(
                negate_args(),
            ),
        )
    )
    o_chain = ochain(
        rename_output('test', 'test2'),
    )
    io_chain = iochain(oper, i_chain, o_chain)
    out = io_chain(w=w, x=x, y=y, z=z)
    assert out['test2'][0] == -11 / 4
    assert out['test2'][1] == 10 / 3

    w, x, y, z = [1, -1], [2, -2], [3, -3], [4, -4]
    i_chain = ichain(
        name_output('test'),
        split_chain(
            ichain(
                increment_args(incr=1),
            ),
            null_transform,
            map_spec=('w', 'x', 'y', 'z'),
        )
    )
    io_chain = iochain(oper, i_chain, o_chain)
    out = io_chain(w=w, x=x, y=y, z=z)
    assert out['test2'][0] == -11 / 4
    assert out['test2'][1] == 10 / 3


def test_replicating_transform():
    pass
