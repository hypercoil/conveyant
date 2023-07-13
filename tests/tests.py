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
    direct_compositor,
    null_transform,
    # null_op,
    # null_stage,
    imapping_composition,
    omapping_composition,
    imap,
    omap,
    join,
    replicate,
    inject_params,
    PipelineArgument as A,
    PipelineStage as S,
    SanitisedFunctionWrapper as F,
    SanitisedPartialApplication as P,
    Primitive,
)


def oper(name, w, x, y, z):
    # print(f'{name}: {w}, {x}, {y}, {z}')
    # print({name: (2 * w - x * z) / y})
    return {name: (2 * w - x * z) / y}


def increment_args(incr):
    def transform(f, compositor=direct_compositor):
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
            return compositor(f, transformer_f)(**other_params)(**numeric_params)

        return f_transformed
    return transform


def negate_args():
    def transform(f, compositor=direct_compositor):
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
            return compositor(f, transformer_f)(**other_params)(**numeric_params)

        return f_transformed
    return transform


def name_output(name):
    def transform(f, compositor=direct_compositor):
        def transformer_f():
            return {'name': name}

        def f_transformed(**params):
            return compositor(f, transformer_f)(**params)()
        return f_transformed
    return transform


def rename_output(old_name, new_name):
    def transform(f, compositor=direct_compositor):
        def transformer_f(**params):
            return {new_name: params.pop(old_name), **params}

        def f_transformed(**params):
            return compositor(transformer_f, f)()(**params)
        return f_transformed
    return transform


def increment_output(incr):
    def transform(f, compositor=direct_compositor):
        def transformer_f(**params):
            return {k: v + incr for k, v in params.items()}

        def f_transformed(**params):
            return compositor(transformer_f, f)()(**params)
        return f_transformed
    return transform


def increment_output_unhoisted():
    def transform(f, compositor=direct_compositor):
        def transformer_f(**params):
            print(params)
            incr = params.pop('incr')
            return {k: v + incr for k, v in params.items()}

        def f_transformed(**params):
            return compositor(transformer_f, f)()(**params)
        return f_transformed
    return transform


def intermediate_oper(vars):
    def transform(f, compositor=direct_compositor):
        def transformer_f(**params):
            return {
                k: [v, 2 * v, 4 * v]
                for k, v in params.items()
                if k in vars
            }

        def f_transformed(**params):
            inner_params = {k: v for k, v in params.items() if k in vars}
            outer_params = {k: v for k, v in params.items() if k not in vars}
            return compositor(f, transformer_f)(**outer_params)(**inner_params)
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


def test_direct_compositor():
    w, x, y, z = 1, 2, 3, 4
    name = 'test'
    out = oper(name=name, w=w, x=x, y=y, z=z)
    assert out[name] == -2

    transformed_oper = increment_args(incr=1)(
        oper, compositor=direct_compositor)
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


def test_omapping_compositor():
    w, x, y, z = 1, 2, 3, 4
    ref = [oper(name='test', w=w, x=x, y=y, z=z) for w, x, y, z in zip(
        [1, 2, 4, 1, 2, 4, 1, 2, 4],
        [2, 2, 2, 4, 4, 4, 8, 8, 8],
        [3, 3, 3, 6, 6, 6, 12, 12, 12],
        [4, 8, 16, 4, 8, 16, 4, 8, 16],
    )]

    i_chain = ichain(
        name_output('test'),
        omapping_composition(
            intermediate_oper(['x', 'y']),
            map_spec=('x', 'y'),
        ),
        omapping_composition(
            intermediate_oper(['w', 'z']),
            map_spec=('w', 'z'),
        ),
    )
    o_chain = ochain(
        omapping_composition(
            increment_output(2),
            map_spec='test',
        ),
    )
    io_chain = iochain(
        oper,
        i_chain,
        o_chain,
    )
    out = io_chain(w=w, x=x, y=y, z=z)
    ref0 = {'test': tuple(r['test'] + 2 for r in ref)}
    assert out == ref0

    i_chain = ichain(
        omapping_composition(
            intermediate_oper(['x', 'y']),
            mapping={'name': ['test1', 'test2', 'test3']},
            map_spec=('x', 'y'),
        ),
        omapping_composition(
            intermediate_oper(['w', 'z']),
            map_spec=('w', 'z'),
        ),
    )
    o_chain = ochain(
        omapping_composition(
            increment_output(2),
            map_spec=['test1', 'test2', 'test3'],
        ),
    )
    io_chain = iochain(
        oper,
        i_chain,
        o_chain,
    )
    out = io_chain(w=w, x=x, y=y, z=z)
    ref1 = {
        'test1': tuple(r['test'] + 2 for i, r in enumerate(ref) if i // 3 == 0),
        'test2': tuple(r['test'] + 2 for i, r in enumerate(ref) if i // 3 == 1),
        'test3': tuple(r['test'] + 2 for i, r in enumerate(ref) if i // 3 == 2),
    }
    assert out == ref1

    i_chain = ichain(
        omapping_composition(
            intermediate_oper(['x', 'y']),
            map_spec=('x', 'y'),
        ),
        omapping_composition(
            intermediate_oper(['w', 'z']),
            mapping={'name': ['test1', 'test2', 'test3']},
            map_spec=('w', 'z'),
        ),
    )
    o_chain = ochain(
        omapping_composition(
            increment_output(2),
            map_spec=['test1', 'test2', 'test3'],
        ),
    )
    io_chain = iochain(
        oper,
        i_chain,
        o_chain,
    )
    out = io_chain(w=w, x=x, y=y, z=z)
    ref1 = {
        'test1': tuple(r['test'] + 2 for i, r in enumerate(ref) if i % 3 == 0),
        'test2': tuple(r['test'] + 2 for i, r in enumerate(ref) if i % 3 == 1),
        'test3': tuple(r['test'] + 2 for i, r in enumerate(ref) if i % 3 == 2),
    }
    assert out == ref1


def test_imapping_compositor():
    w, x, y, z = 1, 2, 3, 4
    ref = [oper(name='test', w=wi, x=x, y=y, z=z) for wi in [1, 2, 3, 4]]
    ref = {'test': tuple(r['test'] + 2 for r in ref)}
    i_chain = ichain(
        name_output('test'),
        imapping_composition(
            inject_params(),
            outer_mapping={'w': [1, 2, 3, 4]},
            map_spec='w',
        ),
    )
    o_chain = ochain(
        omapping_composition(
            increment_output(2),
            map_spec='test',
        ),
    )
    io_chain = iochain(
        oper,
        i_chain,
        o_chain,
    )
    out = io_chain(x=x, y=y, z=z)
    assert out == ref

    ref = oper(name='test', w=w, x=x, y=y, z=z)
    ref = {'test': tuple(ref['test'] + i for i in [2, 3, 4, 5])}
    i_chain = ichain(
        name_output('test'),
    )
    o_chain = ochain(
        imapping_composition(
            increment_output_unhoisted(),
            outer_mapping=({'incr': (2, 3, 4, 5)}),
            map_spec='incr',
        ),
    )
    io_chain = iochain(
        oper,
        i_chain,
        o_chain,
    )
    out = io_chain(w=w, x=x, y=y, z=z)
    assert out == ref


def test_imap_omap_convenience():
    x, y, z = 2, 3, 4
    ref = [oper(name='test', w=wi, x=x, y=y, z=z) for wi in [1, 2, 3, 4]]
    ref = {'test': tuple(r['test'] + 2 for r in ref)}
    i_chain = ichain(
        name_output('test'),
        imap(
            mapping={'w': [1, 2, 3, 4]},
        ),
    )
    o_chain = ochain(
        omap(
            increment_output(2),
            map_spec='test',
        ),
    )
    io_chain = iochain(
        oper,
        i_chain,
        o_chain,
    )
    out = io_chain(x=x, y=y, z=z)
    assert out == ref


def test_join():
    w, x, y, z = 1, 2, 3, 4
    wr, xr, yr, zr = sum([1, 2, 4]), sum([2, 4, 8]), sum([3, 6, 12]), sum([4, 8, 16])
    ref = oper(name='test', w=wr, x=xr, y=yr, z=zr)
    ref['test'] += 2

    i_chain = ichain(
        name_output('test'),
        join(joining_f=sum, join_vars=('w', 'x', 'y', 'z'))(
            intermediate_oper(['x', 'y']),
            intermediate_oper(['w', 'z']),
        ),
    )
    o_chain = ochain(
        join(joining_f=sum, join_vars=('test'))(
            ochain(
                omapping_composition(
                    increment_output(2),
                    map_spec='test',
                ),
                inject_params(),
            ),
        ),
    )
    o_chain = increment_output(2)
    io_chain = iochain(
        oper,
        i_chain,
        o_chain,
    )
    out = io_chain(w=w, x=x, y=y, z=z)
    assert out == ref


def test_sanitisers():
    fn = F(oper)
    assert repr(fn) == "oper"
    assert fn('test', 1, 2, 3, 4) == oper('test', 1, 2, 3, 4)

    ptl = P(oper, 'test', w=1, x=2)
    assert repr(ptl) == "oper(test, w=1, x=2)"
    ptl = P(oper, name='test', w=1, x=2)
    assert repr(ptl) == "oper(name=test, w=1, x=2)"
    ptl = P(oper, 'test', 1, 2)
    assert repr(ptl) == "oper(test, 1, 2)"
    assert ptl(y=3, z=4) == oper(name='test', w=1, x=2, y=3, z=4)

    w, x, y, z = 1, 2, 3, 4
    i_chain = ichain(
        increment_args(incr=1),
        name_output('test'),
    )
    o_chain = ochain(
        rename_output('test', 'test2'),
    )
    io_chain = iochain(oper, i_chain, o_chain)
    ref = io_chain(w=w, x=x, y=y, z=z)

    i_chain = ichain(
        S(increment_args, A(incr=1)),
        S(name_output, A('test')),
    )
    o_chain = ochain(
        S(rename_output, A('test', 'test2')),
    )
    io_chain = iochain(oper, i_chain, o_chain)
    out = io_chain(w=w, x=x, y=y, z=z)
    assert out == ref


def test_primitive():
    oper_p = Primitive(
        oper,
        name='oper',
        output=('output',),
        forward_unused=True,
    )
    assert repr(oper_p) == "Primitive(oper)"
    assert (
        oper_p(name='test', w=1, x=2, y=3, z=4) ==
        {'output': oper('test', 1, 2, 3, 4)}
    )
    assert (
        oper_p(name='test', v=0, w=1, x=2, y=3, z=4) ==
        {'output': oper('test', 1, 2, 3, 4), 'v': 0}
    )
    with pytest.raises(TypeError):
        oper_p('test', 1, 2, 3, 4)

    oper_p = Primitive(
        oper,
        name='oper',
        output=('output',),
        forward_unused=False,
    )
    assert (
        oper_p(name='test', v=0, w=1, x=2, y=3, z=4) ==
        {'output': oper('test', 1, 2, 3, 4)}
    )
