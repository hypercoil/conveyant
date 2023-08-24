# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Unit tests
"""
import inspect, pytest


from conveyant import (
    ichain,
    ochain,
    iochain,
    split_chain,
    emulate_assignment,
    splice_on,
    direct_compositor,
    reversed_args_compositor,
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
    FunctionWrapper as F,
    PartialApplication as P,
    Primitive,
    Composition,
)


class UnknownCallable:
    def __init__(self, f):
        self.f = f
    def __call__(self, *pparams, **params):
        return self.f(*pparams, **params)


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


def increment_output_p(incr, **params):
    return {k: v + incr for k, v in params.items()}


def increment_output(incr):
    def transform(f, compositor=direct_compositor):
        transformer_f = P(increment_output_p, incr=incr)

        def f_transformed(**params):
            return compositor(transformer_f, f)()(**params)
        return f_transformed
    return transform


def increment_output_unhoisted():
    def transform(f, compositor=direct_compositor):
        def transformer_f(**params):
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


def consume_all(all):
    pass


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
    assert params_out['c'] == [[7, 8, 9]]
    assert params_out['d'] == [[10, 11, 12]]

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
        'a': [1, 2, 3],
        'b': [4, 5, 6],
        'c': (),
        'd': [10, 11, 12],
    }
    spec = ['a', ('b', 'c')]
    transformer = replicate(
        spec=spec,
        weave_type='maximal',
        maximum_aggregation_depth=None,
        broadcast_out_of_spec=True,
    )
    params_out = transformer(**params)
    assert params_out['a'] == [1, 1, 1, 2, 2, 2, 3, 3, 3]
    assert params_out['b'] == [4, 5, 6, 4, 5, 6, 4, 5, 6]
    assert params_out['c'] == [(), (), (), (), (), (), (), (), ()]

    transformer = replicate(
        spec=spec,
        weave_type='maximal',
        maximum_aggregation_depth=None,
        broadcast_out_of_spec=False,
    )
    params_out = transformer(**params)
    assert params_out['a'] == [1, 1, 1, 2, 2, 2, 3, 3, 3]
    assert params_out['b'] == [4, 5, 6, 4, 5, 6, 4, 5, 6]
    assert params_out['c'] == [()]

    # params = {
    #     'a': [1, 2, 3],
    #     'b': (4, 5, 6),
    #     'c': (7, 8, 9),
    #     'd': [10, 11, 12],
    # }
    # spec = ('a', 'b', 'c', 'd')
    # SETTINGS.set_aggregator_types(list)
    # transformer = replicate(
    #     spec=spec,
    #     weave_type='maximal',
    #     maximum_aggregation_depth=None,
    #     broadcast_out_of_spec=True,
    # )
    # params_out = transformer(**params)
    # assert params_out['a'] == [1, 2, 3]
    # assert params_out['b'] == [(4, 5, 6), (4, 5, 6), (4, 5, 6)]
    # assert params_out['c'] == [(7, 8, 9), (7, 8, 9), (7, 8, 9)]
    # assert params_out['d'] == [10, 11, 12]
    # SETTINGS.set_aggregator_types(list, tuple)

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
    fn2 = fn.bind(w=1, x=2)
    assert isinstance(fn2, F)
    assert fn2 is fn

    fn = F(oper, __allowed__=('w', 'x', 'y', 'z'))
    ptl = fn.bind(w=1, x=2)
    assert isinstance(ptl, P)
    assert repr(ptl) == "oper(w=1, x=2)"

    ptl = P(oper, 'test', w=1, x=2, __allowed__=('y', 'z'))
    assert repr(ptl) == "oper(test, w=1, x=2)"
    ptl = ptl.bind(y=3, z=4)
    assert repr(ptl) == "oper(test, w=1, x=2, y=3, z=4)"
    assert ptl() == oper(name='test', w=1, x=2, y=3, z=4)

    ptl = P(oper, 'test', w=1, x=2, __allowed__=('y', 'z'))
    ptl = ptl.set_priority('ice')
    ptl = ptl.bind(y=3, z=4)
    assert repr(ptl) == "oper(test, y=3, z=4, w=1, x=2)"
    assert ptl() == oper(name='test', w=1, x=2, y=3, z=4)

    ptl = P(oper, name='test', w=1, x=2)
    assert repr(ptl) == "oper(name=test, w=1, x=2)"
    ptl = P(oper, 'test', 1, 2)
    assert repr(ptl) == "oper(test, 1, 2)"
    assert ptl(y=3, z=4) == oper(name='test', w=1, x=2, y=3, z=4)

    ptl = P(oper, name='test', w=1, x=2, __conditions__={
        ('w', 1): [('y', 3), ('x', 5)],
        ('x', 2): [('z', 4)],
    })
    assert repr(ptl) == "oper(name=test, w=1, x=2)"
    assert ptl() == oper(name='test', w=1, x=5, y=3, z=4)
    ptl = ptl.set_priority('eic')
    assert ptl() == oper(name='test', w=1, x=2, y=3, z=4)

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

    fn = F(UnknownCallable(oper))
    ptl = P(UnknownCallable(oper), 'test', w=1, x=2)
    assert repr(fn) == 'wrapped UnknownCallable'
    assert repr(ptl) == '(wrapped UnknownCallable)(test, w=1, x=2)'


def test_primitive():
    oper_p = Primitive(
        oper,
        name='oper',
        output=('output',),
        forward_unused=True,
    )
    fn = F(oper_p)
    ptl = P(oper_p, name='test', w=1, x=2)
    assert repr(oper_p) == "Primitive(oper)"
    assert repr(fn) == "Primitive(oper)"
    assert repr(ptl) == "Primitive(oper)(name=test, w=1, x=2)"
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

    def null2(x, y):
        return x, y
    null2_p = Primitive(
        null2,
        name='null2',
        output=('x', 'y'),
        forward_unused=True,
    )
    assert null2_p(x=1, y=2) == {'x': 1, 'y': 2}

    oper_p = Primitive(
        oper,
        name='oper',
        output=None,
        forward_unused=False,
    )
    assert (
        oper_p(name='test', v=0, w=1, x=2, y=3, z=4) ==
        oper('test', 1, 2, 3, 4)
    )

    null2_p = Primitive(
        null2,
        name='null2',
        output=None,
        forward_unused=True,
        splice_on_call=True,
    )
    with pytest.raises(TypeError):
        null2_p(x=1, y=2)

    consume_p = Primitive(
        consume_all,
        name='consume_all',
        output=(),
        forward_unused=True,
        splice_on_call=False,
    )
    assert consume_p(all=0) == {}


def test_composition():
    c = Composition(
        compositor=direct_compositor,
        outer=P(increment_output_p, incr=1),
        inner=oper,
    ).bind_curried()
    assert c.curried_fn == 'outer'
    assert c(name='test', w=1, x=2, y=3, z=4) == {'test': -1}
    c2 = c.bind(w=1, x=2, y=3, z=4)
    assert isinstance(c2.outer, P)

    c = Composition(
        compositor=reversed_args_compositor,
        outer=P(increment_output_p, incr=1),
        inner=oper,
    )
    c = c.bind_curried(name='test', w=1, x=2, y=3, z=4)
    assert c.curried_fn == 'inner'
    assert c() == {'test': -1}
    c2 = c.bind(a=1, b=2, c=3, d=4) # no effect
    assert c2 == c

    def add_args(a, b):
        return {'e': a + b}

    def mul_args(c, d):
        return {'b': c * d}

    # TODO: Oh, no -- we cannot use ``f`` as a variable name! This is because
    #       ``f`` is a reserved name in several container classes. We need to
    #       fix this by using reserved names that are less likely to be used
    #       as variable names -- like underscore-prefixed names
    def div_args(e, g):
        return {'h': e / g}

    c0 = Composition(
        compositor=direct_compositor,
        outer=add_args,
        inner=mul_args,
        __allowed__=('c', 'd'),
    ).bind_curried(a=2)
    assert c0.curried_fn == 'outer'
    assert c0(c=1, d=1)['e'] == 3
    assert c0.bind(c=1)(d=1)['e'] == 3

    c0 = Composition(
        compositor=reversed_args_compositor,
        outer=add_args,
        inner=mul_args,
        __allowed__=('a',),
    ).bind_curried(c=1, d=1)
    assert c0.curried_fn == 'inner'
    assert c0(a=2)['e'] == 3
    assert c0.bind(a=2)()['e'] == 3

    c1 = Composition(
        compositor=direct_compositor,
        outer=div_args,
        inner=c0,
    ).bind_curried(g=2)
    assert c1.curried_fn == 'outer'
    assert c1(a=2)['h'] == 1.5
    assert c1.bind()(a=2)['h'] == 1.5

    c1 = Composition(
        compositor=reversed_args_compositor,
        outer=div_args,
        inner=c0,
        __allowed__=('g',),
    ).bind_curried(a=2)
    c1.set_priority('ice')
    assert c1.curried_fn == 'inner'
    assert c1(g=2)['h'] == 1.5
    assert c1.bind(g=2)()['h'] == 1.5


def test_emulation():
    def indef_oper(**params):
        return oper(**params)

    indef_oper.__signature__ = inspect.signature(oper)
    indef_oper_w = emulate_assignment(strict=True)(indef_oper)
    assert (
        indef_oper_w(name='test', w=1, x=2, y=3, z=4) ==
        oper('test', 1, 2, 3, 4)
    )

    with pytest.raises(TypeError):
        indef_oper_w(name='test', v=0, w=1, x=2, y=3, z=4)

    def oper_with_defaults(name, w=1, x=2, y=3, z=4):
        return oper(name, w, x, y, z)

    indef_oper.__signature__ = inspect.signature(oper_with_defaults)
    indef_oper_w = emulate_assignment(strict=True)(indef_oper)
    assert (
        indef_oper_w(name='test', y=3) == oper('test', 1, 2, 3, 4)
    )

    @splice_on(oper, occlusion=('w', 'x'))
    def set_w_and_x(name, val=1, **params):
        w = x = val
        return oper(name, w, x, **params)

    assert set_w_and_x(name='test', y=2, z=3) == oper('test', 1, 1, 2, 3)
    assert (
        set(p for p in set_w_and_x.__signature__.parameters) ==
        {'name', 'val', 'y', 'z'}
    )

    with pytest.raises(TypeError):
        set_w_and_x(name='dog')

    def indef_oper(**params):
        return oper(**params)

    @splice_on(indef_oper, occlusion=('w', 'x'), allow_variadic=True)
    def set_w_and_x(name, val=1, **params):
        w = x = val
        return indef_oper(name=name, w=w, x=x, **params)

    assert set_w_and_x(name='test', y=2, z=3) == oper('test', 1, 1, 2, 3)
    assert (
        set(p for p in set_w_and_x.__signature__.parameters) ==
        {'name', 'val', 'params'}
    )
