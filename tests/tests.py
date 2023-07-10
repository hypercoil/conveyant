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
