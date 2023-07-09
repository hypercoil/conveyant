# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Miscellaneous utility functions for tensor axis manipulation.
"""
from typing import Tuple, Union


def axis_complement(
    ndim: int,
    axis: Union[int, Tuple[int, ...]],
) -> Tuple[int, ...]:
    """
    Return the complement of the axis or axes for a tensor of dimension ndim.
    """
    if isinstance(axis, int):
        axis = (axis,)
    ax = [True for _ in range(ndim)]
    for a in axis:
        ax[a] = False
    ax = [i for i, a in enumerate(ax) if a]
    return tuple(ax)


def standard_axis_number(axis: int, ndim: int) -> int:
    """
    Convert an axis number to a standard axis number.
    """
    if axis < 0 and axis >= -ndim:
        axis += ndim
    elif axis < -ndim or axis >= ndim:
        return None
    return axis


def promote_axis(
    ndim: int,
    axis: Union[int, Tuple[int, ...]],
) -> Tuple[int, ...]:
    """
    Promote an axis or axes to the outermost dimension.
    """
    if isinstance(axis, int):
        axis = (axis,)
    axis = [standard_axis_number(ax, ndim) for ax in axis]
    return (*axis, *axis_complement(ndim, axis))
