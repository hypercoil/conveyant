# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
from .compositors import (
    close_imapping_compositor,
    close_omapping_compositor,
    delayed_outer_compositor,
    direct_compositor,
    reversed_args_compositor,
)
from .containers import (
    Composition,
    FunctionWrapper,
    PartialApplication,
    PipelineArgument,
    PipelineStage,
    Primitive,
)
from .emulate import (
    emulate_assignment,
    splice_on,
)
from .flows import (
    ichain,
    imap,
    imapping_composition,
    inject_params,
    iochain,
    join,
    null_transform,
    ochain,
    omap,
    omapping_composition,
    split_chain,
)
from .replicate import (
    replicate,
)
