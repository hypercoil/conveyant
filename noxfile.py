# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Noxfile
"""
import nox

@nox.session()
def clean(session):
    session.install('coverage[toml]')
    session.run('coverage', 'erase')

@nox.session(python=["3.10", "3.11"])
def tests(session):
    session.install('.[dev]')
    session.run(
        'pytest',
        '--cov', 'conveyant',
        '--cov-append',
        'tests/tests.py',
    )
    session.run('ruff', 'check', 'src/conveyant')

@nox.session()
def report(session):
    session.install('coverage[toml]')
    session.run(
        'coverage',
        'report', '--fail-under=90',
        "--omit='*test*,*__init__*'",
    )
    session.run(
        'coverage',
        'html',
        "--omit='*test*,*__init__*'",
    )
    session.run(
        'coverage',
        'xml',
        "--omit='*test*,*__init__*'",
    )
