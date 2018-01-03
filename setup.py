#!/usr/bin/env python

from __future__ import with_statement

from setuptools import setup, find_packages

with open("README") as readme:
    documentation = readme.read()

setup(
    name="tbucket",
    version="0.99.0",
    description="A sqlite-backed token bucket rate limiter implementation.",
    long_description=documentation,
    author="AllSeeingEyeTolledEweSew",
    author_email="allseeingeyetolledewesew@protonmail.com",
    url="http://github.com/AllSeeingEyeTolledEweSew/tbucket",
    license="Unlicense",
    py_modules=["tbucket"],
)
