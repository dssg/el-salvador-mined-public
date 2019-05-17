# coding: utf-8

from setuptools import setup, find_packages

setup(
    name="mined",
    version="0.1",
    packages=find_packages(),

    author=open('AUTHORS').read(),
    long_description=open('README.md').read(),

    entry_points="""
      [console_scripts]
      mined=mined.run:run
    """
)
