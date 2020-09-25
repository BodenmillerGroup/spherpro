# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='spherpro',
    version='0.1.1',
    description='Tool to analize tumor spheroid data',
    long_description=readme,
    long_description_content_type='text/x-rst',
    author='Vito Zanotelli, Matthias Leutenegger, Bodenmiller Lab UZH',
    author_email='vito.zanotelli@uzh.ch',
    url='https://github.com/bodenmillerlab/spherpro',
    license='BSD-3 License',
    packages=find_packages(exclude=('tests', 'docs')),
    install_requires=[
        'anndata',
        'colorcet',
        'imctools==1.0.7',
        'matplotlib',
        'numpy',
        'pandas',
        'pyyaml',
        'requests',
        'scikit-image',
        'scikit-learn',
        'scipy',
        'seaborn',
        'sqlalchemy',
        'tifffile',
        'sphinx==3.2.1',
        'plotnine',
        'matplotlib_scalebar',
        'pycytools>0.6',
        'ipywidgets']
)
