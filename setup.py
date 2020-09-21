# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='spherpro',
    version='0.0.3',
    description='Tool to analize tumor spheroid data',
    long_description=readme,
    author='Vito Zanotelli,',
    author_email='vito.zanotelli@uzh.ch',
    url='https://github.com/bodenmillerlab/spherpro',
    license=license,
    packages=find_packages(exclude=('tests', 'docs')),
    install_requires=[
        'anndata',
        'colorcet',
        'imctools==1.0.7',
        'odo @ git+https://github.com/blaze/odo@master#egg=package-1.0',
        # 'PyMySQL',
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
        'pycytools @ git+https://github.com/BodenmillerGroup/pycytools@master#egg=package-1.0',
        'ipywidgets']
)
