# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='spherpro',
    version='0.0.1',
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
        'imctools',
        'odo',
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
        'tifffile'],
)
