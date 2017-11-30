import os
import shutil
import sys
import glob
from setuptools import setup, find_packages


setup(
    name='chado-export',
    version='1.0.0',
    description='generate_gff_from_chado.py: a script to export organism genome data in Chado, to GFF files.',
    package_dir={'': 'bin'},
	package_data={'chado-export': ['bin/*.ini']},
    author='Sascha Steinbiss, Kevin Pepper',
    author_email='path-help@sanger.ac.uk',
    url='https://github.com/sanger-pathogens/chado-export',
    scripts=glob.glob('bin/*.py') + glob.glob('bin/*.ini'),
    test_suite='nose.collector',
    tests_require=['nose >= 1.3'],
    install_requires=[],
    license='GPLv3',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'Programming Language :: Python :: 3 :: Only',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
    ],
)


