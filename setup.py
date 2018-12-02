import multiprocessing
from setuptools import setup, find_packages
import os
import glob

# datafiles = ['src/pathways/courseNameAcademicOrg.csv',
#              'src/pathways/crsNmDescriptions.csv'
#              ]

setup(
    name = "canvas_utils",
    version = "0.1",
    packages = find_packages(),

    # Dependencies on other packages:
    # Couldn't get numpy install to work without
    # an out-of-band: sudo apt-get install python-dev
    setup_requires   = [],
    install_requires = ['lxml>=4.2.5'
                        ],

    #dependency_links = ['https://github.com/DmitryUlyanov/Multicore-TSNE/tarball/master#egg=package-1.0']
    # Unit tests; they are initiated via 'python setup.py test'
    test_suite       = 'nose.collector', 

    # metadata for upload to PyPI
    author = "Andreas Paepcke",
    author_email = "paepcke@cs.stanford.edu",
    description = "Utilities for Canvas mining.",
    license = "BSD",
    keywords = "MySQL",
    url = "git@github.com:paepcke/canvas_utils.git",   # project home page, if any
)
