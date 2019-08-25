import multiprocessing
from setuptools import setup, find_packages
import os
import glob

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name = "canvas_utils",
    version = "0.1",
    packages = find_packages(),

    # Dependencies on other packages:
    # Couldn't get numpy install to work without
    # an out-of-band: sudo apt-get install python-dev
    setup_requires   = [],
    install_requires = ['lxml>=4.2.5',
                        'pymysql_utils>=2.1.4',
                        'nltk>=3.4',
                        'requests>=2.21.0',
                        ],

    #dependency_links = ['https://github.com/DmitryUlyanov/Multicore-TSNE/tarball/master#egg=package-1.0']
    # Unit tests; they are initiated via 'python setup.py test'
    test_suite       = 'nose.collector',
    #test_suite       = 'tests',
    tests_require    =['nose'],

    # metadata for upload to PyPI
    author = "Andreas Paepcke",
    author_email = "paepcke@cs.stanford.edu",
    description = "Utilities for Canvas mining.",
    long_description_content_type = "text/markdown",
    long_description = long_description,
    license = "BSD",
    keywords = "MySQL",
    url = "https://github.com/paepcke/canvas_utils.git",   # project home page, if any
)
