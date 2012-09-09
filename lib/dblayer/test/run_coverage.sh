#!/bin/sh
# Runs the unit test suite with code coverage analysis
# Requires package: python-coverage
python-coverage -e
python-coverage -x ./run_tests.py
python coverage_annotate.py ..
