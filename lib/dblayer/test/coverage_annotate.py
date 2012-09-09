#!/usr/bin/python

import os, sys

source_files = []

DO_NOT_COVER = set(['coverage_annotate.py'])

for dirpath, dirnames, filenames in os.walk(sys.argv[1]):
    for fn in filenames:
        if fn in DO_NOT_COVER:
            continue
        if not fn.endswith('.py'):
            continue
        fp = os.path.join(dirpath, fn)
        os.system('python-coverage -a "%s"' % fp)
        source_files.append(fp)
        
os.system('python-coverage -r -m %s >report.coverage' % ' '.join('"%s"' % fp for fp in source_files))
