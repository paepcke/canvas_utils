#!/usr/bin/env python
'''
Created on Jan 2, 2019

@author: paepcke
'''
import argparse
import os
import sys


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     description="Substitute db name for <canvas_db> in Queries/<sqlFile>.sql."
                                     )

    parser.add_argument('sqlfile',
                        help='fully qualified path to SQL file, or just the filename. \n' +
                             'In latter case, location in Queries subdir assumed'
                        )
    parser.add_argument('database',
                        help='Name of database to substitute for <canvas_db> in SQL files.'
                        )

    args = parser.parse_args();

    curr_dir = os.path.dirname(__file__)
    if not os.path.isabs(args.sqlfile):
        queries_dir = os.path.join(curr_dir, 'Queries')
        sql_file = os.path.join(queries_dir, args.sqlfile)
    else:
        sql_file = args.sqlfile
    with open(sql_file, 'r') as fd:
        sql_txt = fd.read().strip()
        sql_txt_final = sql_txt.replace('<canvas_db>', args.database)
        
    sys.stdout.write(sql_txt_final)