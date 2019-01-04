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
    parser.add_argument('prod_db',
                        help='Name of database to substitute for <canvas_db> in SQL files.'
                        )
    parser.add_argument('aux_db',
                        help='Name of database to substitute for <canvas_aux> in SQL files.'
                        )
    parser.add_argument('data_dir',
                        help="Full path to Data dir, or just the last part (e.g. 'Data'."
                        )

    args = parser.parse_args();

    curr_dir = os.path.dirname(__file__)
    if not os.path.isabs(args.sqlfile):
        queries_dir = os.path.join(curr_dir, 'Queries')
        sql_file = os.path.join(queries_dir, args.sqlfile)
    else:
        sql_file = args.sqlfile
        
    if not os.path.isabs(args.data_dir):
        data_dir = os.path.join(curr_dir, args.data_dir)
    else:
        data_dir = args.data_dir
        
    
    with open(sql_file, 'r') as fd:
        sql_txt = fd.read().strip()
        sql_txt_final = sql_txt.replace('<canvas_db>', args.prod_db)
        sql_txt_final = sql_txt_final.replace('<canvas_aux>', args.aux_db)
        sql_txt_final = sql_txt_final.replace('<data_dir>', data_dir)
        
    sys.stdout.write(sql_txt_final)