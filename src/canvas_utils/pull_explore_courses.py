#!/usr/bin/env python
'''
Created on Jan 2, 2019

@author: Andreas Paepcke
'''
from contextlib import contextmanager
import logging
import os
import sys

import requests
from explore_courses_etl import ECXMLExtractor


class ECPuller(object):
    '''
    Retrieves the latest ExploreCourses data in xml format.
    Window: July 1, 2014 to present. Optional logging. Can
    be invoked from the command line or as an imported module.
    Places result in a specified file. 
    
    Instances can be queried for number of bytes pulled right
    after instanciation: inst.bytes_pulled
    '''
    # Root of the request URL:
    ec_url      = "https://explorecourses.stanford.edu/search"

    # Dict with the eventual HTTP request parameters: 
    ec_req_dict = {'view' : 'xml-20140630',
                   'filter-coursestatus-Active' : 'on',
                   'q' : '%25'
                   }
    
    log_level_translation = {'criticl' : logging.CRITICAL,
                             'error'   : logging.ERROR,
                             'warning' : logging.WARNING,
                             'info'    : logging.INFO,
                             'debug'   : logging.DEBUG,
                             'notset'  : logging.NOTSET,
                             None      : logging.NOTSET}

    def __init__(self, 
                 outfile, 
                 overwrite_existing=False, 
                 log_level=None,
                 logger=None):
        '''
        Do sanity checks, then pull the xml into outfile
        
        Outfile may be relative the the directory of this script,
        or absolute. If directory of requested outfile location does
        not exist, it is created. 
        
        Log levels may be specified as logging.INFO, logging.DEBUG,
        etc, or as a string: 'critical', 'error', 'warning', 'info', 
        'debug', or 'notset'. We accept both to make it easy to invoke
        this script from the command line, or from another script from
        which an ECPuller is created and used.
        
        If log_level is None, 'notset', of logging.NOTSET, no logging
        occurs, regardless of the value in logger. Else if a logger
        is provided it is used for logging. If no logger provided when
        logging is requested, a new logger is created. 
        
        @param outfile: file to which .xml is to be written. 
        @type outfile:
        @param overwrite_existing: if True, and the output file already exists,
            error. Else existing files are overwritten.
        @type overwrite_existing: str
        @param log_level: logging level. If None, no logging.
        @type log_level: {str|logging.LogingLevel}
        @param logger: a Python logger to use for progress reporting.
        @type logger: logging.Logger
        '''
        if not os.path.isabs(outfile):
            # Relative path: take this script's dir as the root:
            curr_dir = os.path.dirname(__file__)
            outfile = os.path.join(curr_dir, outfile)
            
        # Ensure that the outfile directory exists:
        out_dir = os.path.dirname(outfile)
        os.makedirs(out_dir, mode=0o755, exist_ok=True)
        
        if os.path.exists(outfile) and not overwrite_existing:
            raise RuntimeError("File %s already exists, and overwrite_existing is False." % outfile)
        
        if log_level is not None and log_level != 'notset':
            if type(log_level) == str:
                log_level = ECPuller.log_level_translation[log_level]
            if logger is None:
                self.setup_logging(log_level, logFile=None)
                logger = self.logger
        else:
            logger = None
        
        self.outfile = outfile
        self.logger  = logger
        
        # Ready for caller to invoke pull_ec(outfile, logger) for
        # pulling a new xml file, or ec_xml_to_csv(xml_file, outfile)
        # to turn an already pulled xml file into CSV 
        
        
    #-------------------------
    # pull_ec 
    #--------------

    def pull_ec(self, outfile=None, logger=None):
        '''
        Contact server, and pull data in chunks. Outfile
        may be relative the the directory of this script,
        or absolute.
        
        @param outfile: file to which .xml is to be written. 
        @type outfile:
        @param overwrite_existing: if True, and the output file already exists,
            error. Else existing files are overwritten.
        @type overwrite_existing: str
        @return: number of kbytes pulled
        @rtype: float
        '''
        if outfile is None:
            outfile = self.outfile
        if logger is None:
            logger = self.logger
        
        # Number KB after which to report progress, if
        # a logger is available. Here: every 20MB:
        if logger is not None:
            log_freq = 100
            kbytes_pulled = 0
            next_reporting_point = log_freq
        
        req = requests.get(ECPuller.ec_url, ECPuller.ec_req_dict)
        with open(outfile, 'wb') as fd:
            for chunk in req.iter_content(chunk_size=128):
                fd.write(chunk)
                kbytes_pulled += 0.128

                if logger is not None and kbytes_pulled >= next_reporting_point:
                    logger.info('Pulled {:.0f}KB from Explore Courses.'.format(kbytes_pulled))
                    next_reporting_point += log_freq
                        
        if logger:
            logger.info("Result in {} ({:.1f}KB)".format(outfile, kbytes_pulled))
        
        return kbytes_pulled
        
    #-------------------------
    # bytes_pulled 
    #--------------
        
    def bytes_pulled(self, human_readable=False):
        '''
        Return number of bytes retrieved from ExploreCourses.
        If human_readable is True, returns a string: xxxGB,
        xxxMB, xxxKB, or xxx bytes. If human_readable is False,
        then number of bytes are returned as an integer.
        
        @param human_readable: whether or not to return human-friendly form
        @type human_readable: bool
        @return: number of bytes
        @rtype: {int | str}
        '''

        byte_size = os.stat(self.outfile).st_size
        if not human_readable:
            return byte_size
        
        gb = 1000**3
        mb = 1000**2
        kb = 1000
        
        if byte_size >= gb:
            # Return in GB with one decimal point:
            return "{}GB".format(round(byte_size / gb, 1))
        elif byte_size >= mb:
            # Return in MB with one decimal point:
            return "{}MB".format(round(byte_size / mb, 1))
        elif byte_size >= kb:
            # Return in KB with one decimal point:
            return "{}KB".format(round(byte_size / kb, 1))
        else:
            return "{} bytes".format(byte_size)

    #-------------------------
    # ec_xml_to_csv 
    #--------------
    
    def ec_xml_to_csv(self, xml_infile, csv_outfile=None):

        with open(xml_infile, 'r') as fd:
            if csv_outfile is not None:
                with open(csv_outfile, 'w') as file_out_fd:
                    with stdout_redirected(file_out_fd):
                        ECXMLExtractor(fd)
            else:
                ECXMLExtractor(fd)
    
    #-------------------------
    # setup_logging 
    #--------------
        
    def setup_logging(self, loggingLevel=logging.INFO, logFile=None):
        '''
        Set up the standard Python logger.

        @param loggingLevel: initial logging level
        @type loggingLevel: {logging.INFO|WARN|ERROR|DEBUG}
        @param logFile: optional file path where to send log entries
        @type logFile: str
        '''

        self.logger = logging.getLogger(os.path.basename(__file__))

        # Create file handler if requested:
        if logFile is not None:
            handler = logging.FileHandler(logFile)
            print('Logging of control flow will go to %s' % logFile)
        else:
            # Create console handler:
            handler = logging.StreamHandler()
        handler.setLevel(loggingLevel)

        # Create formatter
        formatter = logging.Formatter("%(name)s: %(asctime)s;%(levelname)s: %(message)s")
        handler.setFormatter(formatter)

        # Add the handler to the logger
        self.logger.addHandler(handler)
        self.logger.setLevel(loggingLevel)
    
# ---------------------------------- Support Functions -------------------    
    
#-------------------------
# stdout_redirection 
#--------------

@contextmanager
def stdout_redirected(new_stdout):
    save_stdout = sys.stdout
    sys.stdout = new_stdout
    try:
        yield None
    finally:
        sys.stdout = save_stdout
        
# ---------------------------------- Main -------------------            
        
if __name__ == '__main__':
    
    ec_puller = ECPuller('/tmp/ec.xml', log_level='info', overwrite_existing=True)
    #****ec_puller.pull_ec()
    ec_puller.ec_xml_to_csv('/tmp/ec.xml', '/tmp/ec.csv')
    #print("EC size: {}".format(ec_puller.bytes_pulled(human_readable=True)))   