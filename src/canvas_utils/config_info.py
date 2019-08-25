'''
Created on Aug 24, 2019

@author: paepcke
'''
import configparser
import os

from canvas_utils_exceptions import ConfigurationError

class ConfigInfo(object):
    '''
    Loads configuration file setup.cfg if it exists.
    Else loads setupSample.cfg. Holds all the read
    configuration values for other modules to refer to.
    
    This is a singleton.
    '''
    __instance = None
    __is_initialized = False
    
    #-------------------------
    # __new__ 
    #--------------
    
    def __new__(cls):
        if ConfigInfo.__instance is None:
            ConfigInfo.__instance = object.__new__(cls)
        return ConfigInfo.__instance
    
    #-------------------------
    # __repr__ 
    #--------------
    
    def __repr__(self):
        return f'<CanvasUtils ConfigInfo {hex(id(self))}>'
        
    #-------------------------
    # Constructor 
    #--------------
    
    def __init__(self):
        '''
        Constructor
        '''
        if ConfigInfo._ConfigInfo__is_initialized:
            return
        else:
            self.read_config_file()
            ConfigInfo._ConfigInfo__is_initialized = True

    # -------------------- Property Methods -----------------
    
    @property
    def default_host(self):
        return self._default_host
    
    @property
    def default_user(self):
        return self._default_user

    @property
    def canvas_db_aux(self):
        return self._canvas_db_aux
    
    @property
    def canvas_pwd_file(self):
        return self._canvas_pwd_file

    @property
    def test_default_host(self):
        return self._test_default_host
    
    @property
    def test_default_user(self):
        return self._test_default_user


    #-------------------------
    # read_config_file 
    #--------------
    
    def read_config_file(self):
        
        curr_dir = os.path.dirname(__file__)
        setup_file_dir   = os.path.abspath(os.path.join(curr_dir, '../..'))
        
        # If a customized setup.cfg file exists, we use it.
        # Else we use the setupSample.cfg file that comes
        # with the distro:
        
        setup_local_path = os.path.join(setup_file_dir, 'setup.cfg') 
        setup_sample_file_path = os.path.join(setup_file_dir, 'setupSample.cfg') 
        if os.path.exists(setup_local_path):
            setup_path = setup_local_path
        else:
            setup_path = setup_sample_file_path
        
        setup_file_name = os.path.basename(setup_path)
    
        config_parser = configparser.ConfigParser()
        config_parser.read(setup_path)

        try:
            self._default_host = config_parser['DATABASE']['default_host']
        except KeyError:
            raise ConfigurationError(f"Cannot read DATABASE:default_host from {setup_file_name}")

        try:
            self._canvas_db_aux = config_parser['DATABASE']['canvas_auxiliary_db_name']
        except KeyError:
            raise ConfigurationError(f"Cannot read DATABASE:canvas_auxiliary_db_name from {setup_file_name}")  

        try:
            self._default_user = config_parser['DATABASE']['default_user']
        except KeyError:
            raise ConfigurationError(f"Cannot read DATABASE:default_user from {setup_file_name}")

        try:
            self._test_default_host = config_parser['TESTMACHINE']['mysql_host']
        except KeyError:
            raise ConfigurationError(f"Cannot read TESTMACHINE:mysql_host from {setup_file_name}")

        try:
            self._test_default_user = config_parser['TESTMACHINE']['mysql_user']
        except KeyError:
            raise ConfigurationError(f"Cannot read TESTMACHINE:mysql_user from {setup_file_name}")

        try:
            self._canvas_pwd_file = config_parser['DATABASE']['canvas_pwd_file']
        except KeyError:
            raise ConfigurationError(f"Cannot read DATABASE:canvas_pwd_file from {setup_file_name}")

# -------------------- Testing -----------

if __name__ == '__main__':
    inst1 = ConfigInfo()
    ConfigInfo._ConfigInfo__is_initialized
    inst2 = ConfigInfo()
    assert(inst1 == inst2)
    