import s3fs, logging, os, socket, traceback
from glob import glob
import numpy as np
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import pandas as pd
import xarray as xr
from .base import EISSingleton

class EIS3(EISSingleton):

    def __init__( self, **kwargs ):
        EISSingleton.__init__( self, **kwargs )
        anon = kwargs.pop( 'anon', False )
        self.s3 = s3fs.S3FileSystem( anon=anon, **kwargs )
        self.eis_dir = os.path.expanduser( "~/.eis_smce" )
        self.cache_dir = self.eis_subdir(  "cache" )
        self.log_dir =   self.eis_subdir(  "logging" )
        self.data_dir =  self.eis_subdir(   "data" )

    def eis_subdir(self, name: str ):
        subdir = os.path.join( self.eis_dir, name )
        os.makedirs( subdir, exist_ok=True)
        return subdir

    def get_zarr_dataset(self, bucket: str, key: str, **kwargs ) -> xr.Dataset:
        return xr.open_zarr( self.s3.get_mapper(f'{bucket}/{key}.zarr' ),  **kwargs )

    @classmethod
    def hostname(cls):
        return socket.gethostname()

    @classmethod
    def pid(cls):
        return os.getpid()

    @staticmethod
    def item_path( path: str) -> str:
        return path.split(":")[-1].replace("//", "/").replace("//", "/")

    def get_logger(self):
        _logger = logging.getLogger(f'eis.smce.{self.hostname()}.{self.pid()}')
        _root_logger = logging.getLogger()
        if len( _logger.handlers ) == 0:
            _logger.setLevel(logging.DEBUG)
            log_file = f'{self.log_dir}/eis.smce.{self.hostname()}.{self.pid()}.log'
            root_log_file = f'{self.log_dir}/root.{self.hostname()}.{self.pid()}.log'
            print( f" ***   Opening Log file: {log_file}  *** ")
            os.makedirs( os.path.dirname(log_file), exist_ok=True )
            fh = logging.FileHandler( log_file )
            fh.setLevel(logging.DEBUG)
            ch = logging.StreamHandler()
            ch.setLevel(logging.ERROR)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)
            _logger.addHandler(fh)
            _logger.addHandler(ch)
            rfh = logging.FileHandler(root_log_file)
            rfh.setLevel(logging.INFO)
            rfh.setFormatter(formatter)
            _root_logger.addHandler(rfh)
        return _logger

    def exception(self, msg: str ):
        self.get_logger().error( f"\n{msg}\n{traceback.format_exc()}\n" )


def eis3( *args, **kwargs ):
    return EIS3.instance( *args, **kwargs )

def exception_handled(func):
    def wrapper( *args, **kwargs ):
        try:        return func( *args, **kwargs )
        except:     eis3().exception( f" Error in {func}:")
    return wrapper