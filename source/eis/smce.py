import s3fs, logging, os, socket
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
        self.cache_dir = os.path.expanduser( "~/.eis_smce")
        os.makedirs( self.cache_dir, exist_ok=True )

    def get_zarr_dataset(self, bucket: str, key: str, **kwargs ) -> xr.Dataset:
        consolidated = kwargs.pop( 'consolidated', True )
        return xr.open_zarr( self.s3.get_mapper(f'{bucket}/{key}.zarr' ), consolidated=consolidated, **kwargs )

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
        if len( _logger.handlers ) == 0:
            _logger.setLevel(logging.DEBUG)
            log_file = f'{self.cache_dir}/logging/eis.smce.{self.hostname()}.{self.pid()}.log'
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
        return _logger

def eis3( *args, **kwargs ):
    return EIS3.instance( *args, **kwargs )

