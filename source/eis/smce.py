import s3fs
from glob import glob
import numpy as np
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import pandas as pd
import xarray as xr
from .base import EISSingleton

class EIS3(EISSingleton):

    def __init__( self, **kwargs ):
        anon = kwargs.pop( 'anon', False )
        self.s3 = s3fs.S3FileSystem( anon=anon, **kwargs )

    def get_zarr_dataset(self, bucket: str, key: str, **kwargs ) -> xr.Dataset:
        consolidated = kwargs.pop( 'consolidated', True )
        return xr.open_zarr( self.s3.get_mapper(f'{bucket}/{key}.zarr' ), consolidated=consolidated, **kwargs )

def eis3( *args, **kwargs ):
    return EIS3.instance( *args, **kwargs )