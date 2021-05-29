import s3fs
from glob import glob
import geopandas as gpd
import geoviews as gv
import holoviews as hv
import numpy as np
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import pandas as pd
import xarray as xr
from .base import EISSingleton

class EIS(EISSingleton):

    def __init__(self):
        print( "Created EIS controller")
        self.s3 = s3fs.S3FileSystem(anon=False)

    def get_zarr_dataset(self, bucket: str, key: str ) -> xr.Dataset:
        return xr.open_zarr( self.s3.get_mapper(f'{bucket}/{key}.zarr' ), consolidated=True )

    def read_csv_dataset(self, filePath: str, cols: List ) -> pd.DataFrame:
        usecols = [ cols[k][0] for k in ['ix'] ]
        return pd.read_csv( filePath, usecols=[0, 3, 4], delim_whitespace=True, names=['gage_id', 'lon', 'lat'],
                    dtype={'gage_id': str})

def eis( *args, **kwargs ):
    return EIS.instance( *args, **kwargs )