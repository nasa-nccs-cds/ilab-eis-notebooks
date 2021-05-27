import s3fs
from glob import glob
import geopandas as gpd
import geoviews as gv
import holoviews as hv
import numpy as np
import pandas as pd
import xarray as xr


class EIS():

    def __init__(self):
        print( "Created EIS controller")
        self.s3 = s3fs.S3FileSystem(anon=False)

    def get_zarr_dataset(self, bucket: str, key: str ):
        return xr.open_zarr( self.s3.get_mapper(f'{bucket}/{key}.zarr' ), consolidated=True)