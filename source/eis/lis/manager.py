import xarray as xr
import numpy as np
import pandas as pd
import geopandas as gpd
from eis.controller import eis

class LISGageData:



class LIS:

    @classmethod
    def add_latlon_coords(cls, input_dset: xr.Dataset) -> xr.Dataset:
        """Adds lat/lon as dimensions and coordinates to an xarray.Dataset object."""

        dx = round( float( input_dset.attrs['DX'] ), 3)
        dy = round( float( input_dset.attrs['DY'] ), 3)
        ew_len = len( input_dset['east_west'] )
        ns_len = len( input_dset['north_south'] )
        ll_lat = round( float( input_dset.attrs['SOUTH_WEST_CORNER_LAT'] ), 3)
        ll_lon = round( float( input_dset.attrs['SOUTH_WEST_CORNER_LON'] ), 3)
        ur_lat = ll_lat + (dy * ns_len)
        ur_lon = ll_lon + (dx * ew_len)

        coords = dict(
            lat= np.linspace(ll_lat, ur_lat, ns_len, dtype=np.float32, endpoint=False),
            lon= np.linspace(ll_lon, ur_lon, ew_len, dtype=np.float32, endpoint=False)
        )

        cmap = dict( north_south= 'lat', east_west= 'lon' )
        result = input_dset.drop_vars( cmap.values() ).rename( cmap ).assign_coords( coords )
        result.lon.attrs =  input_dset.lon.attrs
        result.lat.attrs =  input_dset.lat.attrs
        return result

    @classmethod
    def get_zarr_routing(cls, bucket: str, key: str)-> xr.Dataset:
        dset = eis().get_zarr_dataset( bucket, key )
        return cls.add_latlon_coords(dset)

    def get_streamflow_header(self, filePath: str, **kwargs ) -> gpd.GeoDataFrame:
        cols = dict( id= (0, 'gage_id', str), x= (3, 'lon', float), y= (4, 'lat', float) )
        cols.update( kwargs )
        df: pd.DataFrame = eis().read_csv_dataset( filePath, cols )
        geometry = gpd.points_from_xy( getattr( df, cols['x'][1] ), getattr( df, cols['y'][1] ) )
        return  gpd.GeoDataFrame( df, geometry=geometry )
