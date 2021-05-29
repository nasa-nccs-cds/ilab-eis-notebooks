import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
import pandas as pd
import geopandas as gpd
from eis.smce import eis3

class LISRoutingData:

    def __init__( self, dset: xr.Dataset, **kwargs ):
        self.dset = self._add_latlon_coords( dset )

    @classmethod
    def from_smce( cls, bucket: str, key: str ) -> "LISRoutingData":
        dset = eis3().get_zarr_dataset(bucket, key)
        return LISRoutingData( dset )

    def site_data(self, varName: str, lat: float, lon: float, **kwargs ) -> xr.DataArray:
        ts = kwargs.get('ts',None)
        vardata = self.dset[varName]
        sargs = dict( lat=lat, lon=lon )
        if ts is not None: sargs['time'] = slice(*ts)
        return vardata.sel( **sargs )

    def site_graph(self, varName: str, lat: float, lon: float, **kwargs ):
        vardata: xr.DataArray = self.site_data( varName, lat, lon, ts=kwargs.pop('ts',None) )
        figsize = kwargs.pop( 'figsize', (8, 5) )
        lplot: plt.Line2D = vardata.plot( figsize=figsize, **kwargs )
        fig: plt.Figure = lplot.get_figure()
        fig.patch.set_facecolor('yellow')
        fig.set_facecolor('yellow')
        return lplot

    @classmethod
    def _add_latlon_coords(cls, input_dset: xr.Dataset) -> xr.Dataset:
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