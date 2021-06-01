import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
from functools import partial
from scipy.spatial import distance
import matplotlib as mpl
from holoviews.streams import Stream, param
import holoviews as hv
import pandas as pd
import geopandas as gpd
from eis.smce import eis3

class LISRoutingData:

    def __init__( self, dset: xr.Dataset, **kwargs ):
        self.dset: xr.Dataset = self._add_latlon_coords( dset )
        self.loc = self.dset[['lon', 'lat']].isel(time=0).to_dataframe().reset_index()

    @classmethod
    def from_smce( cls, bucket: str, key: str ) -> "LISRoutingData":
        dset = eis3().get_zarr_dataset(bucket, key)
        return LISRoutingData( dset )

    def nearest_grid(self, pt):
        loc_valid = self.loc.dropna()
        pts = loc_valid[ ['lon', 'lat'] ].to_numpy()
        return distance.cdist([pt], pts).argmin()

    def dynamic_map( self, **kwargs  ):
        lat = param.Number( default=0.0, doc='Latitude' )
        lon = param.Number( default=0.0, doc='Longitude' )
        vname = param.String( default="", doc='Variable Name' )
        return hv.DynamicMap( self.site_data, kdims=[ vname, lon, lat ], streams=kwargs.get( 'streams',[]) )

    def idx_dynamic_map( self, **kwargs  ):
        idx = param.Number( default=0.0, doc='Longitude' )
        vname = param.String( default="", doc='Variable Name' )
        return hv.DynamicMap( self.site_data, kdims=[ vname, lon, lat ], streams=kwargs.get( 'streams',[]) )

    def site_data(self, vname: str, lon: float, lat: float, **kwargs ) -> xr.DataArray:
        ts = kwargs.get('ts',None)
        vardata = self.dset[vname]
        sargs = dict( lat=lat, lon=lon )
        if ts is not None: sargs['time'] = slice(*ts)
        return vardata.sel( **sargs )

    def idx_site_data(self, vname: str, index: int ) -> xr.DataArray:
        return self.dset[vname].isel( id = index )

    def site_graph(self, varName: str, lat: float, lon: float, **kwargs ):
        vardata: xr.DataArray = self.site_data( varName, lat, lon, ts=kwargs.pop('ts',None) )
        figsize = kwargs.pop( 'figsize', (8, 5) )
        lplots = vardata.plot( figsize=figsize, **kwargs )
        fig: plt.Figure = lplots[0].get_figure()
        fig.set_facecolor('yellow')
        return lplots

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