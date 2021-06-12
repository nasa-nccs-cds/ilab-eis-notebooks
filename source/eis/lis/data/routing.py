import xarray as xr
import time, numpy as np
import matplotlib.pyplot as plt
from eis.smce import eis3, exception_handled
from functools import partial
from scipy.spatial import distance
import matplotlib as mpl, traceback
from holoviews.streams import Selection1D, Params, Stream, param, Tap
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import holoviews as hv
import panel as pn
import xarray as xa
import logging, pandas as pd
import geopandas as gpd
from eis.smce import eis3
import hvplot.pandas
import hvplot.xarray

class LISRoutingData:

    def __init__( self, dset: xr.Dataset, **kwargs ):
        self.dset: xr.Dataset = self._add_latlon_coords( dset )
        self._vnames = None
        defvar = kwargs.get('default_var','Streamflow_tavg')
        self.default_variable: str = defvar if defvar in self.var_names else self.var_names[0]
    #    self._loc = dset[['lon','lat']].isel(time=0).to_dataframe().reset_index().dropna()
    #   self._pts: np.ndarray = self._loc[['lon', 'lat']].to_numpy()

    def add_variable(self, name: str, variable: xa.DataArray ):
        self.dset = self.dset.assign( {name: variable} )
        self._vnames = None

    @classmethod
    def from_smce( cls, bucket: str, key: str ) -> "LISRoutingData":
        dset = eis3().get_zarr_dataset(bucket, key)
        return LISRoutingData( dset )

    @classmethod
    def from_disk( cls, path: str, **kwargs ) -> "LISRoutingData":
        dset = xr.open_zarr( path, **kwargs )
        return LISRoutingData( dset )

    # def nearest_grid( self, lon, lat ) -> List[int]:
    #     idx = distance.cdist( np.array([lon,lat]), self._pts ).argmin()
    #     return  [ int(self._loc['east_west'].iloc[idx]), int(self._loc['north_south'].iloc[idx]) ]

    @property
    def var_names(self) -> List[str]:
        if self._vnames is None:
            self._vnames = [ str(k) for k,v in self.dset.variables.items() if v.ndim == 3 ]
        return self._vnames

    def dynamic_map( self, **kwargs  ):
        lat = param.Number( default=0.0, doc='Latitude' )
        lon = param.Number( default=0.0, doc='Longitude' )
        vname = param.String( default="", doc='Variable Name' )
        return hv.DynamicMap( self.site_data, kdims=[ vname, lon, lat ], streams=kwargs.get( 'streams',[]) )

    def site_data(self, vname: str, lon: float, lat: float, **kwargs ) -> xr.DataArray:
        ts = kwargs.get('ts',None)
        vardata = self.dset[vname]
        sargs = dict( lat=lat, lon=lon )
        if ts is not None: sargs['time'] = slice(*ts)
        return vardata.sel( **sargs )

    @exception_handled
    def var_image( self, streams ) -> hv.DynamicMap:
        def vmap( vname: str ):
            logger = eis3().get_logger()
            try:
                t0 = time.time()
                logger.info(f"Plotting map image[{vname}]")
                image_data: xr.DataArray = self.dset[vname].isel(time=0)
                image_plot =  image_data.hvplot( title=vname )
                logger.info(f"Result shape = {image_data.shape}, exec time = {time.time() - t0} sec")
                return image_plot
            except Exception as err:
                logger.error(f"Map plot generated exception: {err}\n{traceback.format_exc()}")
                raise err
        return hv.DynamicMap( vmap, streams=streams )

    def var_data( self, vname: str, x: float, y: float ) -> xa.DataArray:
        logger = eis3().get_logger()
        t0 = time.time()
        ics = self.get_indices(x, y)
        logger.info( f"Plotting var_graph[{vname}]: lon={x} ({ics['lon']}), lat={y} ({ics['lat']})")
        gdata = self.dset[vname].isel( lon= ics['lon'], lat= ics['lat'] )
        gdata.compute()
        t1 = time.time()
        logger.info(f"-->> gdata[{vname}] shape = {gdata.shape}, dims={gdata.dims}: read time= {t1 - t0}, plot time= {time.time() - t1} sec")
        gdata.attrs['vname'] = vname
        return gdata

    def var_graph(self, vname: str, x: float, y: float) :
        return self.var_data( vname, x, y ).hvplot( title=vname )

    @exception_handled
    def dvar_graph( self, streams ) -> hv.DynamicMap:
        return hv.DynamicMap( self.var_graph, streams=streams )

    @exception_handled
    def plot(self):
        var_select = pn.widgets.Select(options=self.var_names, value=self.default_variable, name="LIS Variable List")
        var_stream = Params( var_select, ['value'], rename={ 'value': 'vname' } )
        varmap = self.var_image(streams=[var_stream])
        point_stream = Tap( x=self._xc, y=self._yc, source=varmap )
        vargraph = self.dvar_graph( [var_stream, point_stream] )
        return pn.Row( varmap, pn.Column( var_select, vargraph ) )

    @exception_handled
    def site_graph(self, varName: str, lat: float, lon: float, **kwargs ):
        vardata: xr.DataArray = self.site_data( varName, lat, lon, ts=kwargs.pop('ts',None) )
        figsize = kwargs.pop( 'figsize', (8, 5) )
        lplots = vardata.plot( figsize=figsize, **kwargs )
        fig: plt.Figure = lplots[0].get_figure()
        fig.set_facecolor('yellow')
        return lplots

    def get_indices( self, lon: float, lat: float ) -> Dict[str,int]:
        ix = int( ( lon - self._x0 ) // self._dx )
        iy = int( ( lat - self._y0 ) // self._dy )
        return dict( lon=ix, lat=iy )

    def _add_latlon_coords(self, input_dset: xr.Dataset) -> xr.Dataset:
        """Adds lat/lon as dimensions and coordinates to an xarray.Dataset object."""

        self._dx = round( float( input_dset.attrs['DX'] ), 3 )
        self._dy = round( float( input_dset.attrs['DY'] ), 3 )
        self._nx = len( input_dset['east_west'] )
        self._ny = len( input_dset['north_south'] )
        self._y0 = round( float( input_dset.attrs['SOUTH_WEST_CORNER_LAT'] ), 3)
        self._x0 = round( float( input_dset.attrs['SOUTH_WEST_CORNER_LON'] ), 3)
        self._y1 = self._y0 + (self._dy * self._ny )
        self._x1 = self._x0 + (self._dx * self._nx )
        self._lat = np.linspace( self._y0, self._y1, self._ny, dtype=np.float32, endpoint=False )
        self._lon = np.linspace( self._x0, self._x1, self._nx, dtype=np.float32, endpoint=False )
        self._xc = self._lon[ self._nx // 2 ]
        self._yc = self._lat[ self._ny // 2 ]
        coords = dict( lat=self._lat, lon=self._lon )
        cmap = dict( north_south= 'lat', east_west= 'lon' )

        result = input_dset.drop_vars( cmap.values() ).rename( cmap ).assign_coords( coords )
        result.lon.attrs =  input_dset.lon.attrs
        result.lat.attrs =  input_dset.lat.attrs
        return result