import pandas as pd
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
from holoviews.streams import Selection1D, Params, Tap
import panel as pn
import xarray as xa
import geopandas as gpd
from eis.lis.data.routing import LISRoutingData
import numpy as np
import geoviews as gv
import holoviews as hv
from eis.smce import eis3, exception_handled
import hvplot.pandas
import hvplot.xarray

class LISGageDataset:

    def __init__(self, header_file: str, gage_files: List[str]=None, **kwargs):
        logger = eis3().get_logger()
        idcol = kwargs.get( 'idcol', 0 )
        idtype = kwargs.get( 'idtype', str )
        geocols = kwargs.get( 'geom', dict( x=3, y=4 ) )
        datacols = kwargs.get( 'dcols', [] )
        self._null_plot = None
        usecols = [ idcol, geocols['x'], geocols['y'] ] + datacols
        self.header: pd.DataFrame = pd.read_csv( header_file, usecols=usecols, delim_whitespace=True, names=['id', 'lon', 'lat'], dtype={'id': idtype} )
        self._gage_data: List[pd.DataFrame] = []
        self.add_gage_files( gage_files )
        logger.info( f" *** Creating LISGageDataset, header file = {header_file}, gage files = {gage_files}" )

    @property
    def gage_map(self) -> gpd.GeoDataFrame:
        return gpd.GeoDataFrame( self.header, geometry=gpd.points_from_xy( self.header.lon, self.header.lat ) )

    @property
    def points( self, **kwargs ) -> gv.Points:
        kdims = kwargs.pop( 'kdims', ['lon', 'lat'] )
        return gv.Points( self.header, kdims=kdims, vdims='id', **kwargs )

    def plot_map(self, **kwargs ):
        color = kwargs.pop( 'color', 'red' )
        size  = kwargs.pop( 'size', 10 )
        tools = kwargs.pop( 'tools', [ 'tap', 'hover' ] )
        pts_opts = gv.opts.Points( color=color, size=size, tools=tools, **kwargs )
        tiles = gv.tile_sources.EsriImagery()
        dpoints = hv.util.Dynamic( self.points.opts( pts_opts ) ).opts(height=400, width=600)
        return tiles * dpoints

    def plot(self, **kwargs ):
        color = kwargs.pop( 'color', 'red' )
        size  = kwargs.pop( 'size', 10 )
        tools = kwargs.pop( 'tools', [ 'tap', 'hover' ] )
        pts_opts = gv.opts.Points( color=color, size=size, tools=tools, nonselection_fill_alpha=0.2, nonselection_line_alpha=0.6,  **kwargs )
        tiles = gv.tile_sources.EsriImagery()
        dpoints = hv.util.Dynamic( self.points.opts( pts_opts ) ).opts(height=400, width=600)
        select_stream = Selection1D( default=[0], source=dpoints )
        line = hv.DynamicMap( self.gage_data_graph, streams=[select_stream] )
        return pn.Row( tiles * dpoints, line ) # pn.Column(var_select, line))

    @exception_handled
    def gage_data_graph( self, index: List[int] ):
        logger = eis3().get_logger()
        if (index is None) or (len(index) == 0):
            gage_data: pd.DataFrame =  self._null_plot
            return gage_data.hvplot(title=f"No Gage")
        else:
            idx = index[0]
            lon, lat = self.header['lon'][idx], self.header['lat'][idx]
            logger.info( f"gage_data_graph: index = {idx}, lon: {lon}, lat: {lat}")
            gage_data: xa.Dataset = self._gage_data[ idx ].to_xarray()
            dvars = list(gage_data.data_vars.keys())
            return gage_data[dvars[0]].hvplot( 'time', title=f"Gage[{idx}]")

    @exception_handled
    def plot_routing_data(self, routing_data: LISRoutingData, **kwargs ):
        color = kwargs.pop( 'color', 'red' )
        size  = kwargs.pop( 'size', 10 )
        tools = kwargs.pop( 'tools', [ 'tap', 'hover' ] )
        self._routing_data = routing_data
        var_select = pn.widgets.Select(options=routing_data.var_names, value='Streamflow_tavg', name="LIS Variable List")
        var_stream = Params( var_select, ['value'], rename={ 'value': 'vname' } )
        pts_opts = gv.opts.Points( color=color, size=size, tools=tools, nonselection_fill_alpha=0.2, nonselection_line_alpha=0.6,  **kwargs )
        tiles = gv.tile_sources.EsriImagery()
        dpoints = hv.util.Dynamic( self.points.opts( pts_opts ) ).opts(height=400, width=600)
        select_stream = Selection1D( default=[0], source=dpoints )
        routing_graph = hv.DynamicMap(self.routing_plus_gage_data_graph, streams=[select_stream, var_stream ])
        return pn.Row( tiles * dpoints, pn.Column( var_select, routing_graph ) )

    def xa_gage_data(self, gage_index: int ) -> xa.DataArray:
        gage_dataset: xa.Dataset = self._gage_data[gage_index].to_xarray()
        dvars = list(gage_dataset.data_vars.keys())
        return gage_dataset[dvars[0]].rename( date="time" )

    @exception_handled
    def routing_plus_gage_data_graph( self, index: List[int], vname: str ):
        logger = eis3().get_logger()
        null_data = False
        if (index is None) or (len(index) == 0):
            if self._null_plot is not None:
                return self._null_plot
            null_data = True
            idx = 0
        else:
            idx = index[0]
        lon, lat = self.header['lon'][idx], self.header['lat'][idx]
        logger.info( f"routing_data_graph: index = {idx}, lon: {lon}, lat: {lat}")
        streamflow_data: xa.DataArray = self._routing_data.var_data( vname, lon, lat )
        svname = streamflow_data.attrs['vname']
        gage_data: xa.DataArray = self.xa_gage_data( idx )
        ( streamflow_adata, gage_adata ) = xa.align( streamflow_data, gage_data )
        logger.info(f"streamflow_adata: shape = {streamflow_adata.shape}, data = {streamflow_adata[:]}")
        if null_data:
            self._null_plot = xa.zeros_like(gage_adata).hvplot( title=f"No Gages")
            return self._null_plot
        return streamflow_adata.hvplot(title=svname) * gage_adata.hvplot(title=f"Gage[{idx}]")

    def add_gage_file( self, filepath: str ):
        gage_id = filepath.split('/')[-1].strip('.txt')
        df = pd.read_csv( filepath, names=['date', gage_id], delim_whitespace=True,  parse_dates=['date'], index_col='date' )
        self._gage_data.append( df )

    def add_gage_files(self, gage_file_paths: Optional[List[str]] ):
        if gage_file_paths is not None:
            for gage_file in gage_file_paths:
                self.add_gage_file( gage_file )

    def get_empty_dataframe(self, dframe: pd.DataFrame ) -> pd.DataFrame:
        df = dframe.copy( deep = True )
        for col in df.columns:
            if np.issubdtype(df[col].dtype, np.number):
                df[col].values[:] = 0
        return df

    @property
    def gages_data(self) -> pd.DataFrame:
        return pd.concat( self._gage_data, axis=1 )

    def gage_data(self, gage_index: int ) -> pd.DataFrame:
        return self._gage_data[gage_index]
