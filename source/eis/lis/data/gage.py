import pandas as pd
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
from holoviews.streams import Selection1D, Params, Tap
import panel as pn
from functools import partial
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
        self._null_data = None
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
            gage_data: pd.DataFrame =  self._null_data
            return gage_data.hvplot(title=f"No Gage")
        else:
            idx = index[0]
            lon, lat = self.header['lon'][idx], self.header['lat'][idx]
            logger.info( f"gage_data_graph: index = {idx}, lon: {lon}, lat: {lat}")
            gage_data: pd.DataFrame = self._gage_data[ idx ]
            return gage_data.hvplot( title=f"Gage[{idx}]")

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
#        point_stream = Tap(x=self._xc, y=self._yc, source=tiles)
        line = hv.DynamicMap( self.routing_data_graph, streams=[ select_stream, var_stream ] )
        return pn.Row( tiles * dpoints, pn.Column(var_select, line) )

    @exception_handled
    def routing_data_graph( self, index: List[int], vname: str ):
        logger = eis3().get_logger()
        if (index is None) or (len(index) == 0):
            gage_data: pd.DataFrame =  self._null_data
            return gage_data.hvplot()
        else:
            idx = index[0]
            lon, lat = self.header['lon'][idx], self.header['lat'][idx]
            logger.info( f"routing_data_graph: index = {idx}, lon: {lon}, lat: {lat}")
            rdata_graph = self._routing_data.var_graph(vname, lon, lat, index='date')
            gage_data: pd.DataFrame = self._gage_data[ idx ]
            result =  gage_data.hvplot() * rdata_graph
            logger.info(f"*** overlay_graph: {result}")
            return result

    @exception_handled
    def routing_data_graph1( self, index: List[int] ):
        logger = eis3().get_logger()
        if (index is None) or (len(index) == 0):
            gage_data: pd.DataFrame =  self._null_data
            return gage_data.hvplot(title=f"No Gage")
        else:
            idx = index[0]
            vname = 'Streamflow_tavg'
            lon, lat = self.header['lon'][idx], self.header['lat'][idx]
            rdata_graph = self._routing_data.var_graph( vname, lon, lat )
            logger.info( f"routing_data_graph: index = {idx}, lon: {lon}, lat: {lat}, graph: {rdata_graph}")
            gage_data_graph = self._gage_data[ idx ].hvplot( title=f"Gage[{idx}]")
            return gage_data_graph # * rdata_graph

    def add_gage_file( self, filepath: str ):
        gage_id = filepath.split('/')[-1].strip('.txt')
        df = pd.read_csv( filepath, names=['date', gage_id], delim_whitespace=True,  parse_dates=['date'], index_col='date' )
        self._gage_data.append( df )
        if self._null_data is None:
            self._null_data = self.get_empty_dataframe(df)

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
