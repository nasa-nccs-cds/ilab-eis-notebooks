import pandas as pd
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
from holoviews.streams import Selection1D, Params
import matplotlib.pyplot as plt
import panel as pn
from matplotlib.axes import SubplotBase
import geopandas as gpd
import geoviews as gv
import holoviews as hv

class LISGageDataset:

    def __init__(self, header_file: str, gage_files: List[str]=None, **kwargs):
        idcol = kwargs.get( 'idcol', 0 )
        idtype = kwargs.get( 'idtype', str )
        geocols = kwargs.get( 'geom', dict( x=3, y=4 ) )
        datacols = kwargs.get( 'dcols', [] )
        usecols = [ idcol, geocols['x'], geocols['y'] ] + datacols
        self.header: pd.DataFrame = pd.read_csv( header_file, usecols=usecols, delim_whitespace=True, names=['id', 'lon', 'lat'], dtype={'id': idtype} )
        self._gage_data: List[pd.DataFrame] = []
        self.add_gage_files( gage_files )

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

    def gage_data_graph( self, index ):
        gage_data: pd.DataFrame = self._gage_data[index]
        print( f"Gage Data: {gage_data}" )

    def plot(self, **kwargs ):
        color = kwargs.pop( 'color', 'red' )
        size  = kwargs.pop( 'size', 10 )
        tools = kwargs.pop( 'tools', [ 'tap', 'hover' ] )
        pts_opts = gv.opts.Points( color=color, size=size, tools=tools, **kwargs )
        tiles = gv.tile_sources.EsriImagery()
        dpoints = hv.util.Dynamic( self.points.opts( pts_opts ) ).opts(height=400, width=600)
        select_stream = Selection1D( source=dpoints )
        line = hv.DynamicMap( self.gage_data_graph, streams=[select_stream] )
        pn.Row( tiles * dpoints, line ) # pn.Column(var_select, line))

    def add_gage_file( self, filepath: str ):
        gage_id = filepath.split('/')[-1].strip('.txt')
        df = pd.read_csv( filepath, names=['date', gage_id], delim_whitespace=True,  parse_dates=['date'], index_col='date' )
        self._gage_data.append( df )

    def add_gage_files(self, gage_file_paths: Optional[List[str]] ):
        if gage_file_paths is not None:
            for gage_file in gage_file_paths:
                self.add_gage_file( gage_file )

    @property
    def gage_data(self) -> pd.DataFrame:
        return pd.concat( self._gage_data, axis=1 )

    def plot_data( self, **kwargs ):
        lplot = self.gage_data.plot( figsize= kwargs.get( 'figsize', (12, 6) ) )
        lplot.figure.set_facecolor('yellow')
        return lplot