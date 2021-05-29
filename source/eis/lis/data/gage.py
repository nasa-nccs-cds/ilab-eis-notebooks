import pandas as pd
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import matplotlib.pyplot as plt
from matplotlib.axes import SubplotBase
import geopandas as gpd
import geoviews as gv

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

    def plot_gage_map(self):
        kdims = ['lon', 'lat']
        pts_opts = gv.opts.Points(width=600, height=600, color='red', size=10, tools=['hover'])
        pts = gv.Points( self.header, kdims=kdims, vdims='id').opts( pts_opts )
        tiles = gv.tile_sources.EsriImagery()
        return tiles * pts

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

    def plot_gage_data( self, **kwargs ):
        lplot: SubplotBase = self.gage_data.plot( figsize= kwargs.get( 'figsize', (12, 6) ) )
        print( lplot.__class__ )
#        fig: plt.Figure = lplot.
#        fig.set_facecolor('yellow')
        return lplot