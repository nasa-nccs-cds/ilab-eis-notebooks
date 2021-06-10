import pandas as pd
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
from holoviews.streams import Selection1D, Params, Tap
import panel as pn
import xarray as xa
import geopandas as gpd
from eis.lis.data.routing import LISRoutingData
from eis.lis.data.gage import LISGageDataset
import numpy as np
import geoviews as gv
import holoviews as hv
from eis.smce import eis3, exception_handled
import hvplot.pandas
import hvplot.xarray

class LISCombinedDataset:

    def __init__(self, gage_data: LISGageDataset, routing_data: LISRoutingData, **kwargs):
        self.gage_data = gage_data
        self.routing_data = routing_data
        self.init_null_data( **kwargs )

    def init_null_data(self, **kwargs ):
        gage_index: int = kwargs.get( 'gage_index', 0 )
        vname = kwargs.get( 'varname', self.routing_data.var_names[0] )
        (routing_adata, gage_adata) = self.get_aligned_data( gage_index, vname )
        self._null_routing_data = xa.zeros_like( routing_adata )
        self._null_gage_data = xa.zeros_like(gage_adata)

    def get_aligned_data(self, gage_index: int, vname: str  ) -> Tuple[xa.DataArray, ...]:
        coords = self.gage_data.getCoords( gage_index )
        streamflow_data: xa.DataArray = self.routing_data.var_data( vname, coords['lon'], coords['lat'] )
        gage_data: xa.DataArray = self.gage_data.xa_gage_data( gage_index )
        return xa.align( streamflow_data, gage_data )

    @exception_handled
    def routing_data_graph( self, index: List[int], vname: str ):
        logger = eis3().get_logger()
        if (index is None) or (len(index) == 0):
            return self._null_routing_data.hvplot(title=f"No Gages")
        gage_index = index[0]
        (routing_adata, gage_adata) = self.get_aligned_data( gage_index, vname )
        return routing_adata.hvplot( title = vname )

    @exception_handled
    def gage_data_graph(self, index: List[int], vname: str):
        logger = eis3().get_logger()
        if (index is None) or (len(index) == 0):
            return self._null_gage_data.hvplot(title=f"No Gages")
        else:
            gage_index = index[0]
            (routing_adata, gage_adata) = self.get_aligned_data( gage_index, vname )
            return gage_adata.hvplot(title=f"Gage[{gage_index}]")

    @exception_handled
    def plot(self, **kwargs ):
        color = kwargs.pop( 'color', 'red' )
        size  = kwargs.pop( 'size', 10 )
        tools = kwargs.pop( 'tools', [ 'tap', 'hover' ] )
        overlay = kwargs.get( 'overlay', False )
        var_select = pn.widgets.Select(options=self.routing_data.var_names, value='Streamflow_tavg', name="LIS Variable List")
        var_stream = Params( var_select, ['value'], rename={ 'value': 'vname' } )
        pts_opts = gv.opts.Points( color=color, size=size, tools=tools, nonselection_fill_alpha=0.2, nonselection_line_alpha=0.6,  **kwargs )
        tiles = gv.tile_sources.EsriImagery()
        dpoints = hv.util.Dynamic( self.gage_data.points.opts( pts_opts ) ).opts(height=400, width=600)
        select_stream = Selection1D( default=[0], source=dpoints )
        routing_graph = hv.DynamicMap( self.routing_data_graph, streams=[select_stream, var_stream ])
        gage_graph = hv.DynamicMap( self.gage_data_graph, streams=[select_stream, var_stream] )
        graphs = gage_graph * routing_graph if overlay else pn.Column( gage_graph, routing_graph )
        return pn.Row( pn.Column( var_select, tiles * dpoints ), graphs )

