import numpy as np
import pandas as pd
import geopandas
import xarray as xr
import fsspec
from datashader.utils import lnglat_to_meters
from datashader.colors import viridis
import datashader
from holoviews.operation.datashader import datashade, shade, dynspread, spread, rasterize
import holoviews as hv, geoviews as gv
from geoviews import opts
from geoviews import tile_sources as gvts
from holoviews.streams import Selection1D, Params
import panel as pn
import hvplot.pandas
import hvplot.xarray
from eis.smce import eis3
from scipy.spatial import distance
gv.extension('bokeh')

class LISSurfaceData:

    def __init__( self, dset: xr.Dataset, **kwargs ):
        self.dset = dset

    @classmethod
    def from_smce( cls, bucket: str, key: str ) -> "LISSurfaceData":
        dset = eis3().get_zarr_dataset(bucket, key)
        return LISSurfaceData( dset )

    def line_callback(self, index, vname, ts_tag, te_tag):
        time_tag = '2013-02'
        if not index:
            title = 'Var: -- Lon: -- Lat: --'
            return self.dset[vname].isel(north_south=1682, east_west=2).sel(time=time_tag).hvplot(title=title)

        first_index = index[0]
        row = sites.iloc[first_index]

        ix, iy = self.nearest_grid( (row.lon, row.lat) )
        # vals = ds_ol[vname].isel(north_south=iy, east_west=ix).sel(time=time_tag).load()
        vals = var_subset(vname, ix, iy, ts_tag, te_tag)

        vs = vname.split('_')[0]
        title = f'Var: {vs} Lon: {row.lon} Lat: {row.lat}'

        return vals.hvplot(title=title)

    def nearest_grid( self, pt ):
        # pt : input point, tuple (longtitude, latitude)
        # output:
        #        x_idx, y_idx
        loc_valid = df_loc.dropna()
        pts = loc_valid[['lon', 'lat']].to_numpy()
        idx = distance.cdist([pt], pts).argmin()