from eis.lis.data.gage import LISGageDataset
from eis.lis.data.routing import LISRoutingData
import holoviews as hv, geoviews as gv

class StreamflowAtGage:

    def __init__(self, gage_data: LISGageDataset, routing_data: LISRoutingData ):
        self.gages = gage_data
        self.routing = routing_data

    def plot(self, **kwargs):
        points: gv.Points = self.gages.points
        image = gv.tile_sources.EsriImagery()
        routing_data = self.routing.dynamic_map( ).select( vname=kwargs.get('vname') )
        map = image * points

        return map