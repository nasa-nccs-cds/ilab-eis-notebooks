from eis.lis.data.gage import LISGageDataset
from eis.lis.data.routing import LISRoutingData

class StreamflowAtGage:

    def __init__(self, gage_data: LISGageDataset, routing_data: LISRoutingData ):
        self.gages = gage_data
        self.routing = routing_data


    def plot(self, **kwargs):
        streamflow_var = 
        gage_map = self.gages.plot_map()
        self.routing.site_graph()