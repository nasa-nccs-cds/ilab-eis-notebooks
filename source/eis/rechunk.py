import xarray as xr
import os, time, numpy as np
from eis.smce import eis3, exception_handled
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
from zarr.storage import DirectoryStore
from rechunker import rechunk
from eis.smce import eis3


class Rechunker:

    def __init__( self, name: str, dset: xr.Dataset, **kwargs ):
        self.name = name
        self.dset: xr.Dataset = dset
        self.cache_dir = kwargs.pop( 'cache_dir', eis3().cache_dir )
        self.data_dir  = kwargs.get( 'data_dir',  eis3().data_dir  )

    def result_path(self):
        return f"{self.data_dir}/{self.name}.zarr"

    @classmethod
    def from_smce( cls, bucket: str, key: str, **kwargs ) -> "Rechunker":
        dset = eis3().get_zarr_dataset( bucket, key )
        name = key.replace("/",".")
        return Rechunker( name, dset, **kwargs  )

    @classmethod
    def from_disk( cls, zarr_dset_path: str, **kwargs ) -> "Rechunker":
        name = os.path.splitext( os.path.basename( zarr_dset_path ) )[0]
        data_dir = kwargs.pop( 'data_dir', os.path.dirname(zarr_dset_path) )
        dset = xr.open_zarr( zarr_dset_path, consolidated=True )
        return Rechunker( name, dset, data_dir=data_dir, **kwargs  )

    def rechunk( self, chunks: Dict, **kwargs ):
        max_memory =   kwargs.get( 'max_memory', "250MB" )
        target_store = kwargs.get( 'target_store', self.data_dir )
        temp_store =   kwargs.get( 'temp_store', self.cache_dir )
        var_chunks = {v: chunks for v in self.dset.data_vars}
        if isinstance( target_store, str ):
            target_store = f"{target_store}/{self.name}.zarr"
            print( f"Writing result to {target_store} with max-memory-per-worker set to {max_memory}" )
        if isinstance( temp_store, str):
            temp_store =  f"{temp_store}/{self.name}.zarr"
            print(f"Using temp_store: {temp_store} with chunks = {var_chunks}")
        return rechunk( self.dset, var_chunks, max_memory,  target_store=target_store, temp_store=temp_store, **kwargs )