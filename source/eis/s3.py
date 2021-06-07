import fnmatch, s3fs
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import glob, os
from collections.abc import MutableMapping
from .base import EISSingleton

def s3m(): return S3Manager.instance()
def has_char(string: str, chars: str): return 1 in [c in string for c in chars]

class S3Manager(EISSingleton):

    def __init__( self, **kwargs ):
        EISSingleton.__init__( self, **kwargs )
        self._fs: s3fs.S3FileSystem = None

    @property
    def fs(self) -> s3fs.S3FileSystem:
        if self._fs is None:
            self._fs = s3fs.S3FileSystem( anon=False, s3_additional_kwargs=dict( ACL="bucket-owner-full-control" ) )
        return self._fs

    def item_key(self, path: str) -> str:
        return path.split(":")[-1].strip("/")

    def parse(self, urlpath: str) -> Tuple[str, str]:
        ptoks = urlpath.split(":")[-1].strip("/").split("/")
        return ( ptoks[0], "/".join( ptoks[1:] ) )

    def get_store(self, path: str, mode: str ) -> MutableMapping:
        create: bool = (mode == "w")
        mapper = self.fs.get_mapper( path, create=create )
        if create: mapper.clear()
        return mapper




