

class EISSingleton:

    _instance: "EISSingleton" = None


    @classmethod
    def instance( cls, *args, **kwargs ) -> "EISSingleton":
        if cls._instance is None:
            cls._instance =  EISSingleton.__init__( *args, **kwargs )
        return cls._instance