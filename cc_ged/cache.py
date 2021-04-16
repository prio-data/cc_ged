
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import  ResourceNotFoundError

class NotCached(Exception):
    pass

class BlobCache():
    def __init__(self,connection_string,container_name):
        self.container_client = (BlobServiceClient
                .from_connection_string(connection_string)
                .get_container_client(container_name)
            )
    def __getitem__(self,key):
        try:
            blob_client = self.container_client.get_blob_client(key)
            value = blob_client.download_blob().content_as_bytes()
        except ResourceNotFoundError as rnf:
            raise NotCached from rnf
        return value
    def __setitem__(self,key,value):
        blob_client = self.container_client.get_blob_client(key)
        blob_client.upload_blob(value)

class Sigstring:
    def __init__(self,*args,**kwargs):
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        args = "_".join([str(a) for a in self.args])
        kwargs = "_".join([f"{k}+{v}" for k,v in self.kwargs.items()])
        return "&".join((args,kwargs))

    def sig(self):
        return self.args,self.kwargs

def cache(blob_cache):
    def wrapper(fn):
        def inner(*args,**kwargs):
            sig = Sigstring(args,kwargs)
            try:
                result = blob_cache[str(sig)]
            except NotCached:
                result = fn(*args,**kwargs)
                blob_cache[str(sig)] = result

            return result
        return inner
    return wrapper 
