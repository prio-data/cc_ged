
import json
import pickle
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import  ResourceNotFoundError

class NotCached(Exception):
    pass

class BlobCache():
    def __init__(self,connection_string,container_name):
        self.connection_string = connection_string
        self.container_name = container_name

    def client(self):
        return (BlobServiceClient
                .from_connection_string(self.connection_string)
                .get_container_client(self.container_name)
            )


    def __getitem__(self,key):
        try:
            blob_client = self.client().get_blob_client(key)
            value = blob_client.download_blob().content_as_bytes()
        except ResourceNotFoundError as rnf:
            raise NotCached from rnf
        return value

    def __setitem__(self,key,value):
        blob_client = self.client().get_blob_client(key)
        blob_client.upload_blob(value)

class Sigstring:
    def __init__(self,*args,**kwargs):
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        args = "_".join([str(a) for a in self.args])
        kwargs = "_".join([f"{k}-{v}" for k,v in self.kwargs.items()])
        return "~".join((args,kwargs))

    def sig(self):
        return self.args,self.kwargs

def cache(blob_cache,use_json = True):

    if use_json: 
        serialize,deserialize = json.dumps,json.loads
    else:
        serialize,deserialize = pickle.dumps,pickle.loads

    def wrapper(fn):
        key = lambda *args,**kwargs: fn.__name__ + str(Sigstring(*args,**kwargs))
        def inner(*args,**kwargs):
            try:
                result = deserialize(blob_cache[key(*args,**kwargs)])
            except NotCached:
                result = fn(*args,**kwargs)
                blob_cache[key(*args,**kwargs)] = serialize(result)
            return result
        return inner
    return wrapper 
