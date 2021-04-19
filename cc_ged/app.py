import logging
import json
from fastapi import FastAPI,Response
from functools import reduce
import requests
from . import config,fetch,cache,quarters

app = FastAPI()

blob_cache = cache.BlobCache(
        config.config("STORAGE_CONNECTION_STRING"),
        config.config("GED_CACHE_CONTAINER_NAME")
    )

@cache.cache(blob_cache,use_json=True)
def ged_points(country:int,year:int,quarter:int):
    """
    Returns GED points as geopandas
    """
    start,end = quarters.month_quarter_bounds(quarter) 
    data = []
    for m in range(start,end+1):
        data.append(fetch.get_ged_geojson(country,year,m))

    def join_fc(a,b):
        a["features"]+=b["features"]
        return a
    fc = reduce(join_fc, data)
    return fc

handlers = {
        "points":ged_points
    }

"""
@cache.cache(cache.BlobCache)
def ged_buffer(country:int,year:int,quarter:int):
    points = ged_points(country,year,quarter)
    return spatial.buffer(data)
"""

@app.get("/{country:int}/{year:int}/{quarter:int}/{resource}")
def handle(country:int,year:int,quarter:int,resource:str):
    try:
        return handlers[resource](country,year,quarter)
    except requests.HTTPError as httpe:
        return Response(f"Request to {httpe.response.url} " 
                f"returned {httpe.response.status_code}: \"{httpe.response.content}\"",
                status_code=500)

