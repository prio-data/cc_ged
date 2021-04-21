
import re
import importlib
from functools import reduce
from operator import add
from fastapi import FastAPI,Response
from bs4 import BeautifulSoup
from . import config,fetch,cache,quarters,spatial

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
        data.append(fetch.fetch_ged_events(country,year,m))

    try:
        features = reduce(add,
                [list(f) for f in map(spatial.response_to_geojson,data)]
            )
    except KeyError as ke:
        return Response(f"Malformed data from GED API: {ke}",status_code=500)

    return {
        "type": "FeatureCollection",
        "features": features
    }

@cache.cache(blob_cache,use_json=True)
def ged_buffered(country:int,year:int,quarter:int,buffer:int):
    return spatial.buffer(ged_points(country,year,quarter),meters=buffer)

@app.get("/{country:int}/{year:int}/{quarter:int}/points")
def handle_points(country:int,year:int,quarter:int):
    """
    Returns GED events as points
    """
    return ged_points(country,year,quarter)

@app.get("/{country:int}/{year:int}/{quarter:int}/buffered/{buffer}")
def handle_buffered(country:int,year:int,quarter:int,buffer:int):
    """
    Returns GED events as polygon-circles, buffered {buffer} meters around each point.
    """
    return ged_buffered(country,year,quarter,buffer)

@app.get("/map/{data_path:path}")
def show_map(data_path:str):
    module_name = re.search("^[^\.]+",__name__)[0]
    html = importlib.resources.read_text(module_name,"map.html")
    soup = BeautifulSoup(html,"html.parser")
    data_inject = soup.find("script",id="data-url")
    data_inject.string = f"const url = '/{data_path}'"
    return Response(soup.prettify(),media_type="text/html")
