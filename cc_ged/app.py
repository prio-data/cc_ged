
import re
import importlib
from fastapi import FastAPI,Response
from bs4 import BeautifulSoup
from . import config,fetch,cache,spatial,error_handling

app = FastAPI()

blob_cache = cache.BlobCache(
        config.config("BLOB_STORAGE_CONNECTION_STRING"),
        config.config("GED_CACHE_CONTAINER_NAME")
    )

@app.get("/{country:int}/{year:int}/{month:int}/points")
@error_handling.proxy_http_err
@cache.cache(blob_cache,use_json=True)
def ged_points(country:int,year:int,month:int):
    """
    Returns GED points for year - month as GeoJSON
    """
    try:
        assert month <= 12 and month >= 1
    except AssertionError:
        return Response("Month must be a number between 1 and 12!",status_code=400)
    features = spatial.response_to_geojson(fetch.fetch_ged_events(country,year,month))
    return {
        "type": "FeatureCollection",
        "features": [*features]
    }

@app.get("/{country:int}/{year:int}/{month:int}/buffered/{buffer:int}")
@error_handling.proxy_http_err
@cache.cache(blob_cache,use_json=True)
def ged_buffered(country,year,month,buffer):
    return spatial.buffer(ged_points(country,year,month),meters=buffer)

@app.get("/map/{data_path:path}")
def show_map(data_path:str):
    module_name = re.search(r"^[^\.]+",__name__)[0]
    html = importlib.resources.read_text(module_name,"map.html")
    soup = BeautifulSoup(html,"html.parser")
    data_inject = soup.find("script",id="data-url")
    data_inject.string = f"const url = '/{data_path}'"
    return Response(soup.prettify(),media_type="text/html")
