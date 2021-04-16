
from fastapi import FastAPI
from . import config,fetch,cache,quarters

app = FastAPI()
print(config.config("DB_HOST"))

@cache.cache(cache.BlobCache)
def ged_points(country:int,year:int,quarter:int):
    """
    Returns GED points as geopandas
    """
    #TODO fetch and return
    start_date,end_date = quarters.get_quarter_bounds(year,quarter)
    data = fetch.get_ged(country,start_date,end_date)
    return data

@cache.cache(cache.BlobCache)
def ged_buffer(country:int,year:int,quarter:int):
    points = ged_points(country,year,quarter)
    return data

@app.get("/{country:int}/{year:int}/{quarter:int}/{resource:str}")
def handle(country,year,quarter,resource):
    return f"You wanted {country} {year} {quarter} {resource}"
