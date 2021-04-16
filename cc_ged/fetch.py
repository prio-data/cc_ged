
from datetime import date
import requests
from . import config,cache

@cache.cache
def get_ged(country: int,start_date: date,end_date: date):
    
    return data
