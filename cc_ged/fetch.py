from datetime import date
import time
import random
import logging
from urllib.parse import urlencode

import requests
from dateutils import relativedelta
from . import spatial

logger = logging.getLogger(__name__)

def ged_candidate_url(year,month):
    """
    Returns the URL corresponding to a year-month pair
    """
    GED_API_URL = "https://ucdpapi.pcr.uu.se/api/gedevents/{version}"
    version = f"{str(year)[-2:]}.0.{month}"
    return GED_API_URL.format(version=version)

def fetch_ged_events(country: int, year:int, month:int, page_size=10,politeness=.5):
    """
    Get all GED events for a single month from the UCDP GED api 
    """
    start_date = date(year=year,month=month,day=1)
    end_date = (start_date + relativedelta(months=1))-relativedelta(days=1)

    logger.info("Fetching %s for %s",f"{str(year)[-2:]}.0.{month}",country)

    params = {
            "StartDate":str(start_date),
            "EndDate":str(end_date),
            "Country":country,
            "pagesize":page_size,
        }

    next_url = ged_candidate_url(end_date.year,end_date.month)+"?"+urlencode(params)

    events = []
    while next_url != "":
        logger.info("Fetching %s",next_url)
        response = requests.get(next_url)

        try:
            assert response.status_code == 200
        except AssertionError as ae:
            raise requests.HTTPError(response=response) from ae

        data = response.json()

        try:
            events+=data["Result"]
            logger.info("Retrieved %s events",len(data["Result"]))
            next_url = data["NextPageUrl"]
        except KeyError as ke:
            raise ValueError("Request returned malformed data:"
                    f"{next_url}: {str(data)}"
                    ) from ke
                
        time.sleep(politeness*random.random())
    return events 

def get_ged_geojson(*args,**kwargs):
    rows = fetch_ged_events(*args,**kwargs)
    return {
        "type":"FeatureCollection",
        "features":[spatial.ged_to_geojson(r) for r in rows]
    }
