
import sys
import json
from datetime import date
import time
import random
import logging
from urllib.parse import urlencode

import requests
from dateutils import relativedelta

logger = logging.getLogger(__name__)

def ged_candidate_url(year,month):
    GED_API_URL = "https://ucdpapi.pcr.uu.se/api/gedevents/{version}"
    version = f"{str(year)[-2:]}.0.{month}"
    return GED_API_URL.format(version=version)

def ged_to_geojson(row):
    WANTED_PROPS = ["best","high","low","country_id","date_start","date_end",
                    "side_a","side_a_new_id","side_b","side_b_new_id","dyad_new_id",
                    "conflict_new_id","type_of_violence"
                ]

    props = {k:v for k,v in row.items() if k in WANTED_PROPS}
    return {
        "type":"Feature",
        "geometry":{
                "type":"Point",
                "coordinates":[
                        row["latitude"],
                        row["longitude"]
                    ]
            },
        "properties":props,
        "id":row["id"]
        }

def fetch_ged_events(country: int, year:int, month:int, page_size=10,politeness=.5):
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
        "features":[ged_to_geojson(r) for r in rows]
    }

if __name__ == "__main__":
    ctry,year,month = sys.argv[1:]
    logging.basicConfig(level=logging.DEBUG)
    try:
        fc = get_ged_geojson(ctry,int(year),int(month))
    except requests.HTTPError as httpe:
        print(httpe.response.content)
    else:
        print(json.dumps(fc,indent=4))
