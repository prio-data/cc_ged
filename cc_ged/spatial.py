import io
import json
from functools import partial
import shapely
import pyproj
from pyproj.enums import TransformDirection as TD
import utm_zone
 
PROJ = {
    "WGS84":"+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs",
}

def ged_to_geojson(row):
    """
    Transform GED rows to a GeoJSON FeatureCollection of points
    """

    WANTED_PROPS = ["best","high","low","country_id","date_start","date_end",
                    "side_a","side_a_new_id","side_b","side_b_new_id","dyad_new_id",
                    "conflict_new_id","type_of_violence"
                ]

    props = {k:v for k,v in row.items() if k in WANTED_PROPS}
    lat,lon = (row[k] for k in ("latitude","longitude"))

    return {
        "type":"Feature",
        "geometry":{
                "type":"Point",
                "coordinates":[
                        lon,
                        lat 
                    ]
            },
        "properties":props,
        "id":row["id"]
        }

response_to_geojson = partial(map,ged_to_geojson)



def buffer(geojson,meters=50000):
    """
    Return a FeatureCollection with buffered points.
    """

    for idx,feature in enumerate(geojson["features"]):
        utm = utm_zone.proj(feature)
        utm_transformer = pyproj.Transformer.from_proj(PROJ["WGS84"],utm)

        f,i = [partial(utm_transformer.transform,direction=d) for d in [TD.FORWARD,TD.INVERSE]]

        point = shapely.geometry.Point(feature["geometry"]["coordinates"])
        utm_point = shapely.ops.transform(f,point)
        buffered = utm_point.buffer(meters)
        buffered = shapely.ops.transform(i,buffered)

        geojson["features"][idx]["geometry"] = shapely.geometry.mapping(buffered)

    return geojson 
