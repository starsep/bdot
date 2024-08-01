import asyncio
import logging

import geojson
import geopandas
import h3
from httpx import AsyncClient
from starsep_utils import logDuration
from starsep_utils.overpass import DEFAULT_OVERPASS_URL

H3_RESOLUTION = 12


async def getOSMDataFromOverpass():
    areaName = "Warszawa"
    query = f"""
    [out:json][timeout:25];
    area["name"="{areaName}"]->.searchArea;
    way["highway"~"(footway|path|service|track)"](area.searchArea);
    convert item ::=::,::geom=geom(),_osm_type=type();
    out geom;
    """
    with logDuration("download data from Overpass"):
        response = await AsyncClient().post(
            DEFAULT_OVERPASS_URL, data=dict(data=query), timeout=30.0
        )
        response.raise_for_status()
    with logDuration("parsing Overpass response"):
        return geojson.loads(response.text)["elements"]


def h3LineLatLng(start: tuple[float, float], end: tuple[float, float]) -> set[str]:
    startH3 = h3.geo_to_h3(start[1], start[0], H3_RESOLUTION)
    endH3 = h3.geo_to_h3(end[1], end[0], H3_RESOLUTION)
    if startH3 == endH3 or h3.h3_distance(startH3, endH3) == 1:
        return {startH3, endH3}
    middle = ((start[0] + end[0]) / 2, (start[1] + end[1]) / 2)
    return h3LineLatLng(start, middle) | h3LineLatLng(middle, end)


def processLineIntoH3Set(
    line: list[tuple[float, float]], result: set[str], neighbourhood_size: int = 0
) -> set[str]:
    for pointA, pointB in zip(line[:-1], line[1:]):
        for point in h3LineLatLng(pointA, pointB):
            result.update(h3.k_ring(point, neighbourhood_size))
    return result


@logDuration
def processOSMDataIntoH3Set(osmData) -> set[str]:
    result = set()
    for element in osmData:
        if element["geometry"]["type"] != "LineString":
            print(f'Unsupported geometry type {element["geometry"]["type"]}')
            continue
        coords = element["geometry"]["coordinates"]
        result = processLineIntoH3Set(coords, result, neighbourhood_size=1)
    return result


async def getBdotData():
    with logDuration("reading BDOT data in GeoPackage format"):
        bdotData = geopandas.read_file("PL.PZGiK.330.BDOT10k.1465__OT_SKRP_L.gpkg")
    bdotData = bdotData.drop(
        columns=[
            "WERSJA",
            "POCZATEKWERSJIOBIEKTU",
            "PRZESTRZENNAZW",
            "LOKALNYID",
            "KATEGORIAISTNIENIA",
            "KODKARTO10K",
            "TERYT",
            "OZNACZENIEZMIANY",
        ]
    )
    with logDuration("converting BDOT data to GeoJSON"):
        geojsonBdotDataString = bdotData.to_json(to_wgs84=True)
    with logDuration("parsing BDOT GeoJSON"):
        geojsonBdotData = geojson.loads(geojsonBdotDataString)
    return geojsonBdotData


async def getOSMData():
    osmData = await getOSMDataFromOverpass()
    return processOSMDataIntoH3Set(osmData)


async def main():
    [osmH3Set, geojsonBdotData] = await asyncio.gather(getOSMData(), getBdotData())

    outputFeatures = []
    for feature in geojsonBdotData["features"]:
        if feature["geometry"]["type"] != "LineString":
            print(f'Unsupported geometry type {feature["geometry"]["type"]}')
            continue
        coords = feature["geometry"]["coordinates"]
        h3SetFeature = processLineIntoH3Set(coords, set(), neighbourhood_size=0)
        shared = h3SetFeature & osmH3Set
        if len(shared) == 0:
            outputFeatures.append(feature)
    with logDuration("writing missing features to GeoJSON"):
        with open("missing.geojson", "w") as f:
            geojson.dump(geojson.FeatureCollection(outputFeatures), f)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
