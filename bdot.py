#!/usr/bin/env python3
import asyncio
import logging
import zipfile
from dataclasses import dataclass
from pathlib import Path

import geojson
import geopandas
import h3
from httpx import AsyncClient
from starsep_utils import logDuration
from starsep_utils.overpass import DEFAULT_OVERPASS_URL
from tqdm import tqdm

H3_RESOLUTION = 12

missingDir = Path("missing")
missingDir.mkdir(exist_ok=True)
bdotDataDir = Path("bdot-data")
bdotDataDir.mkdir(exist_ok=True)


@dataclass(frozen=True)
class Theme:
    name: str
    overpassWayQuery: str
    bdotLayer: str


THEMES = [
    Theme(
        name="roads",
        overpassWayQuery='"highway"~"(service|primary|secondary|tertiary|motorway|residential|unclassified'
        '|living_street|trunk|trunk_link|primary_link|secondary_link|tertiary_link|motorway_link|pedestrian|track)"',
        bdotLayer="OT_SKJZ_L",
    ),
    Theme(
        name="noise_barriers",
        overpassWayQuery="wall=noise_barrier",
        bdotLayer="OT_OIKM_L",
        # TODO: Filter RODZAJ="ekran akustyczny" in BDOT
    ),
    Theme(
        name="powerlines",
        overpassWayQuery='power~"(line|minor_line)"',
        bdotLayer="OT_SULN_L",
    ),
    Theme(
        name="footways",
        overpassWayQuery='"highway"~"(footway|path|service|track|pedestrian)"',
        bdotLayer="OT_SKRP_L",
    ),
]


async def getOSMDataFromOverpass(theme: Theme, teryt: str):
    query = f"""
    [out:json][timeout:25];
    area["teryt:terc"="{teryt}"]->.searchArea;
    way[{theme.overpassWayQuery}](area.searchArea);
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


async def getBdotData(theme: Theme, teryt: str):
    with logDuration("reading BDOT data in GeoPackage format"):
        bdotData = geopandas.read_file(
            list(bdotDataDir.glob(f"*.BDOT10k.{teryt}__{theme.bdotLayer}.gpkg"))[0]
        )
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


async def getOSMData(theme: Theme, teryt: str):
    osmData = await getOSMDataFromOverpass(theme, teryt)
    return processOSMDataIntoH3Set(osmData)


async def processTheme(theme: Theme, teryt: str):
    outputFile = missingDir / f"{theme.name}-{teryt}.geojson"
    if outputFile.exists():
        return

    [osmH3Set, geojsonBdotData] = await asyncio.gather(
        getOSMData(theme, teryt), getBdotData(theme, teryt)
    )

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
        with outputFile.open("w") as f:
            geojson.dump(geojson.FeatureCollection(outputFeatures), f)


async def downloadBdot(teryt: str):
    url = f"https://opendata.geoportal.gov.pl/bdot10k/schemat2021/GPKG/{teryt[:2]}/{teryt}_GPKG.zip"
    file = bdotDataDir / f"{teryt}_GPKG.zip"
    if not file.exists():
        logging.info(f"Downloading {url}")
        async with AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
            with file.open("wb") as f:
                f.write(response.content)
        with zipfile.ZipFile(file) as z:
            z.extractall(bdotDataDir)
    else:
        logging.info(f"File {file} already exists")


async def main():
    terytCodes = {
        "Warszawa": "1465",
        "Gdańsk": "2261",
        "Kraków": "1261",
        "Tczew": "2214",
        "Inowrocław": "0407",
        "Starachowice": "2611",
        "Żyrardów": "1438",
        "Kutno": "1002",
    }
    for teryt in tqdm(terytCodes.values()):
        await downloadBdot(teryt)
        for theme in tqdm(THEMES):
            await processTheme(theme, teryt)
    with Path("index.html").open("w") as f:
        for name, teryt in terytCodes.items():
            for theme in THEMES:
                outputFile = missingDir / f"{theme.name}-{teryt}.geojson"
                f.write(f"<a href='./{outputFile}' download>{name} {theme.name}</a><br/>\n")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    asyncio.run(main())
