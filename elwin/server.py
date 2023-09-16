import datetime
import itertools
import json
import time

import duckdb
import googlemaps
import numpy as np

from main import sensor_file, congestion_file, sql

api_key = "AIzaSyDMLKomC5ca7FS8WbH-7x9fn-i-Psvs8bU"


class Route:
    def __init__(self, path, duration, duration_traffic):
        self.path = path
        self.time = duration
        self.traffic_time = duration_traffic


def distance(a, b, c):
    return np.norm(np.cross(b - a, a - c)) / np.norm(b - a)


def extract_routes(data):
    out = []
    for alternative in data:
        route = []
        for step in alternative['legs'][0]['steps']:
            route.append((step['start_location']['lat'], step['start_location']['lng']))

        out.append(Route(
            route,
            alternative["legs"][0]["duration"]["value"],
            alternative["legs"][0]["duration_in_traffic"]["value"],
        ))

    return out


def sensor_points(route, start_time, end_time):
    path = ", ".join([f"ST_POINT({lat}, {lon})" for (lat, lon) in route.path])

    start_time = start_time.strftime('%Y-%M-%dT%H:%M:%S')
    end_time = end_time.strftime('%Y-%M-%dT%H:%M:%S')

    sql(f"""
        install spatial;
        load spatial;
        
            select ST_Point(Latitude, Longitude) as point, min(congestion.fraction)
            from {sensor_file} as sensor, {congestion_file} as congestion
            where ST_Distance(ST_MakeLine([{path}]), point) < 0.001
            and sensor.MSR_id = congestion.MSR_id
            and congestion.fraction < 0.7
            and congestion.time > strptime('{start_time}', '%Y-%M-%dT%H:%M:%S')
            and congestion.time < strptime('{end_time}', '%Y-%M-%dT%H:%M:%S')
            group by point
    """)


def estimate(start="Zürich, Affoltern", stop="Visp", departure_time=datetime.datetime.now(), mock=True):
    if mock:
        res = json.load(open("gmaps.json"))

    else:
        gmaps = googlemaps.Client(key=api_key)

        # Geocoding an address
        res = gmaps.directions(
            start,
            stop,
            mode="driving",
            departure_time=departure_time,
            alternatives=True,
        )

    routes = extract_routes(res)

    for route in routes:
        sensor_points(
            route,
            start_time=departure_time,
            end_time=departure_time + datetime.timedelta(seconds=route.time),
        )


def main():
    # sql("""
    # select * from congestion.parquet
    # """)

    estimate()


if __name__ == '__main__':
    main()
