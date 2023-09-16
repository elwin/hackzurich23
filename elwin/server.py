import dataclasses
import datetime
import itertools
import json
import os
import time
import flask
import duckdb
import googlemaps
import numpy as np
from flask import Flask, request

from main import sensor_file, congestion_file, sql

api_key = os.getenv("GOOGLE_API_KEY")
production = os.getenv("PRODUCTION") != ""

app = Flask(__name__)
gmaps = googlemaps.Client(key=api_key)


def get_arg(key):
    return request.args.get(key)


@app.route("/estimate")
def estimate_endpoint():
    start = get_arg('origin')
    stop = get_arg('destination')
    departure_time = get_arg('departure_time')

    departure_time = datetime.datetime.strptime(departure_time, "%Y-%m-%d %H:%M:%S")

    return estimate(start, stop, departure_time)


@dataclasses.dataclass
class Route:
    traffic_score: int
    overview_polyline: str

    def __init__(self, path, duration, duration_traffic, overview_polyline):
        self.path = path
        self.time = duration
        self.traffic_time = duration_traffic
        self.overview_polyline = overview_polyline

    def dict(self):
        traffic_delay = abs(self.traffic_time - self.time) * self.traffic_score * 10
        return {
            "traffic_score": self.traffic_score,
            "overview_polyline": self.overview_polyline,
            "estimated_traffic_delay": int(traffic_delay),
            "total_time": traffic_delay + self.time,
        }


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
            overview_polyline=alternative['overview_polyline']['points']
        ))

    return out


def get_traffic_score(route, start_time=None, end_time=None):
    path = ", ".join([f"ST_POINT({lat}, {lon})" for (lat, lon) in route.path])

    if start_time is None and end_time is None:
        time_query = ""
    else:
        start_time = start_time.strftime('%Y-%M-%dT%H:%M:%S')
        end_time = end_time.strftime('%Y-%M-%dT%H:%M:%S')

        time_query = f"""
        and congestion.time > strptime('{start_time}', '%Y-%M-%dT%H:%M:%S')
        and congestion.time < strptime('{end_time}', '%Y-%M-%dT%H:%M:%S')
        """

    congestions = duckdb.sql(f"""
            install spatial;
            load spatial;

            select ST_Point(Latitude, Longitude) as point, min(congestion.fraction)
            from {sensor_file} as sensor, {congestion_file} as congestion
            where ST_Distance(ST_MakeLine([{path}]), point) < 0.001
            and sensor.MSR_id = congestion.MSR_id
            and congestion.fraction < 0.7
            {time_query}
            group by point
        """)

    x = congestions.count("point").df()

    return int(x['count(point)'][0])


def sensor_points(route, start_time, end_time):
    max_score = get_traffic_score(route)
    cur_score = get_traffic_score(route, start_time, end_time)

    return cur_score / max_score


def estimate(start="Zürich, Affoltern", stop="Visp", departure_time=datetime.datetime.now(), mock=False):
    if mock:
        res = json.load(open("gmaps.json"))

    else:

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
        traffic_score = sensor_points(
            route,
            start_time=departure_time,
            end_time=departure_time + datetime.timedelta(seconds=route.time),
        )
        route.traffic_score = traffic_score

    return [route.dict() for route in routes]


def main():
    # sql("""
    # select * from congestion.parquet
    # """)

    estimate()


if __name__ == '__main__':
    # main()
    if production:
        duckdb.sql("SET home_directory='/'")

    app.run(port=8000, host="0.0.0.0")
