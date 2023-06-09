import dataclasses
import math
from typing import Union

from orjson import orjson

@dataclasses.dataclass(frozen=True, eq=True)
class Location:
    lat: float
    lng: float

    def __getitem__(self, index):
        return self.lat if index == 0 else self.lng

    def to_json(self) -> bytes:
        return orjson.dumps(self)

    def __str__(self) -> str:
        return f"{self.lat}, {self.lng}"

    @staticmethod
    def from_json(json_str: Union[bytes, str]):
        raw = orjson.loads(json_str)
        if isinstance(raw, list):
            return Location(raw[0], raw[1])
        elif isinstance(raw, dict):
            lat = raw.get("lat", 0.0)
            lng = raw.get("lng", 0.0)
            return Location(lat, lng)
        else:
            return None

    def get_distance_from_in_meters(self, dest_lat: float, dest_lon: float):
        # approximate radius of earth in km
        earth_radius = 6373.0

        lat1 = math.radians(self.lat)
        lon1 = math.radians(self.lng)
        lat2 = math.radians(dest_lat)
        lon2 = math.radians(dest_lon)

        dlon = lon2 - lon1
        dlat = lat2 - lat1

        angle = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        circ = 2 * math.atan2(math.sqrt(angle), math.sqrt(1 - angle))

        distance = earth_radius * circ

        return distance * 1000

    @staticmethod
    def calculate_cooldown(distance, speed):
        if distance >= 1335000:
            speed = 180.43  # Speed can be abt 650 km/h
        elif distance >= 1100000:
            speed = 176.2820513
        elif distance >= 1020000:
            speed = 168.3168317
        elif distance >= 1007000:
            speed = 171.2585034
        elif distance >= 948000:
            speed = 166.3157895
        elif distance >= 900000:
            speed = 164.8351648
        elif distance >= 897000:
            speed = 166.1111111
        elif distance >= 839000:
            speed = 158.9015152
        elif distance >= 802000:
            speed = 159.1269841
        elif distance >= 751000:
            speed = 152.6422764
        elif distance >= 700000:
            speed = 151.5151515
        elif distance >= 650000:
            speed = 146.3963964
        elif distance >= 600000:
            speed = 142.8571429
        elif distance >= 550000:
            speed = 138.8888889
        elif distance >= 500000:
            speed = 134.4086022
        elif distance >= 450000:
            speed = 129.3103448
        elif distance >= 400000:
            speed = 123.4567901
        elif distance >= 350000:
            speed = 116.6666667
        elif distance >= 328000:
            speed = 113.8888889
        elif distance >= 300000:
            speed = 108.6956522
        elif distance >= 250000:
            speed = 101.6260163
        elif distance >= 201000:
            speed = 90.54054054
        elif distance >= 175000:
            speed = 85.78431373
        elif distance >= 150000:
            speed = 78.125
        elif distance >= 125000:
            speed = 71.83908046
        elif distance >= 100000:
            speed = 64.1025641
        elif distance >= 90000:
            speed = 60
        elif distance >= 80000:
            speed = 55.55555556
        elif distance >= 70000:
            speed = 50.72463768
        elif distance >= 60000:
            speed = 47.61904762
        elif distance >= 45000:
            speed = 39.47368421
        elif distance >= 40000:
            speed = 35.0877193
        elif distance >= 35000:
            speed = 32.40740741
        elif distance >= 30000:
            speed = 29.41176471
        elif distance >= 25000:
            speed = 27.77777778
        elif distance >= 20000:
            speed = 27.77777778
        elif distance >= 15000:
            speed = 27.77777778
        elif distance >= 10000:
            speed = 23.80952381
        elif distance >= 8000:
            speed = 26.66666667
        elif distance >= 5000:
            speed = 22.34137623
        elif distance >= 4000:
            speed = 22.22222222
        delay_used = distance / speed
        if delay_used > 7200:  # There's a maximum of 2 hours wait time
            delay_used = 7200
        return delay_used
