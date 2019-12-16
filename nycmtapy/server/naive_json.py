from aiohttp import web
import functools
from pathlib import Path
from nycmtapy import load_one_msg, generate_key, get_realtime_feeds
from nycmtapy.nyct_subway_pb2 import nyct_trip_descriptor, NyctTripDescriptor
import zipfile
import csv

DIR_MAP = dict(_[::-1] for _ in NyctTripDescriptor.Direction.items())


async def hello(request):
    return web.Response(text="Hello, world")


async def echo_handler(request):
    return web.json_response(dict(request.query))


async def latest_handler(request, data_path):
    data_path = Path(data_path)
    out = {}
    out["timestamp"] = {}
    out["trains"] = {}
    out["updates"] = {}
    for feed in get_realtime_feeds():
        if "mnr" in feed or "lirr" in feed:
            # commuter rail feeds are structured differently
            continue
        latest = sorted(
            data_path.glob(f"{feed}*pb"), key=lambda f: f.lstat().st_ctime, reverse=True
        )[0]
        with open(latest, "rb") as fin:
            m = load_one_msg(fin.read())
        out["timestamp"][feed] = m.header.timestamp
        for entry in m.entity:
            if entry.HasField("vehicle"):
                vehic = entry.vehicle
                e_dir = DIR_MAP[vehic.trip.Extensions[nyct_trip_descriptor].direction]
                key = generate_key(vehic.trip)
                out["trains"]["_".join(str(_) for _ in key)] = {
                    "key": key,
                    "stop_id": vehic.stop_id,
                    "current_stop_sequence": vehic.current_stop_sequence,
                    "direction": e_dir,
                    "route_id": vehic.trip.route_id,
                    "feed": feed,
                }

    return web.json_response(out)


async def stop_names(request, data_path):
    data_path = Path(data_path)
    latest = sorted(
        data_path.glob("mta_static*zip"), key=lambda f: f.lstat().st_ctime, reverse=True
    )[0]
    out = {}
    with zipfile.ZipFile(latest, "r") as zin:
        with zin.open("stops.txt") as stops:
            g = csv.reader(_.decode() for _ in stops.readlines())
            h = next(g)
            for row in g:
                data = {k: v for k, v in zip(h, row)}
                out[data["stop_id"]] = data["stop_name"]

    return web.json_response(out)


async def routes(request, data_path):
    data_path = Path(data_path)
    latest = sorted(
        data_path.glob("mta_static*zip"), key=lambda f: f.lstat().st_ctime, reverse=True
    )[0]
    out = {}
    with zipfile.ZipFile(latest, "r") as zin:
        with zin.open("routes.txt") as stops:
            g = csv.reader(_.decode() for _ in stops.readlines())
            h = next(g)
            for row in g:
                data = {k: v for k, v in zip(h, row)}
                out[data["route_id"]] = data

    return web.json_response(out)


def setup_routes(app, data_path):
    app.add_routes(
        [
            web.get("/", hello),
            web.get("/echo", echo_handler),
            web.get("/latest", functools.partial(latest_handler, data_path=data_path)),
            web.get("/stop_names", functools.partial(stop_names, data_path=data_path)),
            web.get("/routes", functools.partial(routes, data_path=data_path)),
        ]
    )


def init_func(argv):
    (data_path,) = argv
    app = web.Application()
    setup_routes(app, data_path)
    return app
