from ._version import get_versions

from tqdm import tqdm
import requests
import datetime
import time
from pathlib import Path

__version__ = get_versions()["version"]
del get_versions

def load_one_msg(buff):
    """Parse one protbuf blob -> FeedMessage

    Parameters
    ----------
    buff : bytes

    Returns
    -------
    m : FeedMessage
    """
    from nycmtapy.gtfs_realtime_pb2 import FeedMessage

    # need to import this to register extensions!
    import nycmtapy.nyct_subway_pb2  # noqa

    m = FeedMessage()
    m.ParseFromString(buff)
    return m


def generate_key(trip):
    """Generate a unique key from the given trip

    The 'trip_id' is only unique with in it's scheduled day.  Thus, we
    need to use both trip_id and the start_date to get a globally
    unique ID from a train run.

    Parameters
    ----------
    trip : TripDescriptor

    Returns
    -------
    key : tuple

    """
    return (trip.start_date, trip.trip_id)


def fetch_static(data_path):
    """Fetch the static feed information and write to disk

    Parameters
    ----------
    data_path : Path or str
         Where to write the data


    """
    data_path = Path(data_path)
    data_path.mkdir(exist_ok=True, parents=True)
    urls = {
        "lirr": "http://web.mta.info/developers/data/lirr/google_transit.zip",
        # "mta": "http://web.mta.info/developers/data/nyct/subway/google_transit.zip",
        "mta": "http://web.mta.info/developers/files/google_transit_supplemented.zip",
        "mnr": "http://web.mta.info/developers/data/mnr/google_transit.zip",
    }

    for key, url in urls.items():
        r = requests.get(url)
        with open(
            Path(data_path) / f"{key}_static_{int(time.time())}.zip", "wb"
        ) as fout:
            fout.write(r.content)

