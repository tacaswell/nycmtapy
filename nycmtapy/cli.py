import argparse
import os
from itertools import count
import time
import tqdm

from . import __version__, fetch_realtime


def fetch_subway_to_disk():
    parser = argparse.ArgumentParser(
        description="Poll the subway realtime feed",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"nycmtapy version {__version__}",
    )
    parser.add_argument("datapath", type=str)
    parser.add_argument("--key", type=str)
    parser.add_argument("--poll", type=float, default=60)
    args = parser.parse_args()

    key = os.environ.get("NYC_KEY", "")
    if not key:
        key = args.key

    for j in tqdm.tqdm(count()):
        start = time.monotonic()
        fetch_realtime(args.datapath, key)
        done = time.monotonic()
        time.sleep(max(0, args.poll - (done - start)))
