import argparse
import os
import requests
from pathlib import Path
from itertools import count
import time
import tqdm

from . import __version__

MTA_URL = 'http://datamine.mta.info/mta_esi.php?key={key}&feed_id=26'


def fetch_subway_to_disk():
    parser = argparse.ArgumentParser(
        description="Poll the subway realtime feed",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f'nycmtapy version {__version__}')
    parser.add_argument('datapath', type=str)
    parser.add_argument('--key', type=str)
    parser.add_argument('--poll', type=float, default=60)
    args = parser.parse_args()

    key = os.environ.get('NYC_KEY', '')
    if not key:
        key = args.key
    data_path = Path(args.datapath)
    data_path.mkdir(exist_ok=True)

    url = MTA_URL.format(key=key)
    for j in tqdm.tqdm(count()):
        r = requests.get(url)
        with open(data_path / f'mta_{int(time.time())}.pb', 'wb') as fout:
            fout.write(r.content)
        time.sleep(args.poll)
