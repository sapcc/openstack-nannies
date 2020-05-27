# Copyright (c) 2020 SAP SE
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#
from __future__ import absolute_import

import argparse
import datetime
from http.server import BaseHTTPRequestHandler
from threading import Lock
from typing import Dict

from prometheus_client import Gauge
from sqlalchemy import Table, select

from manilananny import ManilaNanny, response, update_dict


class MyHandler(BaseHTTPRequestHandler):
    ''' http server handler '''
    def do_GET(self):
        if self.path == '/':
            status_code, header, data = self.server.get_orphan_snapshots()
        else:
            status_code, header, data = self.server.undefined_route(self.path)
        self.send_response(status_code)
        self.send_header(*header)
        self.end_headers()
        self.wfile.write(data.encode('utf-8'))


class ManilaShareServerNanny(ManilaNanny):
    """ Manila Share Server """
    def __init__(self, config_file, interval, prom_port, port, handler):
        super(ManilaShareServerNanny, self).__init__(config_file,
                                                     interval,
                                                     prom_port=prom_port,
                                                     port=port,
                                                     handler=handler)
        self.orphan_snapshots_lock = Lock()
        self.orphan_snapshots: Dict[str, Dict[str, str]] = {}
        self.orphan_snapshots_gauge = Gauge('manila_nanny_orphan_share_snapshots',
                                            'Orphan Manila Share Snapshots',
                                            ['id'])

    def _run(self):
        s = self.query_orphan_snapshots()
        orphan_snapshots = {
            snapshot_id: {
                'snapshot_id': snapshot_id,
                'share_id': share_id,
                'since': datetime.datetime.utcnow()
            }
            for snapshot_id, share_id in s}
        with self.orphan_snapshots_lock:
            self.orphan_snapshots = update_dict(self.orphan_snapshots, orphan_snapshots)
        for snapshot_id in orphan_snapshots:
            self.orphan_snapshots_gauge.labels(id=snapshot_id).set(1)

    def query_orphan_snapshots(self):
        Snapshots = Table('share_snapshots', self.db_metadata, autoload=True)
        Shares = Table('shares', self.db_metadata, autoload=True)
        q = select([Snapshots.c.id, Snapshots.c.share_id])\
            .select_from(Snapshots.join(Shares, Snapshots.c.share_id == Shares.c.id))\
            .where(Snapshots.c.deleted == 'False')\
            .where(Shares.c.deleted != 'False')
        return list(q.execute())

    @response
    def get_orphan_snapshots(self):
        with self.orphan_snapshots_lock:
            return list(self.orphan_snapshots.values())


def parse_cmdline_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default='/etc/manila/manila.conf', help='configuration file')
    parser.add_argument("--interval", type=float, default=3600, help="interval")
    parser.add_argument("--listen-port", type=int, default=8000, help="http server listen port")
    parser.add_argument("--prom-port", type=int, default=9000, help="http server listen port")
    return parser.parse_args()


def main():
    args = parse_cmdline_args()
    ManilaShareServerNanny(
        args.config,
        args.interval,
        prom_port=args.prom_port,
        port=args.listen_port,
        handler=MyHandler
    ).run()


if __name__ == "__main__":
    main()
