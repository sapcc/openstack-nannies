#!/usr/bin/env python
#
# Copyright (c) 2018 SAP SE
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

import argparse
import sys
import datetime
import time
import requests

# from prettytable import PrettyTable
from sqlalchemy import and_
from sqlalchemy import delete
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy.sql.expression import false
from prometheus_client import start_http_server, Counter
from manila_nanny import ManilaNanny, get_db_url

query = 'netapp_capacity_svm{metric="size_total"} + ignoring(metric) netapp_capacity_svm{metric="size_reserved_by_snapshots"}'
onegb = 1073741824

class ManilaShareSyncNanny(ManilaNanny):
    _shares = {}
    _non_exist_shares = {}

    def __init__(self, db_url, prom_host, prom_query, interval, dry_run):
        super(ManilaShareSyncNanny, self).__init__(db_url, interval, dry_run)
        self.prom_host = prom_host+"/api/v1/query"
        self.prom_query = prom_query
        self.MANILA_SHARE_SIZE_SYNCED = Counter('manila_nanny_share_size_synced', '')
        self.MANILA_SHARE_NOT_EXIST = Counter('manila_nanny_share_not_exist', '')

    def _run(self):
        self._shares = {}
        self.get_shares_from_netapp()
        self.get_shares_from_manila()

        for share_id, v in self._shares.items():
            if v.get('manila_size') is not None and v.get('size') is not None:
                if v.get('manila_size') != v.get('size'):
                    if (datetime.datetime.utcnow() - v.get('updated_at')).total_seconds() > 600 \
                        and self.dry_run is False:
                        print("share %s: manila share size (%d) does not " + \
                              "match share size (%d) on backend, fixing ...") % (\
                              share_id, v.get('manila_size'), v.get('size'))
                        self.set_share_size(share_id, v.get('size'))
                        self.MANILA_SHARE_SIZE_SYNCED.inc()
                    else:
                        print("share %s: manila share size (%d) does not " + \
                              "match share size (%d) on backend") % (\
                              share_id, v.get('manila_size'), v.get('size'))
            elif v.get('manila_size') is not None and v.get('size') is None:
                print("[WARNING] ShareNotExistOnBackend: id=%s" % share_id)
                if self._non_exist_shares.get(share_id, 0) == 0:
                    self._non_exist_shares[share_id] = 1
                    self.MANILA_SHARE_NOT_EXIST.inc()

    def get_shares_from_netapp(self):
        payloads = {
            'query': self.prom_query,
            'time': time.time()
        }
        r = requests.get(self.prom_host, params=payloads)

        if r.status_code != 200:
            return
        for s in r.json()['data']['result']:
            if not s['metric'].get('share_id'):
                continue
            x = {
                'share_id': s['metric']['share_id'],
                'vserver': s['metric']['vserver'],
                'volume': s['metric']['volume'],
                'size': int(s['value'][1])/onegb
            }
            if x['share_id'] not in self._shares.keys():
                self._shares[x['share_id']] = x
            else:
                self._shares[x['share_id']].update(x)

    def get_shares_from_manila(self):
        shares_t = Table('shares', self.db_metadata, autoload=True)
        share_instances_t = Table('share_instances', self.db_metadata, autoload=True)
        shares_join = shares_t.join(share_instances_t, shares_t.c.id == share_instances_t.c.share_id)
        q = select(columns=[shares_t.c.id, shares_t.c.size, share_instances_t.c.updated_at]) \
            .select_from(shares_join) \
            .where(and_(shares_t.c.deleted=='False', share_instances_t.c.status=='available'))
        for (share_id, share_size, updated_at) in q.execute():
            if share_id not in self._shares.keys():
                self._shares[share_id] = {
                    'share_id': share_id,
                    'manila_size': share_size,
                    'updated_at': updated_at
                }
            else:
                self._shares[share_id].update({
                    'manila_size': share_size,
                    'updated_at': updated_at
                })
        return q

    def set_share_size(self, share_id, share_size):
        now = datetime.datetime.utcnow()
        shares_t = Table('shares', self.db_metadata, autoload=True)
        share_instances_t = Table('share_instances', self.db_metadata, autoload=True)
        shares_t.update() \
                .values(updated_at=now, size=share_size) \
                .where(shares_t.c.id == share_instances_t.c.share_id) \
                .where(and_(shares_t.c.id == share_id, share_instances_t.c.status == 'available')) \
                .execute()

def parse_cmdline_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",
                        default='/manila-etc/manila.conf',
                        help='configuration file')
    parser.add_argument("--netapp-prom-host",
                        help="never sync resources (no interactive check)")
    parser.add_argument("--netapp-prom-query",
                        default=query,
                        help="always sync resources (no interactive check)")
    parser.add_argument("--dry-run",
                       action="store_true",
                       help='print only what would be done without actually doing it')
    parser.add_argument("--interval",
                        default=600,
                        type=float,
                        help="interval")
    parser.add_argument("--prom-port",
                        default=9456,
                        type=int,
                        help="prometheus port")
    return parser.parse_args()


def main():
    try:
        args = parse_cmdline_args()
        print(args)
    except Exception as e:
        sys.stdout.write("parse command line arguments (%s)" % e.strerror)

    try:
        start_http_server(args.prom_port)
    except Exception as e:
        sys.stdout.write("start_http_server: " + str(e) + "\n")
        sys.exit(-1)

    # connect to the DB
    db_url = get_db_url(args.config)
    ManilaShareSyncNanny(db_url, 
                         args.netapp_prom_host,
                         args.netapp_prom_query,
                         args.interval,
                         args.dry_run
                         ).run()

def test_resize():
    try:
        args = parse_cmdline_args()
    except Exception as e:
        sys.stdout.write("Check command line arguments (%s)" % e.strerror)

    # connect to the DB
    db_url = get_db_url(args.config)
    nanny = ManilaShareSyncNanny(db_url, args.netapp_prom_host, args.netapp_prom_query, args.interval, args.dry_run)
    nanny.set_share_size('7eb50f3b-b5ea-47e2-a6e9-5934de57c777', 4)


if __name__ == "__main__":
    # test_resize()
    main()
