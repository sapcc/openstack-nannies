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
import time
import requests
import logging
from datetime import datetime

# from prettytable import PrettyTable
from sqlalchemy import and_
from sqlalchemy import delete
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy.sql.expression import false
from prometheus_client import start_http_server, Counter
from manila_nanny import ManilaNanny

log = logging.getLogger('nanny-manila-share-sync')
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')

NETAPP_VOLUME_QUERY = "netapp_volume_total_bytes{app='netapp-capacity-exporter-manila'} + " \
                      "netapp_volume_snapshot_reserved_bytes"
NETAPP_VOLUME_STATE_QUERY = "netapp_volume_state{app='netapp-capacity-exporter-manila'}"
onegb = 1073741824

class ManilaShareSyncNanny(ManilaNanny):
    _shares = {}
    _non_exist_shares = {}

    def __init__(self, config_file, prom_host, interval, dry_run):
        super(ManilaShareSyncNanny, self).__init__(config_file, interval, dry_run)
        self.prom_host = prom_host+"/api/v1/query"
        self.MANILA_SHARE_SIZE_SYNCED = Counter('manila_nanny_share_size_synced', '')
        self.MANILA_SHARE_NOT_EXIST = Counter('manila_nanny_share_not_exist', '')

    def _run(self):
        volumes = self.get_netapp_volumes()
        shares = self.get_manila_shares()
        vstates = self.get_netapp_volume_states()

        # Don't correct anything when fetching netapp volumes fails
        if volumes is None or shares is None or vstates is None:
            log.warning("Skip nanny run because queries have failed.")
            return

        for share_id, vol in vstates.iteritems():
            if vol['state'] == 0:
                log.info("Volume %s on filer %s is offline. Reset status of share %s to 'error'",
                         vol.get('volume'), vol.get('filer'), share_id)
                self._resset_share_state(share_id, "error")

        for share_id, share in shares.iteritems():
            ssize = share['size']
            utime = share['updated_at']

            # Backend volume exists, but size does not match
            vol = volumes.get(share_id)
            if vol:
                vsize = vol['size']
                if vsize != ssize:
                    msg = "share %s: manila share size != netapp volume size " \
                          "(%d != %d)".format(share_id, ssize, vsize )
                    if self.dry_run:
                        log.info("Dry run: " + msg)
                    else:
                        log.info(msg)
                        self.set_share_size(share_id, vsize)
                        self.MANILA_SHARE_SIZE_SYNCED.inc()

            # Backend volume does NOT exist
            else:
                # The comparison must between utcnow() and utime, since utime is utc time.
                delta = datetime.utcnow() - utime
                if delta.total_seconds() > 300:
                    self.MANILA_SHARE_NOT_EXIST.inc()
                    log.warn("ShareNotExistOnBackend: id=%s" % share_id)

    def get_netapp_volumes(self):
        results = self._fetch_prom_metrics(NETAPP_VOLUME_QUERY)
        if results is None:
            return None
        vols = {}
        for s in results:
            share_id = s['metric'].get('share_id')
            if share_id is not None:
                vols[share_id] = {
                    'share_id': share_id,
                    'vserver': s['metric']['vserver'],
                    'volume': s['metric']['volume'],
                    'size': int(s['value'][1])/onegb,
                }
        return vols

    def get_netapp_volume_states(self):
        results = self._fetch_prom_metrics(NETAPP_VOLUME_STATE_QUERY)
        if results is None:
            return None
        vols = {}
        for s in results:
            labels = s['metric']
            value = s['value']
            share_id = labels.get('share_id')
            if share_id is not None:
                vols[share_id] = {
                    'share_id': share_id,
                    'volume': labels['volume'],
                    'filer': labels['filer'],
                    'state': int(value[1]),
                }
        return vols

    def _fetch_prom_metrics(self, query):
        try:
            r = requests.get(self.prom_host, params={
                'query': query,
                'time': time.time()
            })
        except Exception as e:
            log.error("_fetch_prom_metrics({}): ".format(query) + str(e))
            return None
        if r.status_code != 200:
            return None
        return r.json()['data']['result']

    def get_manila_shares(self):
        shares_t = Table('shares', self.db_metadata, autoload=True)
        share_instances_t = Table('share_instances', self.db_metadata, autoload=True)
        shares_join = shares_t.join(share_instances_t, shares_t.c.id == share_instances_t.c.share_id)
        q = select(columns=[shares_t.c.id, shares_t.c.size, share_t.c.updated_at]) \
            .select_from(shares_join) \
            .where(and_(shares_t.c.deleted=='False', share_instances_t.c.status=='available'))
        shares = {}
        for (share_id, share_size, updated_at) in q.execute():
            shares[share_id] = {
                    'share_id': share_id,
                    'size': share_size,
                    'updated_at': updated_at
            }
        return shares

    def set_share_size(self, share_id, share_size):
        now = datetime.utcnow()
        shares_t = Table('shares', self.db_metadata, autoload=True)
        share_instances_t = Table('share_instances', self.db_metadata, autoload=True)
        shares_t.update() \
                .values(updated_at=now, size=share_size) \
                .where(shares_t.c.id == share_instances_t.c.share_id) \
                .where(and_(shares_t.c.id == share_id, share_instances_t.c.status == 'available')) \
                .execute()

    def _reset_share_state(self, share_id, state):
        try:
            self.manilaclient.shares.reset_state(share_id, state)
        except Exception as e:
            log.exception("_reset_share_state(): %s", e)

def parse_cmdline_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",
                        default='/manila-etc/manila.conf',
                        help='configuration file')
    parser.add_argument("--netapp-prom-host",
                        help="never sync resources (no interactive check)")
    parser.add_argument("--dry-run",
                       action="store_true",
                       help='print only what would be done without actually doing it')
    parser.add_argument("--interval",
                        default=600,
                        type=float,
                        help="interval")
    parser.add_argument("--prom-port",
                        default=9457,
                        type=int,
                        help="prometheus port")
    return parser.parse_args()


def main():
    try:
        args = parse_cmdline_args()
        log.info("command line arguments...")
        log.info(args)
    except Exception as e:
        sys.stdout.write("parse command line arguments (%s)" % e.strerror)

    try:
        start_http_server(args.prom_port)
    except Exception as e:
        sys.stdout.write("start_http_server: " + str(e) + "\n")
        sys.exit(-1)

    ManilaShareSyncNanny(args.config,
                         args.netapp_prom_host,
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
