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
from prometheus_client import start_http_server, Counter, Gauge
from manilananny import ManilaNanny
from manilaclient.common.apiclient import exceptions as manilaApiExceptions

log = logging.getLogger('nanny-manila-share-sync')
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')

NETAPP_VOLUME_QUERY = "netapp_volume_total_bytes{app='netapp-capacity-exporter-manila'} + " \
                      "netapp_volume_snapshot_reserved_bytes"
NETAPP_VOLUME_STATE_QUERY = "netapp_volume_state{app='netapp-capacity-exporter-manila'}"
onegb = 1073741824

TASK_SHARE_SIZE = '1'
TASK_MISSING_VOLUME = '2'
TASK_OFFLINE_VOLUME = '3'
TASK_ORPHAN_VOLUME = '4'

class ManilaShareSyncNanny(ManilaNanny):
    def __init__(self, config_file, prom_host, interval, tasks, dry_run_tasks):
        super(ManilaShareSyncNanny, self).__init__(config_file, interval)
        self.prom_host = prom_host+"/api/v1/query"
        self.MANILA_NANNY_SHARE_SYNC_FAILURE = Counter(
                'manila_nanny_share_sync_failure', '')
        self.MANILA_SHARE_MISSING_BACKEND_GAUGE = Gauge(
                'manila_nanny_share_missing_backend',
                'Backend volume for manila share does not exist',
                ['id', 'name', 'status', 'project'])
        self.MANILA_ORPHAN_VOLUMES_GAUGE = Gauge(
                'manila_nanny_orphan_volumes',
                'Orphan backedn volumes of Manila service',
                ['share_id', 'filer', 'vserver', 'volume'])
        self.MANILA_SYNC_SHARE_SIZE_COUNTER = Counter(
                'manila_nanny_sync_share_size',
                'manila nanny sync share size')
        self.MANILA_RESET_SHARE_ERROR_COUNTER = Counter(
                'manila_nanny_reset_share_error',
                'manila nanny reset share status to error')

        self._tasks = tasks
        self._dry_run_tasks = dry_run_tasks
        if not any(tasks.values()):
            raise Exception('All tasks are disabled')

    def _run(self):
        # Need to recreate manila client each run, because of session timeout
        self.renew_manila_client()
        try:
            volumes = self.get_netapp_volumes()
            vstates = self.get_netapp_volume_states()
            shares = self.get_shares()
        except Exception as e:
            log.warning("Skip nanny run because queries have failed: %s", e)
            self.MANILA_NANNY_SHARE_SYNC_FAILURE.inc()
            return

        if self._tasks[TASK_SHARE_SIZE]:
            dry_run = self._dry_run_tasks[TASK_SHARE_SIZE]
            self.sync_share_size(shares, volumes, dry_run)
        if self._tasks[TASK_MISSING_VOLUME]:
            dry_run = self._dry_run_tasks[TASK_MISSING_VOLUME]
            self.process_missing_backend(shares, volumes, dry_run)
        if self._tasks[TASK_OFFLINE_VOLUME]:
            dry_run = self._dry_run_tasks[TASK_OFFLINE_VOLUME]
            self.process_offline_volumes(volumes, vstates, dry_run)
        if self._tasks[TASK_ORPHAN_VOLUME]:
            dry_run = self._dry_run_tasks[TASK_ORPHAN_VOLUME]
            self.process_orphan_volumes(shares, volumes, dry_run)

    def sync_share_size(self, shares, volumes, dry_run=True):
        """ Backend volume exists, but share size does not match """
        msg = "share %s: manila share size != netapp volume size (%d != %d)"
        msg_dry_run = "Dry run: " + msg
        for share_id, share in shares.iteritems():
            if volumes.get(share_id) is None:
                continue
            vsize = volumes.get(share_id)['size']
            if vsize != share.size:
                if dry_run:
                    log.info(msg_dry_run, share_id, share.size, vsize)
                else:
                    log.info(msg, share_id, share.size, vsize)
                    self.set_share_size(share_id, vsize)
                    self.MANILA_SYNC_SHARE_SIZE_COUNTER.inc()

    def process_missing_backend(self, shares, volumes, dry_run=True):
        """ Set share state to error when backend volume is missing

            We rely on the backend volume's comment field to relate share and
            volume.
        """
        # clear metrics
        self.MANILA_SHARE_MISSING_BACKEND_GAUGE._metrics.clear()

        msg1 = "ShareMissingBackend: id=%s, status=%s, created_at=%s"
        msg2 = "Set share status to error: " + msg1
        msg1_dry_run = "Dry run: " + msg1
        msg2_dry_run = "Dry run: " + msg2

        for share_id, share in shares.iteritems():
            if volumes.get(share_id) is not None:
                continue
            if share.status == 'available':
                # Only when share is NOT created very recent.
                # It should be compared using utc time.
                c = datetime.strptime(share.created_at, '%Y-%m-%dT%H:%M:%S.%f')
                if (datetime.utcnow() - c).total_seconds() > 600:
                    self.MANILA_SHARE_MISSING_BACKEND_GAUGE.labels(
                        id=share.id, name=share.name, status=share.status,
                        project=share.project_id
                    ).set(1)
                    if dry_run:
                        log.info(msg2_dry_run,
                                 share_id, share.status, share.created_at)
                    else:
                        log.info(msg2, share_id, share.status, share.created_at)
                        self._reset_share_state(share_id, 'error')
                        self.MANILA_RESET_SHARE_ERROR_COUNTER.inc()
            elif share.status == 'error':
                self.MANILA_SHARE_MISSING_BACKEND_GAUGE.labels(
                    id=share.id, name=share.name, status=share.status,
                    project=share.project_id
                ).set(1)
                log.info(msg1, share_id, share.status, share.created_at)
            else:
                log.info(msg1, share_id, share.status, share.created_at)

    def process_offline_volumes(self, volumes, vstates, dry_run=True):
        msg1 = "Volume %s on filer %s is offline"
        msg2 = "Reset status of share %s from '%s' to 'error'"
        msg3 = "Status of share %s is '%s'"

        for share_id, vol in vstates.iteritems():
            if vol['state'] == 1:
                continue
            # share can be deleted meanwhile
            try:
                s = self.manilaclient.shares.get(share_id)
            except manilaApiExceptions.NotFound:
                continue
            if s.status == 'available':
                if dry_run:
                    log.info("Dry run: " + msg2 + ": " + msg1,
                            vol.get('volume'), vol.get('filer'),
                            share_id, s.status)
                else:
                    log.info(msg2 + ": " + msg1,
                            vol.get('volume'), vol.get('filer'),
                            share_id, s.status)
                    self._reset_share_state(share_id, "error")
                    self.MANILA_RESET_SHARE_ERROR_COUNTER.inc()
            else:
                log.info(msg3 + ": " + msg1,
                        vol.get('volume'), vol.get('filer'),
                        share_id, s.status)

    def process_orphan_volumes(self, shares, volumes, dry_run=True):
        msg = "Orphan volume %s is found on filer %s (share_id = %s)"

        orphan_volumes = {}
        share_ids = shares.keys()

        for share_id, vol in volumes.iteritems():
            if share_id not in share_ids:
                try:
                    s = self.manilaclient.shares.get(share_id)
                except manilaApiExceptions.NotFound:
                    orphan_volumes[share_id] = vol

        # It may take backend some time to delete the shares, double check if
        # the shares are deleted recently in manila db
        for s in self.query_shares_by_ids(orphan_volumes.keys()):
            if s['deleted_at'] is not None:
                if (datetime.utcnow() - s['deleted_at']).total_seconds() < 600:
                    orphan_volumes[s['id']] = None
        for share_id, vol in filter(
                lambda (_, v): v is not None, orphan_volumes.iteritems()):
            # set gauge value to 0: orphan volume found but not corrected
            self.MANILA_ORPHAN_VOLUMES_GAUGE.labels(
                share_id=share_id, filer=vol['filer'],
                vserver=vol['vserver'], volume=vol['volume'],
            ).set(0)
            log.info(msg, vol['volume'], vol['filer'], share_id)

    def get_netapp_volumes(self):
        results = self._fetch_prom_metrics(NETAPP_VOLUME_QUERY)
        vols = {}
        for s in results:
            share_id = s['metric'].get('share_id')
            if share_id is not None:
                vols[share_id] = dict(share_id=share_id,
                                      filer=s['metric']['filer'],
                                      vserver=s['metric']['vserver'],
                                      volume=s['metric']['volume'],
                                      size=int(s['value'][1]) / onegb)
        return vols

    def get_netapp_volume_states(self):
        results = self._fetch_prom_metrics(NETAPP_VOLUME_STATE_QUERY)
        vols = {}
        for s in results:
            labels = s['metric']
            value = s['value']
            share_id = labels.get('share_id')
            if share_id is not None:
                vols[share_id] = dict(share_id=share_id,
                                      volume=labels['volume'],
                                      filer=labels['filer'],
                                      state=int(value[1]))
        return vols

    def _fetch_prom_metrics(self, query):
        try:
            r = requests.get(self.prom_host, params={
                'query': query,
                'time': time.time()
            })
        except Exception as e:
            raise type(e)("_fetch_prom_metrics(query=\"%s\"): %s".format(query, e.message))
        if r.status_code != 200:
            return None
        return r.json()['data']['result']

    def query_shares_by_ids(self, share_ids):
        shares_t = Table('shares', self.db_metadata, autoload=True)
        q = select([shares_t]).where(shares_t.c.id.in_(share_ids))
        shares = []
        for r in q.execute():
            shares.append({k: v for (k, v) in r.items()})
        return shares

    def query_shares(self):
        shares_t = Table('shares', self.db_metadata, autoload=True)
        share_instances_t = Table('share_instances', self.db_metadata, autoload=True)
        shares_join = shares_t.join(share_instances_t, shares_t.c.id == share_instances_t.c.share_id)
        q = select(columns=[shares_t.c.id, shares_t.c.size, shares_t.c.updated_at]) \
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

    def get_shares(self):
        # manila api returns maximally 1000 shares
        _limit = 1000
        opts = dict(all_tenants=1, limit=_limit, offset=0)
        try:
            data = self.manilaclient.shares.list(search_opts=opts)
            shares = data
            while len(data) == _limit:
                opts['offset'] += _limit
                data = self.manilaclient.shares.list(search_opts=opts)
                shares.extend(data)
        except Exception as e:
            raise type(e)("get_shares(): " + str(e))
        return {s.id: s for s in shares}

    def _reset_share_state(self, share_id, state):
        try:
            self.manilaclient.shares.reset_state(share_id, state)
        except Exception as e:
            log.exception("_reset_share_state(share_id=%s, state=%s): %s", share_id, state, e)

def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def parse_cmdline_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",
                        default='/manila-etc/manila.conf',
                        help='configuration file')
    parser.add_argument("--netapp-prom-host",
                        help="never sync resources (no interactive check)")
    parser.add_argument("--interval",
                        default=600,
                        type=float,
                        help="interval")
    parser.add_argument("--prom-port",
                        default=9457,
                        type=int,
                        help="prometheus port")
    parser.add_argument("--task-share-size",
                        type=str2bool,
                        default=False,
                        help="enable share size task")
    parser.add_argument("--task-share-size-dry-run",
                        type=str2bool,
                        default=False,
                        help="dry run mode for share size task")
    parser.add_argument("--task-missing-volume",
                        type=str2bool,
                        default=False,
                        help="enable missing-volume task")
    parser.add_argument("--task-missing-volume-dry-run",
                        type=str2bool,
                        default=False,
                        help="dry run mode for missing-volume task")
    parser.add_argument("--task-offline-volume",
                        type=str2bool,
                        default=False,
                        help="enable offline-volume task")
    parser.add_argument("--task-offline-volume-dry-run",
                        type=str2bool,
                        default=False,
                        help="dry run mode for offline-volume task")
    parser.add_argument("--task-orphan-volume",
                        type=str2bool,
                        default=False,
                        help="enable orphan-volume task")
    parser.add_argument("--task-orphan-volume-dry-run",
                        type=str2bool,
                        default=False,
                        help="dry run mode for orphan-volume task")
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

    tasks = {
        TASK_SHARE_SIZE: args.task_share_size,
        TASK_MISSING_VOLUME: args.task_missing_volume,
        TASK_OFFLINE_VOLUME: args.task_offline_volume,
        TASK_ORPHAN_VOLUME: args.task_orphan_volume,
    }

    dry_run_tasks = {
        TASK_SHARE_SIZE: args.task_share_size_dry_run,
        TASK_MISSING_VOLUME: args.task_missing_volume_dry_run,
        TASK_OFFLINE_VOLUME: args.task_offline_volume_dry_run,
        TASK_ORPHAN_VOLUME: args.task_orphan_volume_dry_run,
    }

    ManilaShareSyncNanny(args.config,
                         args.netapp_prom_host,
                         args.interval,
                         tasks,
                         dry_run_tasks,
                         ).run()

if __name__ == "__main__":
    # test_resize()
    main()
