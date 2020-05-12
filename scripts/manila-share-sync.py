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
import logging
import sys
import time
from datetime import datetime

import requests
from prometheus_client import Counter, Gauge, start_http_server
from sqlalchemy import Table, and_, select, update

from manilananny import ManilaNanny

log = logging.getLogger('nanny-manila-share-sync')
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')

ONEGB = 1073741824

TASK_SHARE_SIZE = '1'
TASK_MISSING_VOLUME = '2'
TASK_OFFLINE_VOLUME = '3'
TASK_ORPHAN_VOLUME = '4'


class ManilaShareSyncNanny(ManilaNanny):

    def __init__(self, config_file, prom_host, interval, tasks, dry_run_tasks):
        super(ManilaShareSyncNanny, self).__init__(config_file, interval)
        self.prom_host = prom_host + "/api/v1/query"
        self.MANILA_NANNY_SHARE_SYNC_FAILURE = Counter('manila_nanny_share_sync_failure', '')
        self.MANILA_SYNC_SHARE_SIZE_COUNTER = Counter('manila_nanny_sync_share_size',
                                                      'manila nanny sync share size')
        self.MANILA_RESET_SHARE_ERROR_COUNTER = Counter('manila_nanny_reset_share_error',
                                                        'manila nanny reset share status to error')
        self.MANILA_SHARE_MISSING_BACKEND_GAUGE = Gauge(
            'manila_nanny_share_missing_volume',
            'Manila Share missing backend volume',
            ['id', 'name', 'status'])
        self.MANILA_ORPHAN_VOLUMES_GAUGE = Gauge(
            'manila_nanny_orphan_volumes',
            'Orphan backend volumes of Manila service',
            ['share_id', 'share_status', 'filer', 'vserver', 'volume'])
        self.MANILA_OFFLINE_VOLUMES_GAUGE = Gauge(
            'manila_nanny_offline_volumes',
            'Offline volumes of Manila service',
            ['share_id', 'share_status', 'filer', 'vserver', 'volume'])

        self._tasks = tasks
        self._dry_run_tasks = dry_run_tasks
        if not any(tasks.values()):
            raise Exception('All tasks are disabled')

    def _run(self):
        # Need to recreate manila client each run, because of session timeout
        # self.renew_manila_client()

        # fetch data
        try:
            if self._tasks[TASK_SHARE_SIZE] or self._tasks[TASK_MISSING_VOLUME]\
                    or self._tasks[TASK_ORPHAN_VOLUME]:
                _share_list = self._query_shares()
                _volume_list = self._get_netapp_volumes()

                _shares, _orphan_volumes = self._merge_share_and_volumes(_share_list, _volume_list)

            if self._tasks[TASK_OFFLINE_VOLUME]:
                _offline_volume_list = self._get_netapp_volumes('offline')
        except Exception as e:
            log.warning(e)
            self.MANILA_NANNY_SHARE_SYNC_FAILURE.inc()
            return

        if self._tasks[TASK_SHARE_SIZE]:
            dry_run = self._dry_run_tasks[TASK_SHARE_SIZE]
            self.sync_share_size(_shares, dry_run)

        if self._tasks[TASK_MISSING_VOLUME]:
            dry_run = self._dry_run_tasks[TASK_MISSING_VOLUME]
            self.process_missing_volume(_shares, dry_run)

        if self._tasks[TASK_ORPHAN_VOLUME]:
            dry_run = self._dry_run_tasks[TASK_ORPHAN_VOLUME]
            self.process_orphan_volumes(_orphan_volumes, dry_run)

        if self._tasks[TASK_OFFLINE_VOLUME]:
            dry_run = self._dry_run_tasks[TASK_OFFLINE_VOLUME]
            self.process_offline_volumes(_offline_volume_list, dry_run)

    def sync_share_size(self, shares, dry_run=True):
        """ Backend volume exists, but share size does not match """
        msg = "share %s: share size != netapp volume size (%d != %d)"
        msg_dry_run = "Dry run: " + msg
        for (share_id, _), share in shares.items():
            if 'volume' not in share:
                continue
            size, vsize = share['size'], share['volume']['size']

            # volume size can not be zero, could be in offline state
            if vsize == 0:
                continue

            if size != vsize:
                if dry_run:
                    log.info(msg_dry_run, share_id, size, vsize)
                else:
                    log.info(msg, share_id, size, vsize)
                    self.set_share_size(share_id, vsize)
                    self.MANILA_SYNC_SHARE_SIZE_COUNTER.inc()

    def process_missing_volume(self, shares, dry_run=True):
        """ Set share state to error when backend volume is missing

        Ignore shares that are created/updated within 6 hours.
        """
        msg1 = "ManilaShareMissingVolume: id=%s, status=%s, created_at=%s, updated_at=%s"
        msg = msg1 + ": Set share status to error"
        dry_run_msg = "Dry run: " + msg1

        for (share_id, _), share in shares.items():
            if 'volume' not in share:
                # check if shares are created/updated recently
                if share['updated_at'] is not None:
                    delta = datetime.utcnow() - share['updated_at']
                else:
                    delta = datetime.utcnow() - share['created_at']
                if delta.total_seconds() < 6 * 3600:
                    continue

                if dry_run:
                    log.info(dry_run_msg, share_id, share['status'],
                             share['created_at'], share['updated_at'])
                else:
                    if share['status'] == 'available':
                        log.info(msg, share_id, share['status'],
                                 share['created_at'], share['updated_at'])
                        self._reset_share_state(share_id, 'error')
                        share['status'] = 'error'
                    else:
                        log.info(msg1, share_id, share['status'],
                                 share['created_at'], share['updated_at'])

                self.MANILA_SHARE_MISSING_BACKEND_GAUGE.labels(
                    id=share['id'],
                    name=share['name'],
                    status=share['status'],
                ).set(1)

    def process_offline_volumes(self, offline_volume_list, dry_run=True):
        """ offline volume

        @params offline_volumes:
            List[Volume]

        Volume: Dict[Keys['volume', 'vserver', 'filer'], Any]
        """

        _offline_volumes = {}
        for vol in offline_volume_list:
            if vol['volume'].startswith('share'):
                instance_id = vol['volume'][6:].replace('_', '-')
                _offline_volumes[instance_id] = vol

        # find associated share for offline volumes
        _shares = self._query_shares_by_instance_ids(list(_offline_volumes.keys()))
        for s in _shares:
            instance_id = s['instance_id']
            if instance_id in _offline_volumes:
                _offline_volumes[instance_id].update({'share': s})

        # ignore the shares that are updated/deleted recently
        _offline_volume_keys = list(_offline_volumes.keys())
        for vol_key in _offline_volumes:
            share = _offline_volumes.get('share')
            if share is not None:
                if share['updated_at'] is not None:
                    delta = datetime.utcnow() - share['updated_at']
                else:
                    delta = datetime.utcnow() - share['created_at']
                if delta.total_seconds() < 6 * 3600:
                    _offline_volume_keys.pop(vol_key)

        # process remaining volume
        for vol_key in _offline_volume_keys:
            vol = _offline_volumes[vol_key]
            name, filer = vol['volume'], vol['filer']
            share = vol.get('share')
            if share is not None:
                share_id, status = share['share_id'], share['status']
                log.info('OfflineVolume: %s (%s): Associated with share %s (%s)', name, filer,
                         share_id, status)
            else:
                share_id, status = '', ''
                log.info('OfflineVolume: %s (%s): No associated share found', name, filer)

            self.MANILA_OFFLINE_VOLUMES_GAUGE.labels(
                share_id=share_id,
                share_status=status,
                volume=vol['volume'],
                vserver=vol['vserver'],
                filer=vol['filer'],
            ).set(1)

    def process_orphan_volumes(self, volumes, dry_run=True):
        """ orphan volumes

        Check if the corresponding manila shares are deleted recently (hard coded as 6 hours).
        @params volumes: Dict[InstanceId, Volume]
        """
        # share instance id
        # volume key (extracted from volume name) is manila instance id
        vol_keys = volumes.keys()

        # Shares: List[Share])
        # Share.Keys: share_id, instance_id, deleted_at, status
        shares = self._query_shares_by_instance_ids(vol_keys)

        # merge share into volume
        for s in shares:
            volumes[s['instance_id']].update({'share': s})

        # loop over vol
        for instance_id, vol in volumes.items():
            # double check if the manila shares are deleted recently
            if 'share' in vol:
                share = vol['share']
                deleted_at = share.get('deleted_at', None)
                if deleted_at is not None:
                    if (datetime.utcnow() - deleted_at).total_seconds() < 6*3600:
                        vol_keys.remove(instance_id)

        for vol_key in vol_keys:
            vol = volumes[vol_key]
            name, filer = vol['volume'], vol['filer']
            if 'share' in vol:
                share_id, status = vol['share']['share_id'], vol['share']['status']
                log.info("OrphanVolume: %s (%s): Associated with share %s (%s)", name, filer,
                         share_id, status)
            else:
                share_id, status = '', ''
                log.info("OrphanVolume: %s (%s): No associated share", name, filer)

            self.MANILA_ORPHAN_VOLUMES_GAUGE.labels(
                share_id=share_id,
                share_status=status,
                filer=vol['filer'],
                vserver=vol['vserver'],
                volume=vol['volume'],
            ).set(1)

    def _get_netapp_volumes(self, status='online'):
        """ get netapp volumes from prometheus metrics
        return [<vol>, <vol>, ...]
        """
        def _merge_dicts(dict_a, dict_b):
            dict_a.update(dict_b)
            return dict_a

        def _filter_labels(vol):
            return {
                'volume': vol['volume'],
                'vserver': vol['vserver'],
                'filer': vol['filer'],
            }

        if status == 'online':
            query = "netapp_volume_total_bytes{app='netapp-capacity-exporter-manila'} + "\
                    "netapp_volume_snapshot_reserved_bytes"
            results = self._fetch_prom_metrics(query)
            return [
                _merge_dicts(_filter_labels(vol['metric']),
                             {'size': int(vol['value'][1]) / ONEGB})
                for vol in results
            ]

        if status == 'offline':
            query = "netapp_volume_state{app='netapp-capacity-exporter-manila'}==3"
            results = self._fetch_prom_metrics(query)
            return [
                _filter_labels(vol['metric']) for vol in results
            ]

    def _fetch_prom_metrics(self, query):
        try:
            r = requests.get(self.prom_host, params={'query': query, 'time': time.time()})
        except Exception as e:
            raise type(e)(f'_fetch_prom_metrics(query=\"{query}\"): {e}')
        if r.status_code != 200:
            return None
        return r.json()['data']['result']

    def _query_shares_by_instance_ids(self, instance_ids):
        """
        @return List[Share]

        Share: Dict[Keys['share_id', 'instance_id', 'created_at', 'updated_at', 'deleted_at',
                         'status'], Any]
        """
        shares_t = Table('shares', self.db_metadata, autoload=True)
        instances_t = Table('share_instances', self.db_metadata, autoload=True)
        q = select([shares_t.c.id.label('share_id'),
                    shares_t.c.created_at,
                    shares_t.c.updated_at,
                    shares_t.c.deleted_at,
                    instances_t.c.status,
                    instances_t.c.id.label('instance_id'),
                    ])\
            .where(shares_t.c.id == instances_t.c.share_id)\
            .where(instances_t.c.id.in_(instance_ids))
        r = q.execute()
        return [dict(zip(r.keys(), x)) for x in r.fetchall()]

    def _query_shares(self):
        """ Get shares that are not deleted """

        shares = Table('shares', self.db_metadata, autoload=True)
        instances = Table('share_instances', self.db_metadata, autoload=True)

        stmt = select([shares.c.id,
                       shares.c.display_name,
                       shares.c.size,
                       shares.c.created_at,
                       shares.c.updated_at,
                       instances.c.id,
                       instances.c.status,
                       ])\
            .select_from(
                shares.join(instances, shares.c.id == instances.c.share_id))\
            .where(shares.c.deleted == 'False')

        shares = []
        for (sid, name, size, ctime, utime, siid, status) in stmt.execute():
            shares.append({
                'id': sid,
                'name': name,
                'size': size,
                'created_at': ctime,
                'updated_at': utime,
                'instance_id': siid,
                'status': status,
            })
        return shares

    def _merge_share_and_volumes(self, shares, volumes):
        """ Merge shares and volumes by share id and volume name

        Assuming the volume name is `share_[share_instance_id]`. Update the share object
        with the volume fields ("filer", "vserver", "volume", "volume_size").

        Args:
            shares: List[]
            volumes: List[]

        Return:
            (shares, volumes): merged shares and unmerged volumes

            shares: Dict[(ShareId, InstanceId): Share]
            volumes: Dict[VolumeName: Volume]
        """
        _shares = {(s['id'], s['instance_id']): s for s in shares}
        _volumes = {
            vol['volume'][6:].replace('_', '-'): vol
            for vol in volumes if vol['volume'].startswith('share_')
        }
        for (share_id, instance_id) in _shares.keys():
            vol = _volumes.pop(instance_id, None)
            if vol:
                _shares[(share_id, instance_id)].update({'volume': vol})
        return _shares, _volumes

    def set_share_size(self, share_id, share_size):
        now = datetime.utcnow()
        shares_t = Table('shares', self.db_metadata, autoload=True)
        share_instances_t = Table('share_instances', self.db_metadata, autoload=True)
        update(shares_t) \
            .values(updated_at=now, size=share_size) \
            .where(shares_t.c.id == share_instances_t.c.share_id) \
            .where(and_(shares_t.c.id == share_id,
                        share_instances_t.c.status == 'available')) \
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


def str2bool(val):
    if isinstance(val, bool):
        return val
    if val.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    if val.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    raise argparse.ArgumentTypeError('Boolean value expected.')


def parse_cmdline_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",
                        default='/manila-etc/manila.conf',
                        help='configuration file')
    parser.add_argument("--netapp-prom-host",
                        help="never sync resources (no interactive check)")
    parser.add_argument("--interval",
                        type=float,
                        default=600,
                        help="interval")
    parser.add_argument("--prom-port",
                        type=int,
                        default=9457,
                        help="prometheus port")
    parser.add_argument("--task-share-size",
                        type=str2bool,
                        default=False,
                        help="enable share size task")
    parser.add_argument("--task-share-size-dry-run",
                        type=str2bool,
                        default=True,
                        help="dry run mode for share size task")
    parser.add_argument("--task-missing-volume",
                        type=str2bool,
                        default=False,
                        help="enable missing-volume task")
    parser.add_argument("--task-missing-volume-dry-run",
                        type=str2bool,
                        default=True,
                        help="dry run mode for missing-volume task")
    parser.add_argument("--task-offline-volume",
                        type=str2bool,
                        default=False,
                        help="enable offline-volume task")
    parser.add_argument("--task-offline-volume-dry-run",
                        type=str2bool,
                        default=True,
                        help="dry run mode for offline-volume task")
    parser.add_argument("--task-orphan-volume",
                        type=str2bool,
                        default=False,
                        help="enable orphan-volume task")
    parser.add_argument("--task-orphan-volume-dry-run",
                        type=str2bool,
                        default=True,
                        help="dry run mode for orphan-volume task")
    return parser.parse_args()


def main():
    try:
        args = parse_cmdline_args()
        log.info("command line arguments...")
        log.info(args)
    except Exception as e:
        sys.stdout.write("parse command line arguments (%s)" % e)

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

    ManilaShareSyncNanny(
        args.config,
        args.netapp_prom_host,
        args.interval,
        tasks,
        dry_run_tasks,
    ).run()


if __name__ == "__main__":
    # test_resize()
    main()
