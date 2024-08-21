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
from __future__ import absolute_import

import argparse
import configparser
import logging
import re
import time
import traceback
from datetime import datetime
from threading import Lock

import requests
from prometheus_client import Counter, Gauge
from sqlalchemy import Table, select, update

from helper.manilananny import base_command_parser
from manilananny import CustomAdapter, ManilaNanny, is_utcts_recent, response, update_records

log = logging.getLogger('nanny-manila-share-sync')
logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s')

ONEGB = 1073741824

TASK_SHARE_SIZE = '1'
TASK_MISSING_VOLUME = '2'
TASK_OFFLINE_VOLUME = '3'
TASK_ORPHAN_VOLUME = '4'
TASK_SHARE_STATE = '5'


class ManilaShareSyncNanny(ManilaNanny):

    def __init__(self, config_file, prom_host, interval, tasks, dry_run_tasks, prom_port):
        super(ManilaShareSyncNanny, self).__init__(config_file,
                                                   interval,
                                                   prom_port=prom_port)
        self.prom_host = prom_host + "/api/v1/query"

        self.MANILA_NANNY_SHARE_SYNC_FAILURE = Counter('manila_nanny_share_sync_failure', '')
        self.MANILA_SYNC_SHARE_SIZE_COUNTER = Counter('manila_nanny_sync_share_size',
                                                      'manila nanny sync share size')
        self.MANILA_RESET_SHARE_ERROR_COUNTER = Counter('manila_nanny_reset_share_error',
                                                        'manila nanny reset share status to error')
        self.manila_missing_volume_shares_gauge = Gauge(
            'manila_nanny_share_missing_volume', 'Manila Share missing backend volume',
            ['share_id', 'instance_id', 'share_name', 'share_status'])
        self.manila_orphan_volumes_gauge = Gauge(
            'manila_nanny_orphan_volumes', 'Orphan backend volumes of Manila service',
            ['share_id', 'share_status', 'filer', 'vserver', 'volume'])
        self.manila_offline_volumes_gauge = Gauge(
            'manila_nanny_offline_volumes', 'Offline volumes of Manila service',
            ['share_id', 'share_status', 'filer', 'vserver', 'volume'])

        self._tasks = tasks
        self._dry_run_tasks = dry_run_tasks
        if not any(tasks.values()):
            raise Exception('All tasks are disabled')

        self.orphan_volumes_lock = Lock()
        self.orphan_volumes = {}
        self.missing_volumes_lock = Lock()
        self.missing_volumes = {}
        self.offline_volumes_lock = Lock()
        self.offline_volumes = {}
        self.net_capacity_snap_reserve = self.get_net_capacity_snap_reserve(config_file)

    def get_net_capacity_snap_reserve(self, config_file):
        """Return the snapshot_reserve_percent from the config file"""
        parser = configparser.ConfigParser()
        try:
            parser.read(config_file)
            snap_percent = parser.get('DEFAULT', 'netapp_volume_snapshot_reserve_percent')
            return int(snap_percent)
        except Exception:
            log.warning(
                "WARN: Manila config file missing netapp_volume_snapshot_reserve_percent, setting to 50"
            )
            return 50

    def _run(self):
        # TODO: implement proper re-auth after token lifetime ended
        # Need to recreate manila client each run, because of session timeout
        self.renew_manila_client()

        # fetch data
        try:
            _share_list = self._query_shares()
            _volume_list = self._get_netapp_volumes()
            _shares, _orphan_volumes = self._merge_share_and_volumes(_share_list, _volume_list)
        except Exception as e:
            log.warning(''.join(traceback.format_exception(None, e, e.__traceback__)))
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
            try:
                _offline_volume_list = self._get_netapp_volumes_offline()
                self.process_offline_volumes(_offline_volume_list, dry_run)
            except Exception as e:
                log.warning(e)

        if self._tasks[TASK_SHARE_STATE]:
            dry_run = self._dry_run_tasks[TASK_SHARE_STATE]
            self.reset_share_replica_state(_shares, dry_run)
            _shares = self._query_shares()
            self.sync_share_state(_shares, dry_run)

    def sync_share_size(self, shares, dry_run=True):
        """ Backend volume exists, but share size does not match """
        logger = CustomAdapter(log, {'task': 'sync_share_size', 'dry_run': dry_run})
        for (share_id, _), share in shares.items():
            if 'volume' not in share:
                logger.warn('skip share: no volume found', share_id=share_id)
                continue
            if share['volume']['size'] == 0:
                logger.warn('skip share: volume size is zero', share_id=share_id)
                continue
            if share['volume']['volume_type'] == 'dp':
                logger.warn('skip share: volume type is dp', share_id=share_id)
                continue
            if is_utcts_recent(share['updated_at'] or share['created_at'], 3600):
                logger.warn('skip share: updated/created less than one hour ago', share_id=share_id)
                continue

            # Below, comparing share size (integer from Manila db) and volume
            # size (float) is correct because python can compare int and float.
            # For example (1 == 1.0) is True.

            size = share['size']
            status = share['status']
            vsize = share['volume']['size']
            snap_percent = share['volume']['snap_percent']

            # shares with net_capacity feature enabled
            if snap_percent == self.net_capacity_snap_reserve:
                correct_size = (vsize * (100 - self.net_capacity_snap_reserve) / 100)
                if size != correct_size:
                    self._reset_resize_error_state(dry_run, share_id, status)
                    logger.warn("share size != netapp volume size (%d != %d)", size, correct_size,
                                share_id=share_id, snap_reserve=snap_percent)
                    if not dry_run:
                        self.set_share_size(share_id, correct_size)
                        self.MANILA_SYNC_SHARE_SIZE_COUNTER.inc()
            elif snap_percent == 5:
                if size != vsize:
                    self._reset_resize_error_state(dry_run, share_id, status)
                    logger.warn("share size != netapp volume size (%d != %d)", size, vsize,
                                share_id=share_id, snap_reserve=snap_percent)
                    if not dry_run:
                        self.set_share_size(share_id, vsize)
                        self.MANILA_SYNC_SHARE_SIZE_COUNTER.inc()
            else:
                logger.warning("skip share: snap reserve percentage inconclusive",
                               share_id=share_id, snap_reserve=snap_percent)
                continue

    def sync_share_state(self, shares, dry_run=True):
        """ Deal with share in stuck states for more than 15 minutes """
        msg = "ManilaSyncShareState: share=%s, instance=%s status=%s"
        msg_dry_run = "Dry run: " + msg
        skip_msg = "skipping share state %s: %s"
        for share in shares:
            share_status = share['status']
            # TODO: move status filtering to db query
            if share_status not in ['creating', 'error_deleting', 'deleting',
                                    'extending', 'shrinking']:
                continue

            share_id = share['id']
            instances = self._query_share_instances(share_id)
            if len(instances) == 0:
                log.info(
                    skip_msg,
                    share_id,
                    'no share instances found')
                continue
            elif len(instances) == 1:
                instance = instances[0]
                instance_updated_at = instance.get('updated_at')
                if instance_updated_at is None:
                    instance_updated_at = instance.get('created_at')
                if instance_updated_at:
                    if is_utcts_recent(instance_updated_at, 900):
                        log.debug(
                            skip_msg,
                            share_id,
                            'updated/created less than 15 minutes ago')
                        continue
                else:
                    log.info(
                        skip_msg,
                        share_id,
                        'no timestamp found')
                    continue

                instance_id = instance['id']
                instance_status = instance['status']
                if dry_run:
                    log.info(msg_dry_run, share_id, instance_id, instance_status)
                    continue
                log.info(msg, share_id, instance_id, instance_status)
                if share_status == 'error_deleting':
                    self.share_force_delete(share_id)
                    continue

                if share_status == 'extending':
                    self.share_reset_state(share_id, 'extending_error')
                elif share_status == 'shrinking':
                    self.share_reset_state(share_id, 'shrinking_error')
                elif share_status in ['creating', 'deleting']:
                    self.share_reset_state(share_id, 'error')
                    if share_status == 'deleting':
                        self.share_delete(share_id)
            else:
                for instance in instances:
                    instance_updated_at = instance.get('updated_at')
                    if instance_updated_at is None:
                        instance_updated_at = instance.get('created_at')
                    if instance_updated_at:
                        if is_utcts_recent(instance_updated_at, 900):
                            log.debug(
                                skip_msg,
                                share_id,
                                'updated/created less than 15 minutes ago')
                            continue
                    else:
                        log.info(
                            skip_msg,
                            share_id,
                            'no timestamp found')
                        continue

                    if instance.get('replica_state', None) in ['active', None]:
                        log.debug(
                            skip_msg,
                            share_id,
                            'replica is "active" or not set')
                        continue

                    instance_id = instance['id']
                    instance_status = instance['status']
                    if dry_run:
                        log.info(msg_dry_run, share_id, instance_id, instance_status)
                        continue
                    log.info(msg, share_id, instance_id, instance_status)
                    if instance_status == 'error_deleting':
                        self.share_instance_force_delete(instance_id)
                        continue
                    self.share_instance_reset_state(instance_id, 'error')
                    if instance_status == 'deleting':
                        self.share_replica_delete(instance_id)

    def reset_share_replica_state(self, _share, dry_run=True):
        """ Reset share replica (secondary) state to available when backend
        volume is online and its type is DP. Ignore shares that are
        created/updated within 6 hours.

        We do not reset the state of primary replica, because it is not
        guaranteed that the primary replica is of type "rw".

        """
        logger = CustomAdapter(log, {'task': 'reset_share_replica_state', 'dry_run': dry_run})

        for (share_id, replica_id) in _share:
            share = _share[(share_id, replica_id)]
            if share['status'] != 'error':
                continue
            if share['replica_state'] != 'in_sync':
                # only secondary replica that is in_sync
                continue
            if is_utcts_recent(share['updated_at'] or share['created_at'], 6 * 3600):
                # only updated or created more than 6 hours ago
                continue
            if 'volume' not in share:
                continue
            if share['volume']['volume_type'] != 'dp':
                continue
            if share['volume']['volume_state'] != 'online':
                continue
            if share['volume']['size'] == 0:
                continue
            logger.info(
                "set replica status to available", share_id=share_id, replica_id=replica_id,
                old_status='error')
            if not dry_run:
                try:
                    self.manilaclient.share_replicas.reset_state(replica_id, "available")
                except Exception as e:
                    logger.exception(
                        "failed to set replica status to available: %s", e, share_id=share_id,
                        replica_id=replica_id)

    def process_missing_volume(self, shares, dry_run=True):
        """ Set share state to error when backend volume is missing

        Ignore shares that are created/updated within 6 hours.
        """
        missing_volumes = {}

        for (share_id, instance_id), share in shares.items():
            if 'volume' not in share:
                # check if shares are created/updated recently
                if is_utcts_recent(share['updated_at'] or share['created_at'], 6 * 3600):
                    continue

                share_name = share['name']
                share_status = share['status']
                msg = f'ManilaShareMissingVolume: share={share_id}, '\
                    f'instance={instance_id}, status={share_status}'

                if not dry_run:
                    if share_status == 'available':
                        self._reset_share_state(share_id, 'error')
                        share_status = 'error'
                        msg = f'ManilaShareMissingVolume: Set share {share_id} to error'
                else:
                    msg = 'Dry run: ' + msg

                log.info(msg)

                self.manila_missing_volume_shares_gauge.labels(
                    share_id=share_id,
                    instance_id=instance_id,
                    share_name=share_name,
                    share_status=share_status,
                ).set(1)

                missing_volumes[(share_id, instance_id)] = {
                    'share_id': share_id,
                    'instance_id': instance_id,
                    'share_name': share_name,
                    'share_status': share_status
                }

        # remove outdated record from gauge
        for (share_id, instance_id) in self.missing_volumes:
            s = self.missing_volumes[(share_id, instance_id)]
            share_name, share_status = s['share_name'], s['share_status']
            if (share_id, instance_id) not in missing_volumes:
                self.manila_missing_volume_shares_gauge.remove(share_id, instance_id, share_name,
                                                               share_status)

        with self.missing_volumes_lock:
            self.missing_volumes = update_records(self.missing_volumes, missing_volumes)

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
        for vol_key, vol in _offline_volumes.items():
            share = vol.get('share')
            if share is not None:
                if share['deleted_at'] or share['updated_at']:
                    if is_utcts_recent(share['deleted_at'] or share['updated_at'], 6 * 3600):
                        _offline_volume_keys.remove(vol_key)

        # process remaining volume
        offline_volumes = {}
        for vol_key in _offline_volume_keys:
            vol = _offline_volumes[vol_key]
            name, filer, vserver = vol['volume'], vol['filer'], vol['vserver']
            share = vol.get('share')
            if share is not None:
                share_id, status = share['share_id'], share['status']
            else:
                share_id, status = '', ''

            self.manila_offline_volumes_gauge.labels(
                share_id=share_id,
                share_status=status,
                volume=name,
                vserver=vserver,
                filer=filer,
            ).set(1)

            offline_volumes[name] = {
                'volume': name,
                'filer': filer,
                'vserver': vserver,
                'share_id': share_id,
                'status': status,
            }

        for volname, vol in self.offline_volumes.items():
            if volname not in offline_volumes:
                self.manila_offline_volumes_gauge.remove(vol['share_id'], vol['status'],
                                                         vol['filer'], vol['vserver'], vol['volume'])

        with self.offline_volumes_lock:
            self.offline_volumes = update_records(self.offline_volumes, offline_volumes)

    def process_orphan_volumes(self, volumes, dry_run=True):
        """ orphan volumes

        Check if the corresponding manila shares are deleted recently (hard coded as 6 hours).
        @params volumes: Dict[(FilerName, InstanceId), Volume]
        """
        # share instance id
        # volume key (extracted from volume name) is manila instance id
        vol_keys = list(volumes.keys())

        # Shares: List[Share])
        # Share.Keys: share_id, instance_id, deleted_at, status
        shares = self._query_shares_by_instance_ids([instance_id for (_, instance_id) in vol_keys])

        # merge share into volume
        r = re.compile('^manila-share-netapp-(?P<filer>.+)@(?P=filer)#.*')
        for s in shares:
            m = r.match(s['host'])
            if m:
                filer = m.group('filer')
            else:
                continue
            if (filer, s['instance_id']) in volumes:
                volumes[(filer, s['instance_id'])].update({'share': s})

        # loop over vol
        for (filer, instance_id), vol in volumes.items():
            # double check if the manila shares are deleted recently
            if 'share' in vol:
                share = vol['share']
                deleted_at = share.get('deleted_at', None)
                if deleted_at is not None:
                    if (datetime.utcnow() - deleted_at).total_seconds() < 6 * 3600:
                        vol_keys.remove((filer, instance_id))

        orphan_volumes = {}
        for vol_key in vol_keys:
            vol = volumes[vol_key]
            volume, vserver, filer = vol['volume'], vol['vserver'], vol['filer']
            if 'share' in vol:
                share_id = vol['share']['share_id']
                share_deleted = vol['share']['deleted']
                share_deleted_at = vol['share']['deleted_at']
                instance_id = vol['share']['instance_id']
                instance_status = vol['share']['status']
            else:
                share_id, share_deleted, share_deleted_at, instance_id, instance_status = None, None, None, None, ''

            self.manila_orphan_volumes_gauge.labels(
                share_id=share_id,
                share_status=instance_status,
                filer=filer,
                vserver=vserver,
                volume=volume,
            ).set(1)

            orphan_volumes[vol_key] = {
                'filer': filer,
                'vserver': vserver,
                'volume': volume,
                'share_id': share_id,
                'share_deleted': share_deleted,
                'share_deleted_at': share_deleted_at,
                'instance_id': instance_id,
                'instance_status': instance_status,
            }

        for k, vol in self.orphan_volumes.items():
            if k not in orphan_volumes:
                self.manila_orphan_volumes_gauge.remove(vol['share_id'], vol['instance_status'],
                                                        vol['filer'], vol['vserver'], vol['volume'])

        with self.orphan_volumes_lock:
            self.orphan_volumes = update_records(self.orphan_volumes, orphan_volumes)

    def _get_netapp_volumes(self):
        """ get netapp volumes from prometheus metrics, including both online
        and offline volumes.

        return [<vol>, <vol>, ...]
        """
        query = (
            "netapp_volume_total_bytes{app='netapp-capacity-exporter-manila', volume=~'share.*'} "
            "+ netapp_volume_snapshot_reserved_bytes"
        )
        vol_t_size = self._fetch_prom_metrics(query) or []
        query = "netapp_volume_percentage_snapshot_reserve{app='netapp-capacity-exporter-manila'}"
        snap_percentage = self._fetch_prom_metrics(query) or []
        snap_percentage = {
            vol['metric']['volume']: int(vol['value'][1])
            for vol in snap_percentage if 'volume' in vol['metric']
        }

        return [{
            'volume': vol['metric']['volume'],
            'volume_type': vol['metric'].get('volume_type'),
            'volume_state': vol['metric'].get('volume_state'),
            'vserver': vol['metric'].get('vserver', ''),
            'filer': vol['metric'].get('filer'),
            'size': int(vol['value'][1]) / ONEGB,
            'snap_percent': snap_percentage.get(vol['metric']['volume']),
        } for vol in vol_t_size]

    def _get_netapp_volumes_offline(self):
        """ like _get_netapp_volumes, but only return offline volumes """
        query = "netapp_volume_state{app='netapp-capacity-exporter-manila'}==3"
        offline_vols = self._fetch_prom_metrics(query) or []
        return [{
            'volume': vol['metric']['volume'],
            'vserver': vol['metric'].get('vserver', ''),
            'filer': vol['metric'].get('filer'),
        } for vol in offline_vols
            if 'volume' in vol['metric'] and vol['metric']['volume'].startswith('share_')]

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
                         'deleted', 'status', 'host'], Any]
        """
        shares_t = Table('shares', self.db_metadata, autoload=True)
        instances_t = Table('share_instances', self.db_metadata, autoload=True)
        q = select([shares_t.c.id.label('share_id'),
                    shares_t.c.created_at,
                    shares_t.c.updated_at,
                    shares_t.c.deleted_at,
                    shares_t.c.deleted,
                    instances_t.c.status,
                    instances_t.c.id.label('instance_id'),
                    instances_t.c.host,
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
                       instances.c.host,
                       instances.c.replica_state,
                       ])\
            .select_from(
                shares.join(instances, shares.c.id == instances.c.share_id))\
            .where(shares.c.deleted == 'False')

        shares = []
        for (sid, name, size, ctime, utime, siid, status, host, replica_state) in stmt.execute():
            shares.append({
                'id': sid,
                'name': name,
                'size': size,
                'created_at': ctime,
                'updated_at': utime,
                'instance_id': siid,
                'status': status,
                'host': host,
                'replica_state': replica_state,
            })
        return shares

    def _query_share_instances(self, share_id):
        """ Get share instances for given share and that are not deleted """

        instances = Table('share_instances', self.db_metadata, autoload=True)
        stmt = select([instances.c.id,
                       instances.c.share_id,
                       instances.c.status,
                       instances.c.updated_at,
                       instances.c.created_at,
                       instances.c.host,
                       instances.c.replica_state,
                       ])\
            .where(instances.c.share_id == share_id) \
            .where(instances.c.deleted == 'False')

        share_instances = []
        for (iid, sid, status, utime, ctime, host, replica_state) in stmt.execute():
            share_instances.append({
                'id': iid,
                'share_id': sid,
                'status': status,
                'updated_at': utime,
                'created_at': ctime,
                'host': host,
                'replica_state': replica_state,
            })
        return share_instances

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
            volumes: Dict[(host, VolumeName): Volume]
        """
        _shares = {(s['id'], s['instance_id']): s for s in shares}
        _volumes = {(vol['filer'], vol['volume'][6:].replace('_', '-')): vol
                    for vol in volumes if vol['volume'].startswith('share_')}
        for (share_id, instance_id), share in _shares.items():
            # complete share host should be like "manila-share-netapp-stnpca4-st061@stnpca4-st061#aggr_ssd_stnpa4_01_st061_1"
            # but it can also be imcomplete, e.g. "manila-share-netapp-stnpca4-st061@stnpca4-st061"
            try:
                filer = share['host'].replace('#', '@').split('@')[1]
            except IndexError:
                log.warning("share %s: invalid host %s", share_id, share['host'])
                continue
            vol = _volumes.pop((filer, instance_id), None)
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
            .where(shares_t.c.id == share_id) \
            .execute()

    def _reset_resize_error_state(self, dry_run, share_id, state):
        if state in ['shrinking_error', 'extending_error']:
            state_msg = f"share {share_id}: reset state from '{state}' to 'available'"
            if not dry_run:
                self._reset_share_state(share_id, 'available')
            else:
                state_msg = 'Dry run: ' + state_msg
            log.info(state_msg)

    def _reset_share_state(self, share_id, state):
        try:
            self.manilaclient.shares.reset_state(share_id, state)
        except Exception as e:
            log.exception("_reset_share_state(share_id=%s, state=%s): %s", share_id, state, e)

    @response
    def get_orphan_volumes(self):
        with self.orphan_volumes_lock:
            orphan_volumes = list(self.orphan_volumes.values())
        return orphan_volumes

    @response
    def get_offline_volumes(self):
        with self.offline_volumes_lock:
            offline_volumes = list(self.offline_volumes.values())
        return offline_volumes

    @response
    def get_missing_volume_shares(self):
        with self.missing_volumes_lock:
            missing_volumes = list(self.missing_volumes.values())
        return sorted(missing_volumes, key=lambda v: v['share_id'])


def str2bool(val):
    if isinstance(val, bool):
        return val
    if val.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    if val.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    raise argparse.ArgumentTypeError('Boolean value expected.')


def parse_cmdline_args():
    parser = base_command_parser()
    parser.add_argument("--netapp-prom-host",
                        default='http://prometheus-storage.infra-monitoring.svc:9090',
                        help="prometheus host for netapp metrics")
    parser.add_argument("--task-share-size",
                        type=str2bool,
                        default=False,
                        help="enable share size task")
    parser.add_argument("--task-missing-volume",
                        type=str2bool,
                        default=False,
                        help="enable missing-volume task")
    parser.add_argument("--task-offline-volume",
                        type=str2bool,
                        default=False,
                        help="enable offline-volume task")
    parser.add_argument("--task-orphan-volume",
                        type=str2bool,
                        default=False,
                        help="enable orphan-volume task")
    parser.add_argument("--task-share-state",
                        type=str2bool,
                        default=False,
                        help="enable share state task")
    parser.add_argument("--task-share-size-dry-run",
                        type=str2bool,
                        default=True,
                        help="dry run mode for share size task")
    parser.add_argument("--task-missing-volume-dry-run",
                        type=str2bool,
                        default=True,
                        help="dry run mode for missing-volume task")
    parser.add_argument("--task-offline-volume-dry-run",
                        type=str2bool,
                        default=True,
                        help="dry run mode for offline-volume task")
    parser.add_argument("--task-orphan-volume-dry-run",
                        type=str2bool,
                        default=True,
                        help="dry run mode for orphan-volume task")
    parser.add_argument("--task-share-state-dry-run",
                        type=str2bool,
                        default=True,
                        help="dry run mode for share state task")
    parser.add_argument("--debug", action="store_true",
                        help="add additional debug output")
    return parser.parse_args()


def main():
    args = parse_cmdline_args()
    tasks = {
        TASK_SHARE_SIZE: args.task_share_size,
        TASK_MISSING_VOLUME: args.task_missing_volume,
        TASK_OFFLINE_VOLUME: args.task_offline_volume,
        TASK_ORPHAN_VOLUME: args.task_orphan_volume,
        TASK_SHARE_STATE: args.task_share_state,
    }
    dry_run_tasks = {
        TASK_SHARE_SIZE: args.task_share_size_dry_run,
        TASK_MISSING_VOLUME: args.task_missing_volume_dry_run,
        TASK_OFFLINE_VOLUME: args.task_offline_volume_dry_run,
        TASK_ORPHAN_VOLUME: args.task_orphan_volume_dry_run,
        TASK_SHARE_STATE: args.task_share_state_dry_run,
    }

    if args.debug:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    ManilaShareSyncNanny(
        args.config,
        args.netapp_prom_host,
        args.interval,
        tasks,
        dry_run_tasks,
        prom_port=args.prom_port
    ).run()


if __name__ == "__main__":
    main()
