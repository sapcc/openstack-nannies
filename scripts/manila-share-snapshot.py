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
import logging
from http.server import BaseHTTPRequestHandler
from threading import Lock
from typing import Dict

from manilananny import ManilaNanny, is_utcts_recent, response, update_records
from prometheus_client import Gauge
from sqlalchemy import Table, select

TASK_SHARE_SNAPSHOT_STATE = '1'

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')


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


class ManilaShareSnapshotNanny(ManilaNanny):
    """ Manila Share Snapshot """
    def __init__(self, config_file, interval, tasks, dry_run_tasks, prom_port, http_port, handler):
        super(ManilaShareSnapshotNanny, self).__init__(config_file,
                                                       interval,
                                                       prom_port=prom_port,
                                                       http_port=http_port,
                                                       handler=handler)
        self.orphan_snapshots_lock = Lock()
        self.orphan_snapshots: Dict[str, Dict[str, str]] = {}
        self.orphan_snapshots_gauge = Gauge('manila_nanny_orphan_share_snapshots',
                                            'Orphan Manila Share Snapshots',
                                            ['share_id', 'snapshot_id'])
        self._tasks = tasks
        self._dry_run_tasks = dry_run_tasks
        if not any(tasks.values()):
            raise Exception('All tasks are disabled')

    def _run(self):
        s = self.query_orphan_snapshots()
        orphan_snapshots = {
            snapshot_id: {'snapshot_id': snapshot_id, 'share_id': share_id}
            for snapshot_id, share_id in s}
        for snapshot_id in orphan_snapshots:
            share_id = orphan_snapshots[snapshot_id]['share_id']
            self.orphan_snapshots_gauge.labels(share_id=share_id, snapshot_id=snapshot_id).set(1)
        for snapshot_id in self.orphan_snapshots:
            if snapshot_id not in orphan_snapshots:
                share_id = self.orphan_snapshots[snapshot_id]['share_id']
                self.orphan_snapshots_gauge.remove(share_id, snapshot_id)
        with self.orphan_snapshots_lock:
            self.orphan_snapshots = update_records(self.orphan_snapshots, orphan_snapshots)

        if self._tasks[TASK_SHARE_SNAPSHOT_STATE]:
            snapshots = self._query_share_snapshots()
            dry_run = self._dry_run_tasks[TASK_SHARE_SNAPSHOT_STATE]
            self.sync_share_snapshot_state(snapshots, dry_run)

    def _query_share_snapshots(self):
        Snapshots = Table('share_snapshots', self.db_metadata, autoload=True)
        instances = Table('share_snapshot_instances', self.db_metadata, autoload=True)
        q = select([Snapshots.c.id,
                    Snapshots.c.share_id,
                    Snapshots.c.display_name,
                    Snapshots.c.size,
                    Snapshots.c.created_at,
                    Snapshots.c.updated_at,
                    instances.c.id,
                    instances.c.status,
                    ])\
            .select_from(
                Snapshots.join(instances, Snapshots.c.id == instances.c.snapshot_id))\
            .where(Snapshots.c.deleted == 'False')

        snapshots = []
        for (ssid, sid, name, size, ctime, utime, siid, status) in q.execute():
            snapshots.append({
                'id': ssid,
                'share_id': sid,
                'name': name,
                'size': size,
                'created_at': ctime,
                'updated_at': utime,
                'instance_id': siid,
                'status': status,
            })
        return snapshots

    def _query_share_snapshot_instances(self, snapshot_id):
        """ Get snapshot instances for given snapshot and that are not deleted """
        instances = Table('share_snapshot_instances', self.db_metadata, autoload=True)
        stmt = select([instances.c.id,
                       instances.c.snapshot_id,
                       instances.c.status,
                       instances.c.share_instance_id,
                       instances.c.updated_at,
                       ])\
            .where(instances.c.snapshot_id == snapshot_id) \
            .where(instances.c.deleted == 'False')

        snapshot_instances = []
        for (ssid, sid, status, siid, utime) in stmt.execute():
            snapshot_instances.append({
                'id': ssid,
                'snapshot_id': sid,
                'status': status,
                'share_instance_id': siid,
                'updated_at': utime,
            })
        return snapshot_instances

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

    def sync_share_snapshot_state(self, share_snapshots, dry_run=True):
        """ Deal with share snapshot in stuck states for more than 15 minutes """
        msg = "ManilaSyncSnapshotState: snapshot=%s, instance=%s, status=%s"
        msg_dry_run = "Dry run: " + msg
        for snapshot in share_snapshots:
            snapshot_status = snapshot['status']
            if snapshot_status not in ['creating', 'error_deleting', 'deleting']:
                continue

            snapshot_id = snapshot['id']
            instances = self._query_share_snapshot_instances(snapshot_id)
            if len(instances) == 0:
                continue
            elif len(instances) == 1:
                instance = instances[0]
                instance_updated_at = instance['updated_at']
                if instance_updated_at is not None:
                    if is_utcts_recent(instance_updated_at, 900):
                        continue
                else:
                    continue
                instance_id = instance['id']
                instance_status = instance['status']
                if dry_run:
                    log.info(msg_dry_run, snapshot_id, instance_id, instance_status)
                    continue
                log.info(msg, snapshot_id, instance_id, instance_status)
                if snapshot_status == 'error_deleting':
                    self.share_snapshot_force_delete(snapshot_id)
                    continue
                self.share_snapshot_reset_state(snapshot_id, 'error')
                if snapshot_status  == 'deleting':
                    self.share_snapshot_delete(snapshot_id)
            else:
                for instance in instances:
                    instance_updated_at = instance['updated_at']
                    if instance_updated_at is not None:
                        if is_utcts_recent(instance_updated_at, 900):
                            continue
                    else:
                        continue
                    instance_id = instance['id']
                    instance_status = instance['status']
                    if dry_run:
                        log.info(msg_dry_run, snapshot_id, instance_id, instance_status)
                        continue
                    log.info(msg, snapshot_id, instance_id, instance_status)
                    if snapshot_status == 'error_deleting':
                        continue
                    self.share_snapshot_instance_reset_state(instance_id, 'error')


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
    parser.add_argument("--config", default='/etc/manila/manila.conf', help='configuration file')
    parser.add_argument("--interval", type=float, default=3600, help="interval")
    parser.add_argument("--listen-port", type=int, default=8000, help="http server listen port")
    parser.add_argument("--prom-port", type=int, default=9000, help="http server listen port")
    parser.add_argument("--task-share-snapshot-state", type=str2bool, default=False,
                        help="enable share snapshot state task")
    parser.add_argument("--task-share-snapshot-state-dry-run", type=str2bool, default=False,
                        help="dry run mode for share snapshot state task")
    return parser.parse_args()


def main():
    args = parse_cmdline_args()
    tasks = {
        TASK_SHARE_SNAPSHOT_STATE: args.task_share_snapshot_state,
    }
    dry_run_tasks = {
        TASK_SHARE_SNAPSHOT_STATE: args.task_share_snapshot_state_dry_run,
    }

    ManilaShareSnapshotNanny(
        args.config,
        args.interval,
        tasks,
        dry_run_tasks,
        prom_port=args.prom_port,
        http_port=args.listen_port,
        handler=MyHandler
    ).run()


if __name__ == "__main__":
    main()
