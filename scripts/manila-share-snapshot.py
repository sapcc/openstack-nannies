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
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from threading import Lock
from typing import Dict

from prometheus_client import Gauge
from sqlalchemy import Table, select

from manilananny import ManilaNanny, response, update_records

TASK_SHARE_SNAPSHOT_STATE = '1'

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
        self._task = tasks
        self._task_dry_run = dry_run_tasks
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
            dry_run = self._dry_run_tasks[TASK_SHARE_SNAPSHOT_STATE]
            self.sync_share_snapshot_state(dry_run)

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

    def sync_share_snapshot_state(self, dry_run=True):
        """ Deal with share snapshot in stuck states for more than 15 minutes """
        msg = "ManilaSyncSnapshotState: snapshot=%s, instance=%s, status=%s"
        msg_dry_run = "Dry run: " + msg
        share_snapshots = self.list_share_snapshots()
        for snapshot in share_snapshots:
            snapshot_status = getattr(snapshot, 'status', None)
            if snapshot_status not in ['creating', 'error_deleting', 'deleting']:
                continue

            snapshot_id = getattr(snapshot, 'id', None)
            instances = self.list_share_snapshot_instances(snapshot_id)
            if len(instances) == 0:
                continue
            elif len(instances) == 1:
                instance = instances[0]
                instance_updated_at = getattr(instance, 'updated_at', None)
                if instance_updated_at is not None:
                    try:
                        instance_updated_at = datetime.strptime(
                            instance_updated_at, "%Y-%m-%dT%H:%M:%S.%f")
                        if is_utcts_recent(instance_updated_at, 900):
                            continue
                    except ValueError:
                            continue
                else:
                    continue

                instance_id = getattr(instance, 'id', None)
                instance_status = getattr(instance, 'status', None)
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
                    if instance_updated_at is not None:
                        try:
                            instance_updated_at = datetime.strptime(
                                instance_updated_at, "%Y-%m-%dT%H:%M:%S.%f")
                            if is_utcts_recent(instance_updated_at, 900):
                                continue
                        except ValueError:
                                continue
                    else:
                        continue
                    instance_id = getattr(instance, 'id', None)
                    instance_status = getattr(instance, 'status', None)
                    if dry_run:
                        log.info(msg_dry_run, snapshot_id, instance_id, instance_status)
                        continue
                    log.info(msg, snapshot_id, instance_id, instance_status)


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
