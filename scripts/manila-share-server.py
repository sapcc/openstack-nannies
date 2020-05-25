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
from http.server import BaseHTTPRequestHandler
from threading import Lock
from typing import Dict, List, Tuple
import datetime

from sqlalchemy import Table, and_, func, select

from manilananny import ManilaNanny, response


class MyHandler(BaseHTTPRequestHandler):
    ''' http server handler '''
    def do_GET(self):
        if self.path == '/':
            status_code, header, data = self.server.get_orphan_share_servers()
        else:
            status_code, header, data = self.server.undefined_route(self.path)
        self.send_response(status_code)
        self.send_header(*header)
        self.end_headers()
        self.wfile.write(data.encode('utf-8'))


class ManilaShareServerNanny(ManilaNanny):
    """ Manila Share Server """
    def __init__(self, config_file, interval, port, handler):
        super(ManilaShareServerNanny, self).__init__(config_file, interval, port=port, handler=handler)
        self.orphan_share_servers_lock = Lock()
        self.orphan_share_servers: Dict[str, Dict[str, str]] = {}

    def _run(self):
        s = self.query_share_server_count_share_instance()
        orphan_share_servers = {
            share_server_id: {
                'share_server_id': share_server_id,
                'first_seen_ts': datetime.datetime.utcnow()
            }
            for (share_server_id, count) in s
            if count == 0}
        with self.orphan_share_servers_lock:
            self.orphan_share_servers = update_dict(self.orphan_share_servers, orphan_share_servers)

    def query_share_server_count_share_instance(self) -> List[Tuple[str, int]]:
        """ share servers and count of undeleted share instances """
        instances_t = Table('share_instances', self.db_metadata, autoload=True)
        s_servers_t = Table('share_servers', self.db_metadata, autoload=True)

        q = select([s_servers_t.c.id.label('ssid'),
                    func.count(instances_t.c.id)])\
            .select_from(
                s_servers_t.outerjoin(instances_t,
                                      and_(instances_t.c.share_server_id == s_servers_t.c.id,
                                           instances_t.c.deleted == 'False')))\
            .where(s_servers_t.c.deleted == 'False')\
            .group_by('ssid')
        r = q.execute()
        return list(r)

    @response
    def get_orphan_share_servers(self):
        with self.orphan_share_servers_lock:
            orphan_share_servers = list(self.orphan_share_servers.values())
        return orphan_share_servers

def update_dict(target_dict, new_dict):
    old_dict = target_dict
    target_dict = {}
    for key in new_dict:
        if key in old_dict:
            target_dict[key] = old_dict[key]
        else:
            target_dict[key] = new_dict[key]
    return target_dict

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
    parser.add_argument("--interval",
                        type=float,
                        default=3600,
                        help="interval")
    parser.add_argument("--port",
                        type=int,
                        default=8000,
                        help="http server listen port")
    return parser.parse_args()

def main():
    args = parse_cmdline_args()

    ManilaShareServerNanny(
        args.config,
        args.interval,
        port=args.port,
        handler=MyHandler
    ).run()


if __name__ == "__main__":
    main()
