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
import http.server
import socketserver
from http.server import BaseHTTPRequestHandler
from threading import Lock
from typing import Dict, List, Tuple

from sqlalchemy import Table, and_, func, select

from manilananny import ManilaNanny, response


class MyHandler(BaseHTTPRequestHandler):
    ''' http server handler '''
    def do_GET(self):
        if self.path == '/':
            status_code, header, data = self.server.orphan_share_servers()
        else:
            status_code, header, data = self.server.undefined_route(self.path)
        self.send_response(status_code)
        self.send_header(*header)
        self.end_headers()
        self.wfile.write(data.encode('utf-8'))


class ManilaShareServerNanny(ManilaNanny):
    """ Manila Share Server """
    def __init__(self, config_file, interval, handler=None):
        super(ManilaShareServerNanny, self).__init__(config_file, interval, handler=handler)
        self.orpha_share_servers_lock = Lock()
        self.orpha_share_servers: List[str] = []

    def _run(self):
        s = self.query_share_server_count_share_instance()
        self.orpha_share_servers = [
            share_server_id
            for (share_server_id, count) in s
            if count == 0]

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
    def orphan_share_servers(self):
        return ['abc', 'def']


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
    return parser.parse_args()

def main():
    args = parse_cmdline_args()

    ManilaShareServerNanny(
        args.config,
        args.interval,
        handler=MyHandler
    ).run()


if __name__ == "__main__":
    main()
