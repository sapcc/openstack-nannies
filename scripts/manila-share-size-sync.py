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
import ConfigParser
import datetime
import time
import requests

# from prettytable import PrettyTable
from sqlalchemy import and_
from sqlalchemy import delete
from sqlalchemy import func
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import false
from sqlalchemy.ext.declarative import declarative_base

query = 'netapp_capacity_svm{metric="size_total"} + ignoring(metric) netapp_capacity_svm{metric="size_reserved_by_snapshots"}'
onegb = 1073741824

class SharesController():
    _shares = {}

    def __init__(self, session, metadata, prom_host, prom_query):
        self.db_session = session 
        self.db_meta = metadata
        self.prom_host = prom_host+"/api/v1/query"
        self.prom_query = prom_query

    def get_shares_from_netapp(self):
        payloads = {
            'query': self.prom_query,
            'time': time.time()
        }
        r = requests.get(self.prom_host, params=payloads)

        # print r.status_code
        # print r.reason
        # print r.request.url

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
        shares_t = Table('shares', self.db_meta, autoload=True)
        share_instances_t = Table('share_instances', self.db_meta, autoload=True)
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
        shares_t = Table('shares', self.db_meta, autoload=True)
        share_instances_t = Table('share_instances', self.db_meta, autoload=True)
        shares_t.update() \
                .values(updated_at=now, size=share_size) \
                .where(shares_t.c.id == share_instances_t.c.share_id) \
                .where(and_(shares_t.c.id == share_id, share_instances_t.c.status == 'available')) \
                .execute()

def makeConnection(db_url):

    """Establish a database connection and return the handle"""

    engine = create_engine(db_url)
    engine.connect()
    Session = sessionmaker(bind=engine)
    thisSession = Session()
    metadata = MetaData()
    metadata.bind = engine
    Base = declarative_base()
    tpl = thisSession, metadata, Base
    return tpl


def get_db_url(config_file):

    """Return the database connection string from the config file"""

    parser = ConfigParser.SafeConfigParser()
    try:
        parser.read(config_file)
        db_url = parser.get('database', 'connection', raw=True)
    except:
        print "ERROR: Check Manila configuration file."
        sys.exit(2)
    return db_url


def parse_cmdline_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",
                        default='/manila-etc/manila.conf',
                        help='configuration file')
    parser.add_argument("--promhost",
                        help="never sync resources (no interactive check)")
    parser.add_argument("--promquery",
                        default=query,
                        help="always sync resources (no interactive check)")
    parser.add_argument("--dry-run",
                       action="store_true",
                       help='print only what would be done without actually doing it')
    return parser.parse_args()


def main():
    try:
        args = parse_cmdline_args()
    except Exception as e:
        sys.stdout.write("Check command line arguments (%s)" % e.strerror)

    # connect to the DB
    db_url = get_db_url(args.config)
    manila_session, manila_metadata, manila_Base = makeConnection(db_url)

    ctl = SharesController(manila_session, manila_metadata, args.promhost, args.promquery)
    ctl.get_shares_from_netapp()
    ctl.get_shares_from_manila()

    for share_id, v in ctl._shares.items():
        if v.get('manila_size') is not None and v.get('size') is not None:
            if v.get('manila_size') != v.get('size'):
                if (datetime.datetime.utcnow() - v.get('updated_at')).total_seconds() > 600 \
                    and args.dry_run is False:
                    print ("share %s: manila share size (%d) does not " + \
                        "match share size (%d) on backend, fixing ...") % (\
                        share_id, v.get('manila_size'), v.get('size'))
                    ctl.set_share_size(share_id, v.get('size'))
                else:
                    print ("share %s: manila share size (%d) does not " + \
                           "match share size (%d) on backend") % (\
                           share_id, v.get('manila_size'), v.get('size'))

def test_resize():
    try:
        args = parse_cmdline_args()
    except Exception as e:
        sys.stdout.write("Check command line arguments (%s)" % e.strerror)

    # connect to the DB
    db_url = get_db_url(args.config)
    manila_session, manila_metadata, manila_Base = makeConnection(db_url)

    ctl = SharesController(manila_session, manila_metadata, args.promhost, args.promquery)
    ctl.set_share_size('7eb50f3b-b5ea-47e2-a6e9-5934de57c777', 4)


if __name__ == "__main__":
    # test_resize()
    main()
