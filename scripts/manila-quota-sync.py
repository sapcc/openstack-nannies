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
# based on https://github.com/cernops/cinder-quota-sync

import argparse
import sys
import ConfigParser
import datetime
import logging
import time

from prettytable import PrettyTable
from prometheus_client import start_http_server, Counter
from sqlalchemy import and_
from sqlalchemy import delete
from sqlalchemy import func
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import join
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import false
from sqlalchemy.ext.declarative import declarative_base
from manila_nanny import ManilaNanny, get_db_url

class ManilaQuotaSyncNanny(ManilaNanny):
    def __init__(self, db_url, interval, dry_run):
        super(ManilaQuotaSyncNanny, self).__init__(db_url, interval, dry_run)
        self.MANILA_QUOTA_BY_USER_SYNCED = Counter('manila_nanny_user_quota_synced', '')
        self.MANILA_QUOTA_BY_TYPE_SYNCED = Counter('manila_nanny_share_type_quota_synced', '')

    def get_share_networks_usages_project(self, project_id):
        """Return the share_networks resource usages of a project"""
        networks_t = Table('share_networks', self.db_metadata, autoload=True)
        networks_q = select(columns=[networks_t.c.id,
                                     networks_t.c.user_id],
                            whereclause=and_(
                                    networks_t.c.deleted == "False",
                                    networks_t.c.project_id == project_id))
        return networks_q.execute()

    def get_snapshot_usages_project(self, project_id):
        """Return the snapshots resource usages of a project"""
        snapshots_t = Table('share_snapshots', self.db_metadata, autoload=True)
        share_instances_t = Table('share_instances', self.db_metadata, autoload=True)
        q = snapshots_t.join(share_instances_t, snapshots_t.c.share_id == share_instances_t.c.share_id)
        snapshots_q = select(columns=[snapshots_t.c.id,
                                      snapshots_t.c.user_id,
                                      snapshots_t.c.share_size,
                                      share_instances_t.c.share_type_id],
                             whereclause=and_(
                                    snapshots_t.c.deleted == "False",
                                    snapshots_t.c.project_id == project_id)
                            ).select_from(q)
        return snapshots_q.execute()

    def get_share_usages_project(self, project_id):
        """Return the share resource usages of a project"""
        shares_t = Table('shares', self.db_metadata, autoload=True)
        share_instances_t = Table('share_instances', self.db_metadata, autoload=True)
        q = shares_t.join(share_instances_t, shares_t.c.id == share_instances_t.c.share_id)
        shares_q = select(columns=[shares_t.c.id,
                                   shares_t.c.user_id,
                                   shares_t.c.size,
                                   share_instances_t.c.share_type_id],
                          whereclause=and_(
                                shares_t.c.deleted == "False",
                                shares_t.c.project_id == project_id)
                         ).select_from(q)
        return shares_q.execute()

    def get_quota_usages_project(self, project_id):
        """Return the quota usages of a project"""
        quota_usages_t = Table('quota_usages', self.db_metadata, autoload=True)
        quota_usages_q = select(columns=[quota_usages_t.c.resource,
                                         quota_usages_t.c.user_id,
                                         quota_usages_t.c.share_type_id,
                                         quota_usages_t.c.in_use],
                                whereclause=and_(
                                        quota_usages_t.c.deleted == 0,
                                        quota_usages_t.c.project_id == project_id))
        return quota_usages_q.execute()

    def get_resource_types(self, project_id):
        """Return a list of all resource types"""
        quota_usages_t = Table('quota_usages', self.db_metadata, autoload=True)
        resource_types_q = select(columns=[quota_usages_t.c.resource,
                                           func.count()],
                                  whereclause=quota_usages_t.c.deleted == 0,
                                  group_by=quota_usages_t.c.resource)
        return [resource for (resource, _) in resource_types_q.execute()]

    def get_projects(self):
        """Return a list of all projects in the database"""
        shares_t = Table('shares', self.db_metadata, autoload=True)
        shares_q = select(columns=[shares_t.c.project_id]).group_by(shares_t.c.project_id)
        return [project[0] for project in shares_q.execute()]

    def sync_quota_usages_project(self, project_id, quota_to_sync_by_user, quota_to_sync_by_type):
        """Sync the quota usages of a project from real usages"""
        print("Syncing %s" % (project_id))
        now = datetime.datetime.utcnow()
        quota_usages_t = Table('quota_usages', self.db_metadata, autoload=True)
        # a tuple is used here to have a dict value per project and user
        for resource_tuple, quota in quota_to_sync_by_user.iteritems():
            quota_usages_t.update().values(updated_at=now, in_use=quota).where(and_(
                    quota_usages_t.c.project_id == project_id,
                    quota_usages_t.c.resource == resource_tuple[0],
                    quota_usages_t.c.user_id == resource_tuple[1])).execute()
        for (resource, share_type_id), quota in quota_to_sync_by_type.iteritems():
            quota_usages_t.update().values(updated_at=now, in_use=quota).where(and_(
                quota_usages_t.c.project_id == project_id,
                quota_usages_t.c.resource == resource,
                quota_usages_t.c.share_type_id == share_type_id, 
            )).execute()
    def sync_quota_usages_by_type(self, project_id, quota_to_sync):
        print("Syncing %s" % (project_id))
        now = datetime.datetime.utcnow()
        quota_usages_t = Table('quota_usages', self.db_metadata, autoload=True)

    def _run(self):
        # prepare the output
        ptable_user = PrettyTable(["Project ID", "User ID", "Resource", "Quota -> Real", "Sync Status"])
        ptable_type = PrettyTable(["Project ID", "Share Type ID", "Resource", "Quota -> Real", "Sync Status"])

        try:
            projects = self.get_projects()
        except sqlalchemy.exc.OperationalError:
            self.makeConnection()
            projects = self.get_projects()

        for project_id in projects:
            # get the quota usage of a project
            quota_usages = {}
            for (resource, user, share_type, count) in self.get_quota_usages_project(project_id):
                quota_usages[(resource, user, share_type)] = quota_usages.get((resource, user, share_type), 0) + count

            # get the real usage of a project
            real_usages = {}
            for (_, user, size, share_type_id) in self.get_share_usages_project(project_id):
                real_usages[("shares", user, share_type_id)] = real_usages.get(("shares", user, share_type_id), 0) + 1
                real_usages[("gigabytes", user, share_type_id)] = real_usages.get(("gigabytes", user, share_type_id), 0) + size
            for (_, user, size, share_type_id) in self.get_snapshot_usages_project(project_id):
                real_usages[("snapshots",user, share_type_id)] = real_usages.get(("snapshots",user, share_type_id), 0) + 1
                real_usages[("snapshot_gigabytes", user, share_type_id)] = real_usages.get(("snapshot_gigabytes", user, share_type_id), 0) + size
            for (_, user) in self.get_share_networks_usages_project(project_id):
                real_usages[("share_networks", user, None)] = real_usages.get(("share_networks", user, None), 0) + 1

            # find discrepancies between quota usage and real usage
            quota_usages_by_user_to_sync = {}
            quota_usages_by_type_to_sync = {}

            quota_usages_by_user = { (r, u): q for (r, u, _), q in quota_usages.iteritems() if u != None }
            quota_usages_by_type = { (r, t): q for (r, _, t), q in quota_usages.iteritems() if t != None }
            quota_usages_by_user_sorted_keys = sorted([k for k in quota_usages_by_user.keys()], key=lambda k: k[1])
            quota_usages_by_type_sorted_keys = sorted([k for k in quota_usages_by_type.keys()], key=lambda k: k[1])

            real_usages_by_user = {}
            for (r, u, t), q in real_usages.iteritems():
                real_usages_by_user[(r, u)] = real_usages_by_user.get((r, u), 0) + q
            real_usages_by_type = {}
            for (r, u, t), q in real_usages.iteritems():
                if t != None:
                    real_usages_by_type[(r, t)] = real_usages_by_type.get((r, t), 0) + q

            for resource, user in quota_usages_by_user_sorted_keys:
                quota = quota_usages_by_user[(resource, user)]
                real_quota = real_usages_by_user.get((resource, user), 0)
                if quota != real_quota:
                    quota_usages_by_user_to_sync[(resource, user)] = real_quota
                    ptable_user.add_row([project_id, user, resource,
                                    str(quota) + ' -> ' + str(real_quota),
                                    '\033[1m\033[91mMISMATCH\033[0m'])
                    if not self.dry_run:
                        self.MANILA_QUOTA_BY_USER_SYNCED.inc()

            for resource, type in quota_usages_by_type_sorted_keys:
                quota = quota_usages_by_type[(resource, type)]
                real_quota = real_usages_by_type.get((resource, type), 0)
                if quota != real_quota:
                    quota_usages_by_type_to_sync[(resource, type)] = real_quota
                    ptable_type.add_row([project_id, type, resource,
                                    str(quota) + ' -> ' + str(real_quota),
                                    '\033[1m\033[91mMISMATCH\033[0m'])
                    if not self.dry_run:
                        self.MANILA_QUOTA_BY_TYPE_SYNCED.inc()

            # sync the quota with the real usage
            if not self.dry_run:
                if len(quota_usages_by_type_to_sync) > 0 or len(quota_usages_by_user_to_sync) > 0:
                    self.sync_quota_usages_project(project_id,
                                                   quota_usages_by_user_to_sync,
                                                   quota_usages_by_type_to_sync)

        # format output
        print(ptable_user)
        print(ptable_type)


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

def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--config",
                            default='./manila.conf',
                            help='configuration file')
        parser.add_argument("--dry-run",
                            action="store_true",
                            help="never sync resources (no interactive check)")
        parser.add_argument("--interval",
                            default=600,
                            type=float,
                            help="interval")
        parser.add_argument("--promport",
                            default=9456,
                            type=int,
                            help="prometheus port")
        args = parser.parse_args()
    except Exception as e:
        sys.stdout.write("Check command line arguments (%s)" % e)

    print(args)

    try:
        start_http_server(args.promport)
    except Exception as e:
        logging.fatal("start_http_server: " + str(e))
        sys.exit(-1)

    # args.dry_run = True
    db_url = get_db_url(args.config)
    ManilaQuotaSyncNanny(db_url, args.interval, args.dry_run).run()

if __name__ == "__main__":
    main()
