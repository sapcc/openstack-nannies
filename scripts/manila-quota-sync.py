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

from prettytable import PrettyTable
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


def get_projects(meta):

    """Return a list of all projects in the database"""

    projects = []
    shares_t = Table('shares', meta, autoload=True)
    shares_q = select(columns=[shares_t.c.project_id]). group_by(shares_t.c.project_id)
    for project in shares_q.execute():
        projects.append(project[0])

    return projects


def yn_choice():

    """Return True/False after checking with the user"""

    yes = set(['yes', 'y', 'ye'])
    no = set(['no', 'n'])

    print "Do you want to sync? [Yes/No]"
    while True:
        choice = raw_input().lower()
        if choice in yes:
            return True
        elif choice in no:
            return False
        else:
            sys.stdout.write("Do you want to sync? [Yes/No/Abort]")


def sync_quota_usages_project(meta, project_id, quota_usages_to_sync):

    """Sync the quota usages of a project from real usages"""

    print "Syncing %s" % (project_id)
    now = datetime.datetime.utcnow()
    quota_usages_t = Table('quota_usages', meta, autoload=True)
    # a tuple is used here to have a dict value per project and user
    for resource_tuple, quota in quota_usages_to_sync.iteritems():
        quota_usages_t.update().where(
            and_(quota_usages_t.c.project_id == project_id,
                 quota_usages_t.c.resource == resource_tuple[0],
            quota_usages_t.c.user_id == resource_tuple[1])).values(
            updated_at=now, in_use=quota).execute()


def get_share_networks_usages_project(meta, project_id):

    """Return the share_networks resource usages of a project"""

    networks_t = Table('share_networks', meta, autoload=True)
    networks_q = select(columns=[networks_t.c.id, networks_t.c.user_id],
                         whereclause=and_(
                         networks_t.c.deleted == "False",
                         networks_t.c.project_id == project_id))
    return networks_q.execute()

def get_snapshot_usages_project(meta, project_id):

    """Return the snapshots resource usages of a project"""

    snapshots_t = Table('share_snapshots', meta, autoload=True)
    snapshots_q = select(columns=[snapshots_t.c.id,
                                  snapshots_t.c.user_id,
                                  snapshots_t.c.share_size],
                         whereclause=and_(
                         snapshots_t.c.deleted == "False",
                         snapshots_t.c.project_id == project_id))
    return snapshots_q.execute()


def get_share_usages_project(meta, project_id):

    """Return the share resource usages of a project"""

    shares_t = Table('shares', meta, autoload=True)
    shares_q = select(columns=[shares_t.c.id,
                                shares_t.c.user_id,
                                shares_t.c.size],
                       whereclause=and_(shares_t.c.deleted == "False",
                                        shares_t.c.project_id == project_id))
    return shares_q.execute()


def get_quota_usages_project(meta, project_id):

    """Return the quota usages of a project"""

    quota_usages_t = Table('quota_usages', meta, autoload=True)
    quota_usages_q = select(columns=[quota_usages_t.c.resource,
                                     quota_usages_t.c.user_id,
                                     quota_usages_t.c.in_use],
                            whereclause=and_(quota_usages_t.c.deleted == 0,
                                             quota_usages_t.c.project_id ==
                                             project_id, quota_usages_t.c.user_id != None))
    return quota_usages_q.execute()


def get_resource_types(meta, project_id):

    """Return a list of all resource types"""

    types = []
    quota_usages_t = Table('quota_usages', meta, autoload=True)
    resource_types_q = select(columns=[quota_usages_t.c.resource,
                                       func.count()],
                              whereclause=quota_usages_t.c.deleted == 0,
                              group_by=quota_usages_t.c.resource)
    for (resource, _) in resource_types_q.execute():
        types.append(resource)
    return types


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
                        default='./manila.conf',
                        help='configuration file')
    parser.add_argument("--nosync",
                        action="store_true",
                        help="never sync resources (no interactive check)")
    parser.add_argument("--sync",
                        action="store_true",
                        help="always sync resources (no interactive check)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--list_projects",
                       action="store_true",
                       help='get a list of all projects in the database')
    group.add_argument("--project_id",
                       type=str,
                       help="project to check")
    return parser.parse_args()


def main():
    try:
        args = parse_cmdline_args()
    except Exception as e:
        sys.stdout.write("Check command line arguments (%s)" % e.strerror)

    # connect to the DB
    db_url = get_db_url(args.config)
    manila_session, manila_metadata, manila_Base = makeConnection(db_url)

    # get the resource types
    resource_types = get_resource_types(manila_metadata,
                                        args.project_id)

    # check/sync all projects found in the database
    #
    if args.list_projects:
        for p in get_projects(manila_metadata):
            print p
        sys.exit(0)

    # check a single project
    #
    print "checking project " + args.project_id + ":"

    # get the quota usage of a project
    quota_usages = {}
    for (resource, user, count) in get_quota_usages_project(manila_metadata,
                                                        args.project_id):
        quota_usages[(resource, user)] = quota_usages.get((resource, user), 0) + count

    # get the real usage of a project
    real_usages = {}
    for (_, user, size) in get_share_usages_project(manila_metadata,
                                                        args.project_id):
        real_usages[("shares", user)] = real_usages.get(("shares", user), 0) + 1
        real_usages[("gigabytes", user)] = real_usages.get(("gigabytes", user), 0) + size

    for (_, user, size) in get_snapshot_usages_project(manila_metadata,
                                                          args.project_id):
        real_usages[("snapshots",user)] = real_usages.get(("snapshots",user), 0) + 1
        real_usages[("snapshot_gigabytes", user)] = real_usages.get(("snapshot_gigabytes", user), 0) + size

    for (_, user) in get_share_networks_usages_project(manila_metadata,
                                                        args.project_id):
        real_usages[("share_networks", user)] = real_usages.get(("share_networks", user), 0) + 1

    # prepare the output
    ptable = PrettyTable(["Project ID", "User ID", "Resource", "Quota -> Real",
                         "Sync Status"])

    # find discrepancies between quota usage and real usage
    quota_usages_to_sync = {}
    for resource in resource_types:
        userlist = []
        for resource_tuple in quota_usages:
            userlist.append(resource_tuple[1])
        sorteduserlist = sorted(set(userlist))
        for user in sorteduserlist:
            try:
                if real_usages[(resource, user)] != quota_usages[(resource, user)]:
                    quota_usages_to_sync[(resource, user)] = real_usages[(resource, user)]
                    ptable.add_row([args.project_id, user, resource,
                                   str(quota_usages[(resource, user)]) + ' -> ' +
                                   str(real_usages[(resource, user)]),
                                   '\033[1m\033[91mMISMATCH\033[0m'])
                else:
                    ptable.add_row([args.project_id, user, resource,
                                   str(quota_usages[(resource, user)]) + ' -> ' +
                                   str(real_usages[(resource, user)]),
                                   '\033[1m\033[92mOK\033[0m'])
            except KeyError:
                pass

    if len(quota_usages):
        print ptable

    # sync the quota with the real usage
    if quota_usages_to_sync and not args.nosync and (args.sync or yn_choice()):
        sync_quota_usages_project(manila_metadata, args.project_id,
                                  quota_usages_to_sync)


if __name__ == "__main__":
    main()