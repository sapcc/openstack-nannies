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

import click
import logging
import os
import six
import time
import sys

from openstack import connection, exceptions

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')

# list of servers or volumes we have seen or plan to delete
servers_to_be_deleted = dict()
servers_seen = dict()
volumes_to_be_deleted = dict()
volumes_seen = dict()

# cmdline handling
@click.command()
# every how many minutes the check should be preformed
@click.option('--interval', prompt='Interval in minutes')
# how often a vm should be continously a canditate for some action (delete etc.) before
# we actually do it - the idea behind is that we want to avoid actions due to short
# temporary technical problems of any kind
@click.option('--iterations', prompt='Iterations')
# work on nova db (vms) or cinder db (volumes)?
@click.option('--nova', is_flag=True)
@click.option('--cinder', is_flag=True)
# dry run mode - only say what we would do without actually doing it
@click.option('--dry-run', is_flag=True)
class Cleanup:
    def __init__(self, interval, iterations, nova, cinder, dry_run):
        self.interval = interval
        self.iterations = iterations
        self.nova = nova
        self.cinder = cinder
        self.dry_run = dry_run

        # a dict of all projects we have in openstack
        self.projects = dict()

        # dicts for the ids we have seen and the ones we want to do something with
        self.seen_dict = dict()
        self.to_be_dict = dict()

        self.run_me()

    def connection_buildup(self):
        # a dict of all projects we have in openstack
        self.projects = dict()
        # openstack connection
        try:
            self.conn = connection.Connection(auth_url=os.getenv('OS_AUTH_URL'),
                                     project_name=os.getenv('OS_PROJECT_NAME'),
                                     project_domain_name=os.getenv('OS_PROJECT_DOMAIN_NAME'),
                                     username=os.getenv('OS_USERNAME'),
                                     user_domain_name=os.getenv('OS_USER_DOMAIN_NAME'),
                                     password=os.getenv('OS_PASSWORD'))
        except Exception as e:
            log.warn("- PLEASE CHECK MANUALLY: problems connecting to openstack: %s - retrying in next loop run",
                     str(e))
        else:
            # get all openstack projects
            for project in self.conn.identity.projects(details=False, all_tenants=1):
                self.projects[project.id] = project.name

    def init_seen_dict(self):
        for i in self.seen_dict:
            self.seen_dict[i] = 0

    # reset dict of all vms or volumes we plan to delete from the db
    def reset_to_be_dict(self):
        for i in self.seen_dict:
            if self.seen_dict[i] == 0:
                self.to_be_dict[i] = 0

    def run_me(self):
        if self.nova or self.cinder:
            while True:
                self.connection_buildup()
                if len(self.projects) > 0:
                    self.os_cleanup_items()
                self.wait_a_moment()
        else:
            log.info("either the --nova or the --cinder flag should be given - giving up!")
            sys.exit(0)

    # main cleanup function
    def os_cleanup_items(self):

        # this should be unified maybe with the cinder stuff below
        # get all instances from nova
        if self.nova:
            # create a list of servers, sorted by their id
            self.entity = sorted(self.conn.compute.servers(details=True, all_tenants=1), key=lambda x: x.id)
            self.check_for_project_id("server")

        # get all volumes from cinder
        if self.cinder:
            # create a list of volumes, sorted by their id
            self.entity = sorted(self.conn.block_store.volumes(details=True, all_tenants=1), key=lambda x: x.id)
            self.check_for_project_id("volume")

    def wait_a_moment(self):
        # wait the interval time
        time.sleep(60 * int(self.interval))

    def check_for_project_id(self, type):
        self.init_seen_dict()
        for element in self.entity:
            # element has an existing project id - we keep it
            if self.projects.get(element.project_id):
                log.debug("%s %s has a valid project id: %s", type, str(element.id), str(element.project_id))
                pass
            # element has no existing project id - we plan to delete it
            else:
                log.debug("%s %s has no valid project id!", type, str(element.id))
                self.now_or_later(element.id, "delete of " + type)
        # reset the dict of instances we plan to do delete from the db for all machines we did not see or which disappeared
        self.reset_to_be_dict()

    # here we decide to wait longer before doings the delete from the db or finally doing it
    def now_or_later(self, id, what_to_do):
        default = 0
        self.seen_dict[id] = 1
        # if we did not see this more often than iteration times, do or dry-run print what to do - otherwise do not print anything, so that dry-run mode looks like real mode
        if self.to_be_dict.get(id, default) <= int(self.iterations):
            # we have seen it iteration times, time to act
            if self.to_be_dict.get(id, default) == int(self.iterations):
                # ... or print if we are only in dry-run mode
                if self.dry_run:
                    log.info("- dry-run: %s %s", what_to_do, id)
                else:
                    if what_to_do == "delete of server":
                        log.info("- action: %s %s", what_to_do, id)
                        try:
                            self.conn.compute.delete_server(id)
                        except exceptions.HttpException:
                            log.warn("got an http exception - this will have to be handled later")
                    if what_to_do == "delete of volume":
                        log.info("- action: %s %s", what_to_do, id)
                        try:
                            self.conn.block_store.delete_volume(id)
                        except exceptions.HttpException:
                            log.warn("got an http exception - this will have to be handled later")
                    else:
                        log.warn("- PLEASE CHECK MANUALLY: unsupported action requested for id - %s", id)
            # otherwise print out what we plan to do in the future
            else:
                log.info("- plan: %s %s (%i/%i)", what_to_do, id, self.to_be_dict.get(id, default) + 1, int(self.iterations))
            self.to_be_dict[id] = self.to_be_dict.get(id, default) + 1

if __name__ == '__main__':
    c = Cleanup()
