#!/usr/bin/env python

import click
import logging
import os
import six
import time
import sys

from openstack import connection

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
def run_me(interval, iterations, nova, cinder, dry_run):
    if nova or cinder:
        while True:
            os_cleanup_items(interval, iterations, nova, cinder, dry_run)
    else:
        log.info("either the --nova or the --cinder flag should be given - giving up!")
        sys.exit(0)

def init_seen_dict(seen_dict):
    for i in seen_dict:
        seen_dict[i] = 0

# reset dict of all vms or volumes we plan to delete from the db
def reset_to_be_dict(to_be_dict, seen_dict):
    for i in seen_dict:
        if seen_dict[i] == 0:
            to_be_dict[i] = 0

# here we decide to wait longer before doings the delete from the db or finally doing it
def now_or_later(id, to_be_dict, seen_dict, what_to_do, iterations):
    default = 0
    seen_dict[id] = 1
    if to_be_dict.get(id, default) <= int(iterations):
        if to_be_dict.get(id, default) == int(iterations):
            log.info("- in theory i would now start the %s %s", what_to_do, id)
        else:
            log.info("- considering later %s %s (%i/%i)", what_to_do, id, to_be_dict.get(id, default) + 1, int(iterations))
        to_be_dict[id] = to_be_dict.get(id, default) + 1

# main cleanup function
def os_cleanup_items(interval, iterations, nova, cinder, dry_run):
    # openstack connection
    conn = connection.Connection(auth_url=os.getenv('OS_AUTH_URL'),
                                 project_name=os.getenv('OS_PROJECT_NAME'),
                                 project_domain_name=os.getenv('OS_PROJECT_DOMAIN_NAME'),
                                 username=os.getenv('OS_USERNAME'),
                                 user_domain_name=os.getenv('OS_USER_DOMAIN_NAME'),
                                 password=os.getenv('OS_PASSWORD'))

    projects = dict()
    servers = dict()
    volumes = dict()

    # get all openstack projects
    for project in conn.identity.projects(details=False, all_tenants=1):
        projects[project.id] = project.name

    # get all instances from nova
    if nova:
        for server in conn.compute.servers(details=True, all_tenants=1):
            servers[server.id] = server.project_id
        init_seen_dict(servers_seen)
        for aserver, aprojectid in six.iteritems(servers):
            # instance has an existing project id - we keep it
            if projects.get(aprojectid):
                log.debug("server %s has a valid project id: %", str(aserver), str(aprojectid))
                pass
            # instance has no existing project id - we plan to delete it
            else:
                if not dry_run:
                    log.info("- should not get here")
                else:
                    log.debug("server %s has no valid project id!", str(aserver))
                    now_or_later(aserver, servers_to_be_deleted, servers_seen, "delete of server", iterations)
        # reset the dict of instances we plan to do delete from the db for all machines we did not see or which disappeared
        reset_to_be_dict(servers_to_be_deleted, servers_seen)

    # get all volumes from cinder
    if cinder:
        for volume in conn.block_store.volumes(details=True, all_tenants=1):
            volumes[volume.id] = volume.project_id
        init_seen_dict(volumes_seen)
        for avolume, aprojectid in six.iteritems(volumes):
            # volume has an existing project id - we keep it
            if projects.get(aprojectid):
                log.debug("volume %s has a valid project id: %", str(avolume), str(aprojectid))
                pass
            # volume has no existing project id - we plan to delete it
            else:
                if not dry_run:
                    log.info("- should not get here")
                else:
                    log.debug("volume %s has no valid project id!", str(avolume))
                    now_or_later(avolume, volumes_to_be_deleted, volumes_seen, "delete of volume", iterations)
        # reset the dict of instances we plan to do delete from the db for all machines we did not see or which disappeared
        reset_to_be_dict(volumes_to_be_deleted, volumes_seen)

    # wait the interval time
    time.sleep(60 * int(interval))

if __name__ == '__main__':
    while True:
        run_me()
