#!/usr/bin/env python

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
def now_or_later(id, to_be_dict, seen_dict, what_to_do, iterations, dry_run, conn):
    default = 0
    seen_dict[id] = 1
    if to_be_dict.get(id, default) <= int(iterations):
        if to_be_dict.get(id, default) == int(iterations):
            if dry_run:
                log.info("- dry-run: %s %s", what_to_do, id)
            else:
                if what_to_do == "delete of server":
                    log.info("- action: %s %s", what_to_do, id)
                    try:
                        conn.compute.delete_server(id)
                    except exceptions.HttpException:
                        log.wanr("got an http exception - this will have to be handled later")
                if what_to_do == "delete of volume":
                    log.info("- action: %s %s", what_to_do, id)
                    try:
                        conn.block_store.delete_volume(id)
                    except exceptions.HttpException:
                        log.warn("got an http exception - this will have to be handled later")
                else:
                    log.warn("- PLEASE CHECK MANUALLY: unsupported action requested for id - %s", id)
        else:
            log.info("- plan: %s %s (%i/%i)", what_to_do, id, to_be_dict.get(id, default) + 1, int(iterations))
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

    # a dict of all projects we have in openstack
    projects = dict()

    # get all openstack projects
    for project in conn.identity.projects(details=False, all_tenants=1):
        projects[project.id] = project.name

    # this should be unified maybe with the cinder stuff below
    # get all instances from nova
    if nova:
        # create a list of servers, sorted by their id
        all_servers = sorted(conn.compute.servers(details=True, all_tenants=1), key=lambda x: x.id)
        init_seen_dict(servers_seen)
        for a_server in all_servers:
            # instance has an existing project id - we keep it
            if projects.get(a_server.project_id):
                log.debug("server %s has a valid project id: %s", str(a_server.id), str(a_server.project_id))
                pass
            # instance has no existing project id - we plan to delete it
            else:
                log.debug("server %s has no valid project id!", str(a_server.id))
                now_or_later(a_server.id, servers_to_be_deleted, servers_seen, "delete of server", iterations, dry_run, conn)
        # reset the dict of instances we plan to do delete from the db for all machines we did not see or which disappeared
        reset_to_be_dict(servers_to_be_deleted, servers_seen)

    # get all volumes from cinder
    if cinder:
        # create a list of volumes, sorted by their id
        all_volumes = sorted(conn.block_store.volumes(details=True, all_tenants=1), key=lambda x: x.id)
        init_seen_dict(volumes_seen)
        for a_volume in all_volumes:
            # volume has an existing project id - we keep it
            if projects.get(a_volume.project_id):
                log.debug("volume %s has a valid project id: %s", str(a_volume.id), str(a_volume.project_id))
                pass
            # volume has no existing project id - we plan to delete it
            else:
                log.debug("volume %s has no valid project id!", str(a_volume.id))
                now_or_later(a_volume.id, volumes_to_be_deleted, volumes_seen, "delete of volume", iterations, dry_run, conn)
        # reset the dict of instances we plan to do delete from the db for all machines we did not see or which disappeared
        reset_to_be_dict(volumes_to_be_deleted, volumes_seen)

    # wait the interval time
    time.sleep(60 * int(interval))

if __name__ == '__main__':
    while True:
        run_me()
