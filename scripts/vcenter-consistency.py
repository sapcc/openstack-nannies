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
import time

from openstack import connection, exceptions

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')

# cmdline handling
@click.command()
# every how many minutes the check should be preformed
@click.option('--interval', prompt='Interval in minutes')
# how often a vm should be continously a candidate for some action (delete etc.) before
# we actually do it - the idea behind is that we want to avoid actions due to short
# temporary technical problems of any kind ... another idea is to do the actions step
# by step (i.e. suspend - iterations - power-off - iterations - unlink - iterations -
# delete file path) for vms or rename folder (eph storage) or files (vvol storage), so
# that we have a chance to still roll back in case we notice problems due to some wrong
# action done
@click.option('--iterations', prompt='Iterations')
# dry run mode - only say what we would do without actually doing it
@click.option('--dry-run', is_flag=True)

def run_me(interval, iterations, dry_run):

    while True:

        log.info("INFO: starting new loop run")

        # do the cleanup work
        sync_volume_attachments(interval, iterations, dry_run)

        # wait the interval time
        log.info("INFO: waiting %s minutes before starting the next loop run", str(interval))
        time.sleep(60 * int(interval))

# main volume attachment sync function
def sync_volume_attachments(interval, iterations, dry_run):

    # openstack connection
    conn = connection.Connection(auth_url=os.getenv('OS_AUTH_URL'),
                                 project_name=os.getenv('OS_PROJECT_NAME'),
                                 project_domain_name=os.getenv('OS_PROJECT_DOMAIN_NAME'),
                                 username=os.getenv('OS_USERNAME'),
                                 user_domain_name=os.getenv('OS_USER_DOMAIN_NAME'),
                                 password=os.getenv('OS_PASSWORD'))

    servers_attached_volumes = dict()
    volumes_attached_at = dict()
    all_servers = []
    all_volumes = []
    
    # get all servers, volumes, snapshots and images from openstack to compare the resources we find on the vcenter against
    try:
        service = "nova"
        for server in conn.compute.servers(details=True, all_tenants=1):
            all_servers.append(server.id)
            if server.attached_volumes:
                for attachment in server.attached_volumes:
                    if servers_attached_volumes.get(server.id):
                        servers_attached_volumes[server.id].append(attachment['id'])
                    else:
                        servers_attached_volumes[server.id] = [attachment['id']]
        service = "cinder"
        for volume in conn.block_store.volumes(details=True, all_tenants=1):
            all_volumes.append(volume.id)
            if volume.attachments:
                for attachment in volume.attachments:
                    if volumes_attached_at.get(volume.id):
                        volumes_attached_at[volume.id].append(attachment['server_id'])
                    else:
                        volumes_attached_at[volume.id] = [attachment['server_id']]

    except exceptions.HttpException as e:
        log.warn(
            "- PLEASE CHECK MANUALLY - problems retrieving information from openstack %s: %s - retrying in next loop run",
            service, str(e))
        return
    except exceptions.SDKException as e:
        log.warn(
            "- PLEASE CHECK MANUALLY - problems retrieving information from openstack %s: %s - retrying in next loop run",
            service, str(e))
        return

    log.info("going through all volumes and checking their attachments:")
    for i in volumes_attached_at:
        for j in volumes_attached_at[i]:
            is_attached = False
            if servers_attached_volumes.get(j):
                for m in servers_attached_volumes[j]:
                    if m == i:
                        is_attached = True
            if is_attached:
                log.debug("good: volume %s is attached to server %s", i, j)
            else:
                if j in all_servers:
                    log.warn("bad: volume %s is not attached to server %s", i, j)
                else:
                    log.warn("bad: volume %s is attached to non existing server %s", i, j)
    log.info("going through all servers and checking attached volumes:")
    for k in servers_attached_volumes:
        for l in servers_attached_volumes[k]:
            is_attached = False
            if volumes_attached_at.get(l):
                for n in volumes_attached_at[l]:
                    if n == k:
                        is_attached = True
            if is_attached:
                log.debug("good: volume %s is attached to server %s", l, k)
            else:
                if l in all_volumes:
                    log.warn("bad: volume %s is not attached to server %s", l, k)
                else:
                    log.warn("bad: non existing volume %s is attached to server %s", l, k)

if __name__ == '__main__':
    while True:
        run_me()
