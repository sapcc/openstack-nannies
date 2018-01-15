#!/usr/bin/env python

import atexit
import click
import logging
import re
import os
import six
import ssl
import time

from pyVim.connect import SmartConnect, Disconnect
from pyVim.task import WaitForTask, WaitForTasks
from pyVmomi import vim
from openstack import connection

uuid_re = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', re.IGNORECASE)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')

vms_to_be_poweredoff = dict()
vms_to_be_unregistered = dict()
vms_seen = dict()
files_to_be_deleted = dict()
files_seen = dict()

# find files with a uuid name pattern
def _uuids(task):
    folder_path = task.info.result.folderPath
    for f in task.info.result.file:
        match = uuid_re.search(f.path)
        if match:
            yield match.group(0), {'datastore': folder_path, 'folder': f.path}

# cmdline handling
@click.command()
# vcenter host, user and password
@click.option('--host', help='Host to connect to.')
@click.option('--username', prompt='Your name')
@click.option('--password', prompt='The password')
# every how many minutes the check should be preformed
@click.option('--interval', prompt='Interval in minutes')
# how often a vm should be continously a canditate for some action (delete etc.) before
# we actually do it - the idea behind is that we want to avoid actions due to short
# temporary technical problems of any kind ... another idea is to do the actions step
# by step (i.e. power-off - iterations - unlink - iterations - delete file path), so that
# we have a chance to still roll back in case we notice problems due to some wrong
# action done
@click.option('--iterations', prompt='Iterations')
# dry run mode - only say what we would do without actually doing it
@click.option('--dry-run', is_flag=True)
def run_me(host, username, password, interval, iterations, dry_run):
    while True:
        cleanup_items(host, username, password, interval, iterations, dry_run)

# init dict of all vms or files we have seen already
def init_seen_dict(seen_dict):
    for i in seen_dict:
        seen_dict[i] = 0

# reset dict of all vms or files we plan to do something with (delete etc.)
def reset_to_be_dict(to_be_dict, seen_dict):
    for i in seen_dict:
        # if a machine we planned to delete no longer appears as canditate for delettion, remove it from the list
        if seen_dict[i] == 0:
            to_be_dict[i] = 0

# here we decide to wait longer before doings something (delete etc.) or finally doing it
def now_or_later(id, to_be_dict, seen_dict, what_to_do, iterations):
    default = 0
    seen_dict[id] = 1
    if to_be_dict.get(id, default) <= int(iterations):
        if to_be_dict.get(id, default) == int(iterations):
            log.info("- in theory i would now start the %s %s", what_to_do, id)
        else:
            log.info("- considering later %s %s (%i/%i)", what_to_do, id, to_be_dict.get(id, default) + 1,
                     int(iterations))
        to_be_dict[id] = to_be_dict.get(id, default) + 1

# main cleanup function
def cleanup_items(host, username, password, interval, iterations, dry_run):
    # openstack connection
    conn = connection.Connection(auth_url=os.getenv('OS_AUTH_URL'),
                                 project_name=os.getenv('OS_PROJECT_NAME'),
                                 project_domain_name=os.getenv('OS_PROJECT_DOMAIN_NAME'),
                                 username=os.getenv('OS_USERNAME'),
                                 user_domain_name=os.getenv('OS_USER_DOMAIN_NAME'),
                                 password=os.getenv('OS_PASSWORD'))

    known = dict()

    # get all servers, volumes, snapshots and images from openstack to compare the resources we find on the vcenter against
    for server in conn.compute.servers(details=False, all_tenants=1):
        known[server.id] = server

    for volume in conn.block_store.volumes(details=False, all_tenants=1):
        known[volume.id] = volume

    for snapshot in conn.block_store.snapshots(details=False, all_tenants=1):
        known[snapshot.id] = snapshot

    for image in conn.image.images(details=False, all_tenants=1):
        known[image.id] = image

    # vcenter connection
    if hasattr(ssl, '_create_unverified_context'):
        context = ssl._create_unverified_context()

        service_instance = SmartConnect(host=host,
                                        user=username,
                                        pwd=password,
                                        port=443,
                                        sslContext=context)
    else:
        raise Exception("maybe too old python version with ssl problems?")

    if service_instance:
        atexit.register(Disconnect, service_instance)

    content = service_instance.content
    dc = content.rootFolder.childEntity[0]
    missing = dict()
    # iterate through all datastores in the vcenter
    for ds in dc.datastore:
        log.info("- datacenter / datastore: %s / %s", dc.name, ds.name)

        task = ds.browser.SearchDatastore_Task(datastorePath="[%s] /" % ds.name,
                                           searchSpec=vim.HostDatastoreBrowserSearchSpec(query=[vim.FolderFileQuery()]))

        try:
            WaitForTask(task, si=service_instance)
            # find all the entities we have on the vcenter which have no relation to openstack anymore and write them to a dict
            for uuid, location in _uuids(task):
                if uuid not in known:
                    missing[uuid] = location
        except vim.fault.InaccessibleDatastore as e:
            log.warn("- something went wrong trying to access this datastore: %s", e.msg)
        except vim.fault.FileNotFound as e:
            log.warn("- something went wrong trying to access this datastore: %s", e.msg)

    tasks = []

    init_seen_dict(vms_seen)
    init_seen_dict(files_seen)

    # iterate over all entities we have on the vcenter which have no relation to openstack anymore
    for item, location in six.iteritems(missing):
        # find the vm correspoding to the file path
        path = "{datastore} {folder}".format(**location)
        vmx_path = "{datastore} {folder}/{folder}.vmx".format(**location)
        vm = content.searchIndex.FindByDatastorePath(path=vmx_path, datacenter=dc)
        # there is a vm for that file path
        if vm:
            power_state = vm.runtime.powerState
            # we store the openstack project id in the annotations of the vm
            annotation = vm.config.annotation or ''
            items = dict([line.split(':', 1) for line in annotation.splitlines()])
            if 'projectid' in items:
                log.debug(
                    "{folder}: {power_state} {projectid}".format(power_state=power_state, projectid=items['projectid'],
                                                                 **location))
                # if still powered on the panned action is to power off
                if power_state == 'poweredOn':
                    if not dry_run:
                        log.info("- should not get here")
                        # WaitForTask(vm.PowerOffVM_Task(), si=service_instance)
                    else:
                        now_or_later(vm.config.uuid, vms_to_be_poweredoff, vms_seen, "power off of vm", iterations)
                if not dry_run:
                    log.info("- should not get here")
                    # better unlink the vm only, i.e. leave the files on disk
                    # tasks.append(vm.Destroy_Task())
                # if already powered off the planned action is to unlink the vm
                else:
                    now_or_later(vm.config.uuid, vms_to_be_unregistered, vms_seen, "unregister of vm", iterations)
        # there is no vm anymore for the file path - planned action is to delete the file
        else:
            if not dry_run:
                log.info("- should not get here")
                # tasks.append(content.fileManager.DeleteDatastoreFile_Task(name=path, datacenter=dc))
            else:
                now_or_later(str(str(vmx_path).split()[1]), files_to_be_deleted, files_seen, "delete of datastore file",
                             iterations)
        if len(tasks) % 8 == 0:
            WaitForTasks(tasks[-8:], si=service_instance)
    # reset the dict of vms or files we plan to do something with for all machines we did not see or which disappeared
    reset_to_be_dict(vms_to_be_poweredoff, vms_seen)
    reset_to_be_dict(vms_to_be_unregistered, vms_seen)
    reset_to_be_dict(files_to_be_deleted, files_seen)

    # wait the interval time
    time.sleep(60 * int(interval))

if __name__ == '__main__':
    while True:
        run_me()
