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

def _uuids(task):
    folder_path = task.info.result.folderPath
    for f in task.info.result.file:
        match = uuid_re.search(f.path)
        if match:
            yield match.group(0), {'datastore': folder_path, 'folder': f.path}

@click.command()
@click.option('--host', help='Host to connect to.')
@click.option('--username', prompt='Your name')
@click.option('--password', prompt='The password')
@click.option('--interval', prompt='Interval in minutes')
@click.option('--iterations', prompt='Iterations')
@click.option('--dry-run', is_flag=True)
def run_me(host, username, password, interval, iterations, dry_run):
    while True:
        cleanup_items(host, username, password, interval, iterations, dry_run)

def init_seen_dict(seen_dict):
    for i in seen_dict:
        seen_dict[i] = 0

def reset_to_be_dict(to_be_dict, seen_dict):
    for i in seen_dict:
        if seen_dict[i] == 0:
            to_be_dict[i] = 0

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

def cleanup_items(host, username, password, interval, iterations, dry_run):
    conn = connection.Connection(auth_url=os.getenv('OS_AUTH_URL'),
                                 project_name=os.getenv('OS_PROJECT_NAME'),
                                 project_domain_name=os.getenv('OS_PROJECT_DOMAIN_NAME'),
                                 username=os.getenv('OS_USERNAME'),
                                 user_domain_name=os.getenv('OS_USER_DOMAIN_NAME'),
                                 password=os.getenv('OS_PASSWORD'))

    known = dict()

    for server in conn.compute.servers(details=False, all_tenants=1):
        known[server.id] = server

    for volume in conn.block_store.volumes(details=False, all_tenants=1):
        known[volume.id] = volume

    for snapshot in conn.block_store.snapshots(details=False, all_tenants=1):
        known[snapshot.id] = snapshot

    for image in conn.image.images(details=False, all_tenants=1):
        known[image.id] = image

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
    for ds in dc.datastore:
        log.info("- datacenter / datastore: %s / %s", dc.name, ds.name)

        task = ds.browser.SearchDatastore_Task(datastorePath="[%s] /" % ds.name,
                                           searchSpec=vim.HostDatastoreBrowserSearchSpec(query=[vim.FolderFileQuery()]))

        try:
            WaitForTask(task, si=service_instance)
            for uuid, location in _uuids(task):
                if uuid not in known:
                    missing[uuid] = location
        except vim.fault.InaccessibleDatastore as e:
            log.warn("- something went wrong trying to access this datastore!: %s", e.msg)

    tasks = []

    init_seen_dict(vms_seen)
    init_seen_dict(files_seen)

    for item, location in six.iteritems(missing):
        path = "{datastore} {folder}".format(**location)
        vmx_path = "{datastore} {folder}/{folder}.vmx".format(**location)
        vm = content.searchIndex.FindByDatastorePath(path=vmx_path, datacenter=dc)
        if vm:
            power_state = vm.runtime.powerState
            annotation = vm.config.annotation or ''
            items = dict([line.split(':', 1) for line in annotation.splitlines()])
            if 'projectid' in items:
                log.debug(
                    "{folder}: {power_state} {projectid}".format(power_state=power_state, projectid=items['projectid'],
                                                                 **location))
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
                else:
                    now_or_later(vm.config.uuid, vms_to_be_unregistered, vms_seen, "unregister of vm", iterations)
        else:
            if not dry_run:
                log.info("- should not get here")
                # tasks.append(content.fileManager.DeleteDatastoreFile_Task(name=path, datacenter=dc))
            else:
                now_or_later(str(str(vmx_path).split()[1]), files_to_be_deleted, files_seen, "delete of datastore file",
                             iterations)
        if len(tasks) % 8 == 0:
            WaitForTasks(tasks[-8:], si=service_instance)
    reset_to_be_dict(vms_to_be_poweredoff, vms_seen)
    reset_to_be_dict(vms_to_be_unregistered, vms_seen)
    reset_to_be_dict(files_to_be_deleted, files_seen)
    time.sleep(60 * int(interval))

if __name__ == '__main__':
    while True:
        run_me()
