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
from pyVmomi import vim, vmodl
from openstack import connection

uuid_re = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', re.IGNORECASE)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')

vms_to_be_suspended = dict()
vms_to_be_poweredoff = dict()
vms_to_be_unregistered = dict()
vms_seen = dict()
files_to_be_deleted = dict()
files_to_be_renamed = dict()
files_seen = dict()

tasks = []


# find vmx and vmdk files with a uuid name pattern
def _uuids(task):
    for searchresult in task.info.result:
        folder_path = searchresult.folderPath
        # no files in the folder
        if not searchresult.file:
            log.warn("- PLEASE CHECK MANUALLY - empty folder: %s", folder_path)
        else:
            # its ugly to do it in two loops, but an easy way to make sure to have the vms before the vmdks in the list
            for f in searchresult.file:
                if f.path.lower().endswith(".vmx") or f.path.lower().endswith(".vmx.renamed_by_vcenter_nanny"):
                    match = uuid_re.search(f.path)
                    if match:
                        yield match.group(0), {'folderpath': folder_path, 'filepath': f.path}
            for f in searchresult.file:
                if f.path.lower().endswith(".vmdk") or f.path.lower().endswith(".vmdk.renamed_by_vcenter_nanny"):
                    match = uuid_re.search(f.path)
                    if match:
                        yield match.group(0), {'folderpath': folder_path, 'filepath': f.path}


# cmdline handling
@click.command()
# vcenter host, user and password
@click.option('--host', help='Host to connect to.')
@click.option('--username', prompt='Your name')
@click.option('--password', prompt='The password')
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
# do not power off vms
@click.option('--power-off', is_flag=True)
# do not unregister vms
@click.option('--unregister', is_flag=True)
# do not delete datastore files or folders
@click.option('--delete', is_flag=True)
def run_me(host, username, password, interval, iterations, dry_run, power_off, unregister, delete):

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

    # iterate through all vms and get the config.hardware.device properties (and some other)
    # get vm containerview
    # TODO: destroy the view again
    view_ref = content.viewManager.CreateContainerView(
        container=content.rootFolder,
        type=[vim.VirtualMachine],
        recursive=True
    )

    while True:
        cleanup_items(host, username, password, interval, iterations, dry_run, power_off, unregister, delete, service_instance, content, dc, view_ref)


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
def now_or_later(id, to_be_dict, seen_dict, what_to_do, iterations, dry_run, power_off, unregister, delete, vm, dc, content, detail):
    default = 0
    seen_dict[id] = 1
    if to_be_dict.get(id, default) <= int(iterations):
        if to_be_dict.get(id, default) == int(iterations):
            if dry_run:
                log.info("- dry-run: %s %s", what_to_do, id)
            else:
                if what_to_do == "suspend of vm":
                    log.info("- action: %s %s [%s]", what_to_do, id, detail)
                    # either WaitForTask or tasks.append
                    tasks.append(vm.SuspendVM_Task())
                elif what_to_do == "power off of vm":
                    if power_off:
                        log.info("- action: %s %s [%s]", what_to_do, id, detail)
                        tasks.append(vm.PowerOffVM_Task())
                        if what_to_do == "unregister of vm":
                            if unregister:
                                log.info("- action: %s %s [%s]", what_to_do, id, detail)
                                # either unregisterVM_Task (safer) or destroy_Task
                                tasks.append(vm.UnregisterVM_Task())
                elif what_to_do == "rename of ds path":
                    log.info("- action: %s %s [%s]", what_to_do, id, detail)
                    newname = id.rstrip('/') + ".renamed_by_vcenter_nanny"
                    tasks.append(content.fileManager.MoveDatastoreFile_Task(sourceName=id, sourceDatacenter=dc, destinationName=newname, destinationDatacenter=dc))
                elif what_to_do == "delete of ds path":
                    if delete:
                        log.info("- action: %s %s [%s]", what_to_do, id, detail)
                        tasks.append(content.fileManager.DeleteDatastoreFile_Task(name=id, datacenter=dc))
                elif not what_to_do == "unregister of vm":
                    log.warn("- PLEASE CHECK MANUALLY: unsupported action requested for id - %s", id)
        else:
            log.info("- plan: %s %s [%s] (%i/%i)", what_to_do, id, detail, to_be_dict.get(id, default) + 1,
                     int(iterations))
        to_be_dict[id] = to_be_dict.get(id, default) + 1

# Shamelessly borrowed from:
# https://github.com/dnaeon/py-vconnector/blob/master/src/vconnector/core.py
def collect_properties(service_instance, view_ref, obj_type, path_set=None,
                       include_mors=False):
    """
    Collect properties for managed objects from a view ref
    Check the vSphere API documentation for example on retrieving
    object properties:
        - http://goo.gl/erbFDz
    Args:
        si          (ServiceInstance): ServiceInstance connection
        view_ref (vim.view.*): Starting point of inventory navigation
        obj_type      (vim.*): Type of managed object
        path_set               (list): List of properties to retrieve
        include_mors           (bool): If True include the managed objects
                                       refs in the result
    Returns:
        A list of properties for the managed objects
    """
    collector = service_instance.content.propertyCollector

    # Create object specification to define the starting point of
    # inventory navigation
    obj_spec = vmodl.query.PropertyCollector.ObjectSpec()
    obj_spec.obj = view_ref
    obj_spec.skip = True

    # Create a traversal specification to identify the path for collection
    traversal_spec = vmodl.query.PropertyCollector.TraversalSpec()
    traversal_spec.name = 'traverseEntities'
    traversal_spec.path = 'view'
    traversal_spec.skip = False
    traversal_spec.type = view_ref.__class__
    obj_spec.selectSet = [traversal_spec]

    # Identify the properties to the retrieved
    property_spec = vmodl.query.PropertyCollector.PropertySpec()
    property_spec.type = obj_type

    if not path_set:
        property_spec.all = True

    property_spec.pathSet = path_set

    # Add the object and property specification to the
    # property filter specification
    filter_spec = vmodl.query.PropertyCollector.FilterSpec()
    filter_spec.objectSet = [obj_spec]
    filter_spec.propSet = [property_spec]

    # Retrieve properties
    props = collector.RetrieveContents([filter_spec])

    data = []
    for obj in props:
        properties = {}
        for prop in obj.propSet:
            properties[prop.name] = prop.val

        if include_mors:
            properties['obj'] = obj.obj

        data.append(properties)
    return data

# main cleanup function
def cleanup_items(host, username, password, interval, iterations, dry_run, power_off, unregister, delete, service_instance, content, dc, view_ref):
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

    # the properties we want to collect - some of them are not yet used, but will at a later
    # development stage of this script to validate the volume attachments with cinder and nova
    vm_properties = [
        "config.hardware.device",
        "config.name",
        "config.uuid",
        "config.instanceUuid"
    ]

    # collect the properties for all vms
    data = collect_properties(service_instance, view_ref, vim.VirtualMachine,
                              vm_properties, True)

    # create a dict of volumes mounted to vms to compare the volumes we plan to delete against
    # to find possible ghost volumes
    vcenter_mounted = dict()
    # iterate over the list of vms
    for k in data:
        # get the config.hardware.device property out of the data dict and iterate over its elements
        #for j in k['config.hardware.device']:
        # this check seems to be required as in one bb i got a key error otherwise - looks like a vm without that property
        if k.get('config.hardware.device'):
            for j in k.get('config.hardware.device'):
                # we are only interested in disks - TODO: maybe the range needs to be adjusted
                if 2001 <= j.key <= 2010:
                    vcenter_mounted[j.backing.uuid] = k['config.instanceUuid']

    # do the check from the other end: see for which vms or volumes in the vcenter we do not have any openstack info
    missing = dict()
    # iterate through all datastores in the vcenter
    for ds in dc.datastore:
        # only consider eph and vvol datastores
        if ds.name.lower().startswith('eph') or ds.name.lower().startswith('vvol'):
            log.info("- datacenter / datastore: %s / %s", dc.name, ds.name)

            # get all files and folders recursively from the datastore
            task = ds.browser.SearchDatastoreSubFolders_Task(datastorePath="[%s] /" % ds.name,
                                                             searchSpec=vim.HostDatastoreBrowserSearchSpec(
                                                                 matchPattern="*"))
            # matchPattern = ["*.vmx", "*.vmdk", "*.vmx.renamed_by_vcenter_nanny", "*,vmdk.renamed_by_vcenter_nanny"]))

            try:
                # wait for the async task to finish and then find vms and vmdks with openstack uuids in the name and
                # compare those uuids to all the uuids we know from openstack
                WaitForTask(task, si=service_instance)
                for uuid, location in _uuids(task):
                    if uuid not in known:
                        # multiple locations are possible for one uuid, thus we need to put the locations into a list
                        if uuid in missing:
                            missing[uuid].append(location)
                        else:
                            missing[uuid] = [location]
            except vim.fault.InaccessibleDatastore as e:
                log.warn("- something went wrong trying to access this datastore: %s", e.msg)
            except vim.fault.FileNotFound as e:
                log.warn("- something went wrong trying to access this datastore: %s", e.msg)

    init_seen_dict(vms_seen)
    init_seen_dict(files_seen)

    # needed to mark folder paths and full paths we already dealt with
    vmxmarked = {}
    vmdkmarked = {}
    vvolmarked = {}

    # iterate over all entities we have on the vcenter which have no relation to openstack anymore
    for item, locationlist in six.iteritems(missing):
        # none of the uuids we do not know anything about on openstack side should be mounted anywhere in vcenter
        # so we should neither see it as vmx (shadow vm) or datastore file
        if vcenter_mounted.get(item):
            log.warn("- PLEASE CHECK MANUALLY: possibly mounted ghost volume - %s mounted on %s", item, vcenter_mounted[item])
        else:
            for location in locationlist:
                # foldername on datastore
                path = "{folderpath}".format(**location)
                # filename on datastore
                filename = "{filepath}".format(**location)
                fullpath = path + filename
                # in the case of a vmx file we check if the vcenter still knows about it
                if location["filepath"].lower().endswith(".vmx"):
                    vmx_path = "{folderpath}{filepath}".format(**location)
                    vm = content.searchIndex.FindByDatastorePath(path=vmx_path, datacenter=dc)
                    # there is a vm for that file path we check what to do with it
                    if vm:
                        power_state = vm.runtime.powerState
                        # is the vm located on vvol storage - needed later to check if its a volume shadow vm
                        if vm.config.files.vmPathName.lower().startswith('[vvol'):
                            is_vvol = True
                        else:
                            is_vvol = False
                        # check if the vm has a nic configured
                        for j in vm.config.hardware.device:
                            if j.key == 4000:
                                has_no_nic = False
                            else:
                                has_no_nic = True
                        # we store the openstack project id in the annotations of the vm
                        annotation = vm.config.annotation or ''
                        items = dict([line.split(':', 1) for line in annotation.splitlines()])
                        # we search for either vms with a project_id in the annotation (i.e. real vms) or
                        # for powered off vms with 128mb, one cpu and no nic which are stored on vvol (i.e. shadow vm for a volume)
                        if 'projectid' in items or (
                                vm.config.hardware.memoryMB == 128 and vm.config.hardware.numCPU == 1 and power_state == 'poweredOff' and is_vvol and has_no_nic):
                            # if still powered on the planned action is to suspend it
                            if power_state == 'poweredOn':
                                # mark that path as already dealt with, so that we ignore it when we see it again
                                # with vmdks later maybe
                                vmxmarked[path] = True
                                now_or_later(vm.config.instanceUuid, vms_to_be_suspended, vms_seen, "suspend of vm",
                                             iterations,
                                             dry_run, power_off, unregister, delete, vm, dc, content, filename)
                            # if already suspended the planned action is to power off the vm
                            elif power_state == 'suspended':
                                vmxmarked[path] = True
                                now_or_later(vm.config.instanceUuid, vms_to_be_poweredoff, vms_seen, "power off of vm",
                                             iterations,
                                             dry_run, power_off, unregister, delete, vm, dc, content, filename)
                            # if already powered off the planned action is to unregister the vm
                            else:
                                vmxmarked[path] = True
                                now_or_later(vm.config.instanceUuid, vms_to_be_unregistered, vms_seen, "unregister of vm",
                                             iterations,
                                             dry_run, power_off, unregister, delete, vm, dc, content, filename)
                        # this should not happen
                        elif (
                                vm.config.hardware.memoryMB == 128 and vm.config.hardware.numCPU == 1 and power_state == 'poweredOff' and not is_vvol and has_no_nic):
                            log.warn("- PLEASE CHECK MANUALLY: possible orphan shadow vm on eph storage - %s", path)
                        # this neither
                        else:
                            log.warn(
                                "- PLEASE CHECK MANUALLY: this vm seems to be neither a former openstack vm nor an orphan shadow vm - %s",
                                path)

                    # there is no vm anymore for the file path - planned action is to delete the file
                    elif not vmxmarked.get(path, False):
                        vmxmarked[path] = True
                        if path.lower().startswith("[eph"):
                            if path.endswith(".renamed_by_vcenter_nanny/"):
                                # if already renamed finally delete
                                now_or_later(str(path), files_to_be_deleted, files_seen, "delete of ds path",
                                             iterations, dry_run, power_off, unregister, delete, vm, dc, content, filename)
                            else:
                                # first rename the file before deleting them later
                                now_or_later(str(path), files_to_be_renamed, files_seen, "rename of ds path",
                                             iterations, dry_run, power_off, unregister, delete, vm, dc, content, filename)
                        else:
                            # vvol storage
                            # for vvols we have to mark based on the full path, as we work on them file by file
                            # and not on a directory base
                            vvolmarked[fullpath] = True
                            if fullpath.endswith(".renamed_by_vcenter_nanny/"):
                                now_or_later(str(fullpath), files_to_be_deleted, files_seen, "delete of ds path",
                                             iterations, dry_run, power_off, unregister, delete, vm, dc, content, filename)
                            else:
                                now_or_later(str(fullpath), files_to_be_renamed, files_seen, "rename of ds path",
                                             iterations, dry_run, power_off, unregister, delete, vm, dc, content, filename)

                    if len(tasks) % 8 == 0:
                        WaitForTasks(tasks[-8:], si=service_instance)

                # in case of a vmdk or vmx.renamed_by_vcenter_nanny
                # eph storage case - we work on directories
                elif path.lower().startswith("[eph") and not vmxmarked.get(path, False) and not vmdkmarked.get(path, False):
                    # mark to not redo it for other vmdks as we are working on the dir at once
                    vmdkmarked[path] = True
                    if path.endswith(".renamed_by_vcenter_nanny/"):
                        now_or_later(str(path), files_to_be_deleted, files_seen, "delete of ds path",
                                     iterations, dry_run, power_off, unregister, delete, None, dc, content, filename)
                    else:
                        now_or_later(str(path), files_to_be_renamed, files_seen, "rename of ds path",
                                     iterations, dry_run, power_off, unregister, delete, None, dc, content, filename)
                # vvol storage case - we work file by file as we can't rename or delete the vvol folders
                elif path.lower().startswith("[vvol") and not vvolmarked.get(fullpath, False):
                    # vvol storage
                    if fullpath.endswith(".renamed_by_vcenter_nanny"):
                        now_or_later(str(fullpath), files_to_be_deleted, files_seen, "delete of ds path",
                                     iterations, dry_run, power_off, unregister, delete, None, dc, content, filename)
                    else:
                        now_or_later(str(fullpath), files_to_be_renamed, files_seen, "rename of ds path",
                                     iterations, dry_run, power_off, unregister, delete, None, dc, content, filename)

                if len(tasks) % 8 == 0:
                    WaitForTasks(tasks[-8:], si=service_instance)

    # reset the dict of vms or files we plan to do something with for all machines we did not see or which disappeared
    reset_to_be_dict(vms_to_be_suspended, vms_seen)
    reset_to_be_dict(vms_to_be_poweredoff, vms_seen)
    reset_to_be_dict(vms_to_be_unregistered, vms_seen)
    reset_to_be_dict(files_to_be_deleted, files_seen)
    reset_to_be_dict(files_to_be_renamed, files_seen)

    # wait the interval time
    time.sleep(60 * int(interval))


if __name__ == '__main__':
    while True:
        run_me()
