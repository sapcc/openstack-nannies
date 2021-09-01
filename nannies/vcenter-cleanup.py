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

import atexit
import click
import logging
import re
import os
import six
import ssl
import time
import calendar
import time

from pyVim.connect import SmartConnect, Disconnect
from pyVim.task import WaitForTask, WaitForTasks
from pyVmomi import vim, vmodl
from openstack import connection, exceptions
# prometheus export functionality
from prometheus_client import start_http_server, Gauge

uuid_re = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', re.IGNORECASE)

# compile a regex for trying to filter out openstack generated vms
#  they all have the "name:" field set
openstack_re = re.compile("^name")

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

state_to_name_map = dict()

gauge_value = dict()
gauge_suspend_vm = Gauge('vcenter_nanny_suspend_vm', 'vm suspends of the vcenter nanny', ['kind'])
gauge_power_off_vm = Gauge('vcenter_nanny_power_off_vm', 'vm power offs of the vcenter nanny', ['kind'])
gauge_unregister_vm = Gauge('vcenter_nanny_unregister_vm', 'vm unregisters of the vcenter nanny', ['kind'])
gauge_rename_ds_path = Gauge('vcenter_nanny_rename_ds_path', 'ds path renames of the vcenter nanny', ['kind'])
gauge_delete_ds_path = Gauge('vcenter_nanny_delete_ds_path', 'ds path deletes of the vcenter nanny', ['kind'])
gauge_ghost_volumes = Gauge('vcenter_nanny_ghost_volumes', 'number of possible ghost volumes mounted on vcenter')
gauge_ghost_volumes_ignored = Gauge('vcenter_nanny_ghost_volumes_ignored', 'number of possible ghost volumes on vcenter which can be ignored')
gauge_ghost_volumes_detached = Gauge('vcenter_nanny_ghost_volumes_detached', 'number of ghost volumes detached from vm')
gauge_ghost_volumes_detach_errors = Gauge('vcenter_nanny_ghost_volumes_detach_errors', 'number of possible ghost volumes on vcenter which did not detach properly')
gauge_ghost_ports = Gauge('vcenter_nanny_ghost_ports', 'number of possible ghost ports on vcenter')
gauge_ghost_ports_ignored = Gauge('vcenter_nanny_ghost_ports_ignored', 'number of possible ghost ports on vcenter which can be ignored')
gauge_ghost_ports_detached = Gauge('vcenter_nanny_ghost_ports_detached', 'number of ghost ports detached from vm')
gauge_ghost_ports_detach_errors = Gauge('vcenter_nanny_ghost_ports_detach_errors', 'number of possible ghost ports on vcenter which did not detach properly')
gauge_template_mounts = Gauge('vcenter_nanny_template_mounts', 'number of possible ghost volumes mounted on templates')
gauge_template_ports = Gauge('vcenter_nanny_template_ports', 'number of possible ghost ports attached to templates')
gauge_eph_shadow_vms = Gauge('vcenter_nanny_eph_shadow_vms', 'number of possible shadow vms on eph storage')
gauge_datastore_no_access = Gauge('vcenter_nanny_datastore_no_access', 'number of non accessible datastores')
gauge_empty_vvol_folders = Gauge('vcenter_nanny_empty_vvol_folders', 'number of empty vvols')
gauge_vcenter_connection_problems = Gauge('vcenter_nanny_vcenter_connection_problems', 'number of connection problems to the vcenter')
gauge_vcenter_get_properties_problems = Gauge('vcenter_nanny_get_properties_problems', 'number of get properties problems from the vcenter')
gauge_vcenter_task_problems = Gauge('vcenter_nanny_vcenter_task_problems', 'number of task problems from the vcenter')
gauge_openstack_connection_problems = Gauge('vcenter_nanny_openstack_connection_problems', 'number of connection problems to openstack')
gauge_unknown_vcenter_templates = Gauge('vcenter_nanny_unknown_vcenter_templates', 'number of templates unknown to openstack')
gauge_complete_orphans = Gauge('vcenter_nanny_complete_orphans', 'number of possibly completely orphan vms')
gauge_volume_attachment_inconsistencies = Gauge('vcenter_nanny_volume_attachment_inconsistencies', 'number of volume attachment inconsistencies between nova, cinder and the vcenter')
gauge_big_vm_disable_drs = Gauge('vcenter_nanny_big_vm_disable_drs', 'number of big vms which got drs disabled')
gauge_big_vm_memory_shares_high = Gauge('vcenter_nanny_big_vm_memory_shares_high', 'number of big vms which got memory shares set to high')

# find vmx and vmdk files with a uuid name pattern
def _uuids(task):
    global gauge_value_empty_vvol_folders
    for searchresult in task.info.result:
        folder_path = searchresult.folderPath
        # no files in the folder
        if not searchresult.file:
            log.warn("- PLEASE CHECK MANUALLY - empty folder: %s", folder_path)
            gauge_value_empty_vvol_folders += 1
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
@click.option('--host', prompt='Host to connect to')
@click.option('--username', prompt='Username to connect with')
@click.option('--password', prompt='Password to connect with')
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
# detach ghost volumes if any are discovered
@click.option('--detach-ghost-volumes', is_flag=True)
# detach ghost ports if any are discovered
@click.option('--detach-ghost-ports', is_flag=True)
# deny to detach ghost volumes and ports if there are more than this number of them
@click.option('--detach-ghost-limit', default=3, help='Ghost volume/port detachment limit')
# check consistency of volume attachments
@click.option('--vol-check', is_flag=True)
# size in gb from which on the special bigvm handling gets enabled
@click.option('--bigvm-size', default=1024, help='Bigvm size in GB for drs disablement')
# size in gb from which on the special bigvm handling gets enabled
@click.option('--bigvm-shares-action-size', default=1024, help='Bigvm size in GB for setting memory shares to high')
# port to use for prometheus exporter, otherwise we use 9456 as default
@click.option('--port')
def run_me(host, username, password, interval, iterations, dry_run, power_off, unregister, delete, detach_ghost_volumes, detach_ghost_ports, detach_ghost_limit, vol_check, bigvm_size, bigvm_shares_action_size, port):

    # Start http server for exported data
    if port:
        prometheus_exporter_port = int(port)
    else:
        prometheus_exporter_port = 9456
    try:
        start_http_server(prometheus_exporter_port)
    except Exception as e:
        logging.error("failed to start prometheus exporter http server: " + str(e))

    while True:

        starttime = calendar.timegm(time.gmtime())
        log.info("INFO: starting new loop run")

        gauge_value_vcenter_connection_problems = 0

        # vcenter connection
        if hasattr(ssl, '_create_unverified_context'):
            context = ssl._create_unverified_context()

            try:
                service_instance = SmartConnect(host=host,
                                            user=username,
                                            pwd=password,
                                            port=443,
                                            sslContext=context)
            except Exception as e:
                log.warn("- PLEASE CHECK MANUALLY - problems connecting to vcenter: %s - retrying in next loop run",
                    str(e))
                gauge_value_vcenter_connection_problems += 1
                gauge_vcenter_connection_problems.set(float(gauge_value_vcenter_connection_problems))

            else:
                # reset the prometheus value to 0 whenever we have a working vcenter connection again
                gauge_value_vcenter_connection_problems = 0
                gauge_vcenter_connection_problems.set(float(gauge_value_vcenter_connection_problems))

                atexit.register(Disconnect, service_instance)

                content = service_instance.content
                dc = content.rootFolder.childEntity[0]

                # this is used later
                vcenter_name = dc.name.lower()

                # iterate through all vms and get the config.hardware.device properties (and some other)
                # get vm containerview
                # TODO: destroy the view again - most probably not required, as we close the connection at the end of each loop
                view_ref = content.viewManager.CreateContainerView(
                    container=content.rootFolder,
                    type=[vim.VirtualMachine],
                    recursive=True
                )

                # define the state to verbal name mapping
                state_to_name_map["suspend_vm"] = "suspend of former os server"
                state_to_name_map["power_off_vm"] = "power off of former os server"
                state_to_name_map["unregister_vm"] = "unregister of former os server"
                state_to_name_map["rename_ds_path"] = "rename of ds path"
                state_to_name_map["delete_ds_path"] = "delete of ds path"

                # do the cleanup work
                cleanup_items(host, username, password, iterations, dry_run, power_off, unregister, delete,
                              detach_ghost_volumes, detach_ghost_ports, detach_ghost_limit, bigvm_size, bigvm_shares_action_size,
                              service_instance,
                              content, dc, view_ref)

                # check the consistency of volume attachments if requested
                if vol_check:
                    sync_volume_attachments(host, username, password, dry_run, service_instance, view_ref, vcenter_name)

                # disconnect from vcenter
                Disconnect(service_instance)

        else:
            raise Exception("maybe too old python version with ssl problems?")

        # wait the interval time
        log.info("INFO: waiting so that the next loop run will start %s minutes after the start of this loop run", str(interval))
        endtime = calendar.timegm(time.gmtime())
        time.sleep((60 * int(interval)) - (endtime - starttime))

# init dict of all vms or files we have seen already
def init_seen_dict(seen_dict):
    for i in seen_dict:
        seen_dict[i] = 0


# reset dict of all vms or files we plan to do something with (delete etc.)
def reset_to_be_dict(to_be_dict, seen_dict):
    for i in seen_dict:
        # if a machine we planned to delete no longer appears as candidate for delettion, remove it from the list
        if seen_dict[i] == 0:
            to_be_dict[i] = 0


# here we decide to wait longer before doings something (delete etc.) or finally doing it
# id here is the corresponding old openstack uuid of vm (for vms) or the file-/dirname on the
# datastore (for files and folders on the datastore)
def now_or_later(id, to_be_dict, seen_dict, what_to_do, iterations, dry_run, power_off, unregister, delete, vm, dc,
                 content, detail):
    default = 0
    seen_dict[id] = 1
    if to_be_dict.get(id, default) <= int(iterations):
        if to_be_dict.get(id, default) == int(iterations):
            if dry_run:
                log.info("- dry-run: %s %s", what_to_do, id)
                log.info("           [ %s ]", detail)
                gauge_value[('dry_run', what_to_do)] += 1
            else:
                if what_to_do == "suspend_vm":
                    log.info("- action: %s %s", state_to_name_map[what_to_do], id)
                    log.info("          [ %s ]", detail)
                    tasks.append(vm.SuspendVM_Task())
                    gauge_value[('done', what_to_do)] += 1
                elif what_to_do == "power_off_vm":
                    if power_off:
                        log.info("- action: %s %s", state_to_name_map[what_to_do], id)
                        log.info("          [ %s ]", detail)
                        tasks.append(vm.PowerOffVM_Task())
                        gauge_value[('done', what_to_do)] += 1
                elif what_to_do == "unregister_vm":
                    if unregister:
                        log.info("- action: %s %s", state_to_name_map[what_to_do], id)
                        log.info("          [ %s ]", detail)
                        vm.UnregisterVM()
                        gauge_value[('done', what_to_do)] += 1
                elif what_to_do == "rename_ds_path":
                    log.info("- action: %s %s", state_to_name_map[what_to_do], id)
                    log.info("          [ %s ]", detail)
                    newname = id.rstrip('/') + ".renamed_by_vcenter_nanny"
                    tasks.append(content.fileManager.MoveDatastoreFile_Task(sourceName=id, sourceDatacenter=dc,
                                                                            destinationName=newname,
                                                                            destinationDatacenter=dc))
                    gauge_value[('done', what_to_do)] += 1
                elif what_to_do == "delete_ds_path":
                    if delete:
                        log.info("- action: %s %s", state_to_name_map[what_to_do], id)
                        log.info("          [ %s ]", detail)
                        tasks.append(content.fileManager.DeleteDatastoreFile_Task(name=id, datacenter=dc))
                        gauge_value[('done', what_to_do)] += 1
                else:
                    log.warn("- PLEASE CHECK MANUALLY - unsupported action requested for id: %s", id)
        else:
            log.info("- plan: %s %s", state_to_name_map[what_to_do], id)
            log.info("        [ %s ] (%i/%i)", detail, to_be_dict.get(id, default) + 1, int(iterations))
            gauge_value[('plan', what_to_do)] += 1
        to_be_dict[id] = to_be_dict.get(id, default) + 1

def vc_short_name(vchost):
    # return a shortened vc hostname - i.e. vc-a-0 from vc-a-0.cc.region.some-domain.com for example
    return vchost.split(".")[0]

def vc_region_name(vchost):
    # return the region name extracted from the vc hostname - i.e. region from vc-a-0.cc.region.some-domain.com for example
    return vchost.split(".")[2]

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

    gauge_value_vcenter_get_properties_problems = 0

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

    # initialize data hete, so that we can check for an empty data later in case of an exception while getting the properties
    data = []
    # Retrieve properties
    try:
        props = collector.RetrieveContents([filter_spec])
    except vmodl.fault.ManagedObjectNotFound as e:
        log.warn("- PLEASE CHECK MANUALLY - problems retrieving properties from vcenter: %s - retrying in next loop run",
                 str(e))
        gauge_value_vcenter_get_properties_problems += 1
        gauge_vcenter_get_properties_problems.set(float(gauge_value_vcenter_get_properties_problems))
        return data

    for obj in props:
        properties = {}
        for prop in obj.propSet:
            properties[prop.name] = prop.val

        if include_mors:
            properties['obj'] = obj.obj

        data.append(properties)
    return data

def detach_ghost_port(service_instance, vm, mac_address, dry_run):
    """ Deletes virtual NIC based on mac address
    :param si: Service Instance
    :param vm: Virtual Machine Object
    :param mac_address: Mac Address of the port to be deleted
    :return: True if success
    """

    # TODO proper exception handling
    port_to_detach = None
    for dev in vm.config.hardware.device:
        if isinstance(dev, vim.vm.device.VirtualEthernetCard)   \
                and dev.macAddress == mac_address:
            port_to_detach = dev

    if not port_to_detach:
        log.warn("- PLEASE CHECK MANUALLY - the port to be deleted with mac addresss %s on instance %s does not seem to exist", mac_address, vm.config.instanceUuid)

    if dry_run:
        log.error("- dry-run: detaching ghost port with mac address %s from instance %s [%s]", mac_address, vm.config.instanceUuid, vm.config.name)
        return False

    log.error("- action: detaching ghost port with mac address %s from instance %s [%s]", mac_address, vm.config.instanceUuid, vm.config.name)
    port_to_detach_spec = vim.vm.device.VirtualDeviceSpec()
    port_to_detach_spec.operation = \
        vim.vm.device.VirtualDeviceSpec.Operation.remove
    port_to_detach_spec.device = port_to_detach

    spec = vim.vm.ConfigSpec()
    spec.deviceChange = [port_to_detach_spec]
    task = vm.ReconfigVM_Task(spec=spec)
    try:
        WaitForTask(task, si=service_instance)
    except vmodl.fault.HostNotConnected:
        log.warn("- PLEASE CHECK MANUALLY - cannot detach ghost port from instance %s - the esx host it is running on is disconnected", vm.config.instanceUuid)
        return False
    except vim.fault.InvalidPowerState as e:
        log.warn("- PLEASE CHECK MANUALLY - cannot detach ghost port from instance %s - %s", vm.config.instanceUuid, str(e.msg))
        return False
    except vim.fault.GenericVmConfigFault as e:
        log.warn("- PLEASE CHECK MANUALLY - cannot detach ghost port from instance %s - %s", vm.config.instanceUuid, str(e.msg))
        return False
    except vim.fault.InvalidState as e:
        log.warn("- PLEASE CHECK MANUALLY - cannot detach ghost port from instance %s - %s", vm.config.instanceUuid, str(e.msg))
        return False
    except vim.fault.TaskInProgress as e:
        log.warn("- PLEASE CHECK MANUALLY - cannot detach ghost port from instance %s - %s", vm.config.instanceUuid, str(e.msg))
        return False
    return True


def detach_ghost_volume(service_instance, vm, volume_uuid, dry_run):
    """ Deletes virtual NIC based on mac address
    :param si: Service Instance
    :param vm: Virtual Machine Object
    :param volume_uuid: uuid of the volume to be deleted
    :return: True if success
    """

    # TODO proper exception handling
    volume_to_detach = None
    for dev in vm.config.hardware.device:
        if isinstance(dev, vim.vm.device.VirtualDisk) \
                and dev.backing.uuid == volume_uuid:
            volume_to_detach = dev

    if not volume_to_detach:
        log.warn(
            "- PLEASE CHECK MANUALLY - the volume to be detached with uuid %s on instance %s does not seem to exist", volume_uuid, vm.config.instanceUuid)

    if dry_run:
        log.error("- dry-run: detaching ghost volume with uuid %s from instance %s [%s]", volume_uuid, vm.config.instanceUuid, vm.config.name)
        return False

    log.error("- action: detaching ghost volume with uuid %s from instance %s [%s]", volume_uuid, vm.config.instanceUuid, vm.config.name)
    volume_to_detach_spec = vim.vm.device.VirtualDeviceSpec()
    volume_to_detach_spec.operation = \
        vim.vm.device.VirtualDeviceSpec.Operation.remove
    volume_to_detach_spec.device = volume_to_detach

    spec = vim.vm.ConfigSpec()
    spec.deviceChange = [volume_to_detach_spec]
    task = vm.ReconfigVM_Task(spec=spec)
    try:
        WaitForTask(task, si=service_instance)
    except vmodl.fault.HostNotConnected:
        log.warn("- PLEASE CHECK MANUALLY - cannot detach ghost volume from instance %s - the esx host it is running on is disconnected", vm.config.instanceUuid)
        return False
    except vim.fault.InvalidPowerState as e:
        log.warn("- PLEASE CHECK MANUALLY - cannot detach ghost volume from instance %s - %s", vm.config.instanceUuid, str(e.msg))
        return False
    except vim.fault.GenericVmConfigFault as e:
        log.warn("- PLEASE CHECK MANUALLY - cannot detach ghost volume from instance %s - %s", vm.config.instanceUuid, str(e.msg))
        return False
    except vim.fault.InvalidState as e:
        log.warn("- PLEASE CHECK MANUALLY - cannot detach ghost volume from instance %s - %s", vm.config.instanceUuid, str(e.msg))
        return False
    return True


# main cleanup function
def cleanup_items(host, username, password, iterations, dry_run, power_off, unregister, delete, detach_ghost_volumes, detach_ghost_ports, detach_ghost_limit, bigvm_size, bigvm_shares_action_size, service_instance,
                  content, dc, view_ref):
    # openstack connection
    conn = connection.Connection(auth_url=os.getenv('OS_AUTH_URL'),
                                 project_name=os.getenv('OS_PROJECT_NAME'),
                                 project_domain_name=os.getenv('OS_PROJECT_DOMAIN_NAME'),
                                 username=os.getenv('OS_USERNAME'),
                                 user_domain_name=os.getenv('OS_USER_DOMAIN_NAME'),
                                 password=os.getenv('OS_PASSWORD'))

    mac_to_server = dict()
    server_to_mac = dict()
    known = dict()
    template = dict()
    ghost_port_detach_candidates = dict()
    ghost_volume_detach_candidates = dict()
    ghost_port_detached = dict()
    ghost_volume_detached = dict()

    global gauge_value_empty_vvol_folders

    # reset all gauge counters
    for kind in [ "plan", "dry_run", "done"]:
        for what in state_to_name_map:
            gauge_value[(kind, what)] = 0
    gauge_value_ghost_volumes = 0
    gauge_value_ghost_volumes_ignored = 0
    gauge_value_ghost_volumes_detached = 0
    gauge_value_ghost_volumes_detach_errors = 0
    gauge_value_ghost_ports = 0
    gauge_value_ghost_ports_ignored = 0
    gauge_value_ghost_ports_detached = 0
    gauge_value_ghost_ports_detach_errors = 0
    gauge_value_template_mounts = 0
    gauge_value_template_ports = 0
    gauge_value_eph_shadow_vms = 0
    gauge_value_datastore_no_access = 0
    gauge_value_empty_vvol_folders = 0
    gauge_value_vcenter_task_problems = 0
    gauge_value_openstack_connection_problems = 0
    gauge_value_unknown_vcenter_templates = 0
    gauge_value_complete_orphans = 0
    gauge_value_big_vm_disable_drs = 0
    gauge_value_big_vm_memory_shares_high = 0

    # get all servers, volumes, snapshots and images from openstack to compare the resources we find on the vcenter against
    # TODO: this should be filtered by the current vc like below - it works without as we only check if some resource is in
    #       openstack or not and we do not really care about the vcenter here (although we might overlook cases where
    #       something got moved from one vc to another)
    try:
        service = "nova"
        temporary_server_list = list(conn.compute.servers(details=False, all_projects=1))
        if not temporary_server_list:
            raise RuntimeError('- PLEASE CHECK MANUALLY - did not get any nova instances back from the nova api - this should in theory never happen ...')
        for server in temporary_server_list:
            known[server.id] = 'server'
        service = "cinder"
        temporary_volume_list = list(conn.block_store.volumes(details=False, all_projects=1))
        if not temporary_volume_list:
            raise RuntimeError('- PLEASE CHECK MANUALLY - did not get any cinder volumes back from the cinder api - this should in theory never happen ...')
        for volume in temporary_volume_list:
            known[volume.id] = 'volume'
        service = "cinder"
        temporary_snapshot_list = list(conn.block_store.snapshots(details=False, all_projects=1))
        if temporary_snapshot_list:
            for snapshot in temporary_snapshot_list:
                known[snapshot.id] = 'snapshot'
        service = "glance"
        temporary_image_list = list(conn.image.images())
        if not temporary_image_list:
            raise RuntimeError('- PLEASE CHECK MANUALLY - did not get any glance images back from the glance api - this should in theory never happen ...')
        for image in temporary_image_list:
            known[image.id] = 'image'
        service = "neutron"
        temporary_port_list = list(conn.network.ports())
        if not temporary_port_list:
            raise RuntimeError('- PLEASE CHECK MANUALLY - did not get any neutron ports back from the neutron api - this should in theory never happen ...')
        # build a dict of ports related to the network interfaces on the servers on the vcenter
        for port in temporary_port_list:
            # we only care about ports handled by nova-compute here
            if str(port.binding_host_id).startswith('nova-compute-'):
                # new style code - build the comparision around the instance uuid instead of the mac address as it is definitely unique per region
                # a server can have multiple mac addresses, so keep them in a list
                if server_to_mac.get(port.device_id):
                    server_to_mac[str(port.device_id)].append(str(port.mac_address))
                else:
                    server_to_mac[str(port.device_id)] = [str(port.mac_address)]

    except exceptions.HttpException as e:
        log.warn(
            "- PLEASE CHECK MANUALLY - problems retrieving information from openstack %s: %s - retrying in next loop run",
            service, str(e))
        gauge_value_openstack_connection_problems += 1
        gauge_openstack_connection_problems.set(float(gauge_value_openstack_connection_problems))
        return
    except exceptions.SDKException as e:
        log.warn(
            "- PLEASE CHECK MANUALLY - problems retrieving information from openstack %s: %s - retrying in next loop run",
            service, str(e))
        gauge_value_openstack_connection_problems += 1
        gauge_openstack_connection_problems.set(float(gauge_value_openstack_connection_problems))
        return
    else:
        # reset the prometheus value to 0 whenever we have a working openstack connection again
        gauge_value_openstack_connection_problems = 0
        gauge_openstack_connection_problems.set(float(gauge_value_openstack_connection_problems))

    # the properties we want to collect - some of them are not yet used, but will at a later
    # development stage of this script to validate the volume attachments with cinder and nova
    vm_properties = [
        "config.hardware.device",
        "config.hardware.memoryMB",
        "resourcePool",
        "config.name",
        "config.uuid",
        "config.instanceUuid",
        "config.template",
        "config.annotation",
        "config.memoryAllocation.shares.level"
    ]

    # collect the properties for all vms
    data = collect_properties(service_instance, view_ref, vim.VirtualMachine,
                              vm_properties, True)
    # in case we have problems getting the properties from the vcenter, start over from the beginning
    if data is None:
        return

    # create a dict of volumes mounted to vms to compare the volumes we plan to delete against
    # to find possible ghost volumes
    vc_server_uuid_with_mounted_volume = dict()
    vc_server_name_with_mounted_volume = dict()
    big_vm_drs_action_necessary = []
    big_vm_memory_shares_action_necessary = []
    # iterate over the list of vms
    for k in data:
        # only work with results, which have an instance uuid defined and are openstack vms (i.e. have an annotation set)
        if k.get('config.instanceUuid') and openstack_re.match(k.get('config.annotation', 'no_annotation')):
            # check if this instance is a vcenter template
            if k.get('config.template'):
                template[k['config.instanceUuid']] = k['config.template']
                log.debug("==> uuid: %s - template: %s", str(k['config.instanceUuid']), str(k['config.template']))
            # get the config.hardware.device property out of the data dict and iterate over its elements
            # for j in k['config.hardware.device']:
            # this check seems to be required as in one bb i got a key error otherwise - looks like a vm without that property
            # Collect 'BigVMs' with >= bigvm_size
            if k.get('config.hardware.memoryMB', 0) >= bigvm_size * 1024:
                cluster = k['resourcePool'].owner
                for drs in cluster.configuration.drsVmConfig:
                    if str(drs.key) != str(k['obj']):
                        continue

                    wanted_behavior = vim.cluster.DrsConfigInfo.DrsBehavior.partiallyAutomated
                    if not drs.enabled or drs.enabled and drs.behavior != wanted_behavior:
                        log.warn("- discovered bigVM %s with %.02f TB Ram and old DRS override",
                                 k['config.name'],
                                 k['config.hardware.memoryMB'] / (1024.0 * 1024.0))
                        big_vm_drs_action_necessary.append((k['obj'], cluster, drs))
                    break
                else:
                    log.warn("- discovered new bigVM %s with %.02f TB Ram and no DRS override",
                             k['config.name'],
                             k['config.hardware.memoryMB'] / (1024.0 * 1024.0))
                    big_vm_drs_action_necessary.append((k['obj'], cluster, None))
            # Collect 'BigVMs' with >= bigvm_shares_action_size
            if bigvm_shares_action_size:
                if k.get('config.hardware.memoryMB', 0) >= bigvm_shares_action_size * 1024 and str(k.get('config.memoryAllocation.shares.level')) != 'high':
                    log.warn("- discovered bigVM %s with %.02f TB Ram and memory shares not yet at high",
                                 k['config.name'],
                                 k['config.hardware.memoryMB'] / (1024.0 * 1024.0))
                    big_vm_memory_shares_action_necessary.append(k['obj'])
            if k.get('config.hardware.device'):
                for j in k.get('config.hardware.device'):
                    # we are only interested in disks for ghost volumes ...
                    # old test was: if 2000 <= j.key < 3000:
                    if isinstance(j, vim.vm.device.VirtualDisk):
                        # we only care for vvols - in the past we checked starting with 2001 as 2000 usual was the eph
                        # storage, but it looks like eph can also be on another id and 2000 could be a vvol as well ...
                        if j.backing.fileName.lower().startswith('[vvol_') or j.backing.fileName.lower().startswith('[vmfs_'):
                            vc_server_uuid_with_mounted_volume[j.backing.uuid] = k['config.instanceUuid']
                            vc_server_name_with_mounted_volume[j.backing.uuid] = k['config.name']
                            log.debug("==> mount - instance: %s - volume: %s", str(k['config.instanceUuid']), str(j.backing.uuid))
                    # ... and network interfaces for ghost ports
                    # old test was: if 4000 <= j.key < 5000:
                    if isinstance(j, vim.vm.device.VirtualEthernetCard):
                        # new style code - build the comparision around the instance uuid instead of the mac address as it is definitely unique per region
                        if template.get(k['config.instanceUuid']):
                            log.warn("- discovered ghost port with mac %s attached to vcenter template %s [%s] - ignoring it", str(j.macAddress), k['config.instanceUuid'], k['config.name'])
                            gauge_value_ghost_ports += 1
                            gauge_value_template_ports += 1
                            gauge_value_ghost_ports_ignored += 1
                        elif server_to_mac.get(k['config.instanceUuid']):
                            mac_address_found = False
                            for i in server_to_mac[k['config.instanceUuid']]:
                                log.debug("==> instance %s - mac %s", k['config.instanceUuid'], i)
                                if str(j.macAddress) == i:
                                    mac_address_found = True
                            if mac_address_found:
                                log.debug("==> port with mac %s on %s [%s] is in sync between vcenter and neutron", str(j.macAddress), str(k['config.instanceUuid']), k['config.name'])
                            else:
                                log.warn("- discovered ghost port with mac %s on %s [%s] in vcenter", str(j.macAddress), str(k['config.instanceUuid']), k['config.name'])
                                gauge_value_ghost_ports += 1
                                # if we plan to delete ghost ports, collect them in a dict of mac addresses by instance uuid
                                if detach_ghost_ports:
                                    # multiple ghost ports are possible for one instance, thus we need to put the ghost ports into a list
                                    if ghost_port_detach_candidates.get(k['config.instanceUuid']):
                                        ghost_port_detach_candidates[k['config.instanceUuid']].append(str(j.macAddress))
                                    else:
                                        ghost_port_detach_candidates[k['config.instanceUuid']] = [str(j.macAddress)]
                        else:
                            log.warn("- discovered ghost port with mac %s on %s [%s] in vcenter - instance does not seem to exist in neutron and is not a vcenter template", str(j.macAddress), k['config.instanceUuid'], k['config.name'])
                            gauge_value_ghost_ports += 1
                            # if we plan to delete ghost ports, collect them in a dict of mac addresses by instance uuid
                            if detach_ghost_ports:
                                # multiple ghost ports are possible for one instance, thus we need to put the ghost ports into a list
                                if ghost_port_detach_candidates.get(k['config.instanceUuid']):
                                    ghost_port_detach_candidates[k['config.instanceUuid']].append(str(j.macAddress))
                                else:
                                    ghost_port_detach_candidates[k['config.instanceUuid']] = [str(j.macAddress)]

    # do the check from the other end: see for which vms or volumes in the vcenter we do not have any openstack info
    missing = dict()
    # a dict of locations by uuid known to openstack
    not_missing = dict()

    # Disable DRS for bigVMs
    for vm, cluster, old_drs_vm_config_info in big_vm_drs_action_necessary:
        drs_vm_config_info = vim.cluster.DrsVmConfigInfo()
        drs_vm_config_info.enabled = True
        drs_vm_config_info.behavior = vim.cluster.DrsConfigInfo.DrsBehavior.partiallyAutomated
        drs_vm_config_info.key = vm

        drs_vm_config_spec = vim.cluster.DrsVmConfigSpec()
        if old_drs_vm_config_info:
            drs_vm_config_spec.operation = vim.option.ArrayUpdateSpec.Operation.edit
        else:
            drs_vm_config_spec.operation = vim.option.ArrayUpdateSpec.Operation.add
        drs_vm_config_spec.info = drs_vm_config_info

        cluster_spec = vim.cluster.ConfigSpecEx()
        cluster_spec.drsVmConfigSpec = [drs_vm_config_spec]

        log.warn("- setting drs override for server %s", vm.config.name)
        cluster.ReconfigureComputeResource_Task(cluster_spec, True)
        gauge_value_big_vm_disable_drs += 1

    # set memory shares to high for bigvms
    for vm in big_vm_memory_shares_action_necessary:
        configspec = vim.vm.ConfigSpec()
        configspec.memoryAllocation = vim.ResourceAllocationInfo()
        configspec.memoryAllocation.shares = vim.SharesInfo()
        configspec.memoryAllocation.shares.level = 'high'
        log.warn("- setting memory shares to high for server %s", vm.config.name)
        task = vm.Reconfigure(configspec)
        try:
                # wait for the async task to finish 
                WaitForTask(task, si=service_instance)
        except Exception as e:
                log.warn("- PLEASE CHECK MANUALLY - problems running vcenter tasks: %s - they will run next time then", e.msg)
                gauge_value_vcenter_task_problems += 1
        gauge_value_big_vm_memory_shares_high += 1

    # iterate through all datastores in the vcenter
    for ds in dc.datastore:
        # only consider eph and vvol datastores
        if ds.name.lower().startswith('eph') or ds.name.lower().startswith('vvol') or ds.name.lower().startswith('vmfs'):
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
                        # only handle uuids which are not templates in the vcenter - otherwise theny might confuse the nanny
                        if template.get(uuid) is True:
                            log.warn("- PLEASE CHECK MANUALLY - uuid %s is a vcenter template and unknown to openstack",
                                     uuid)
                            gauge_value_unknown_vcenter_templates += 1
                        else:
                            # multiple locations are possible for one uuid, thus we need to put the locations into a list
                            if uuid in missing:
                                missing[uuid].append(location)
                            else:
                                missing[uuid] = [location]
                    else:
                        # multiple locations are possible for one uuid, thus we need to put the locations into a list
                        if uuid in not_missing:
                            not_missing[uuid].append(location)
                        else:
                            not_missing[uuid] = [location]
            except vim.fault.InaccessibleDatastore as e:
                log.warn("- PLEASE CHECK MANUALLY - something went wrong trying to access this datastore (vim.fault.InaccessibleDatastore): %s", e.msg)
                gauge_value_datastore_no_access += 1
            except vim.fault.FileNotFound as e:
                log.warn("- PLEASE CHECK MANUALLY - something went wrong trying to access this datastore (vim.fault.FileNotFound): %s", e.msg)
                gauge_value_datastore_no_access += 1
            except vim.fault.NoHost as e:
                log.warn("- PLEASE CHECK MANUALLY - something went wrong trying to access this datastore (vim.fault.NoHost): %s", e.msg)
                gauge_value_datastore_no_access += 1
            except vmodl.fault.SystemError as e:
                log.warn("- PLEASE CHECK MANUALLY - something went wrong trying to access this datastore (vmodl.fault.SystemError): %s", e.msg)
                gauge_value_datastore_no_access += 1
            except vmodl.fault.HostCommunication as e:
                log.warn("- PLEASE CHECK MANUALLY - something went wrong trying to access this datastore (vmodl.fault.HostCommunication): %s", e.msg)
                gauge_value_datastore_no_access += 1
            except task.info.error as e:
                log.warn("- PLEASE CHECK MANUALLY - problems running vcenter tasks: %s - they will run next time then", e.msg)
                gauge_value_vcenter_task_problems += 1

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
        if vc_server_uuid_with_mounted_volume.get(item):
            if template.get(vc_server_uuid_with_mounted_volume[item]) is True:
                log.warn("- discovered ghost volume %s mounted on vcenter template %s - ignoring it", item,
                         vc_server_uuid_with_mounted_volume[item])
                gauge_value_ghost_volumes += 1
                gauge_value_template_mounts += 1
                gauge_value_ghost_volumes_ignored += 1
            else:
                log.warn("- discovered ghost volume %s mounted on %s [%s] in vcenter", item,
                         vc_server_uuid_with_mounted_volume[item], vc_server_name_with_mounted_volume[item])
                gauge_value_ghost_volumes += 1
                # if we plan to delete ghost volumes, collect them in a dict of volume uuids by instance uuid
                if detach_ghost_volumes:
                    # multiple ghost volumes are possible for one instance, thus we need to put the ghost volumes into a list
                    if ghost_volume_detach_candidates.get(vc_server_uuid_with_mounted_volume[item]):
                        ghost_volume_detach_candidates[vc_server_uuid_with_mounted_volume[item]].append(item)
                    else:
                        ghost_volume_detach_candidates[vc_server_uuid_with_mounted_volume[item]] = [item]
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
                        # maybe there is a better way to get the moid ...
                        # TODO - test: vm_moid = vm._moId
                        vm_moid = str(vm).strip('"\'').split(":")[1]
                        power_state = vm.runtime.powerState
                        # is the vm located on vvol storage - needed later to check if its a volume shadow vm
                        if vm.config.files.vmPathName.lower().startswith('[vvol') or vm.config.files.vmPathName.lower().startswith('[vmfs'):
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
                                now_or_later(vm.config.instanceUuid, vms_to_be_suspended, vms_seen, "suspend_vm",
                                             iterations,
                                             dry_run, power_off, unregister, delete, vm, dc, content, filename + " / " + vm_moid + " / " + vm.config.name)
                            # if already suspended the planned action is to power off the vm
                            elif power_state == 'suspended':
                                vmxmarked[path] = True
                                now_or_later(vm.config.instanceUuid, vms_to_be_poweredoff, vms_seen, "power_off_vm",
                                             iterations,
                                             dry_run, power_off, unregister, delete, vm, dc, content, filename + " / " + vm_moid + " / " + vm.config.name)
                            # if already powered off the planned action is to unregister the vm
                            elif power_state == 'poweredOff':
                                vmxmarked[path] = True
                                now_or_later(vm.config.instanceUuid, vms_to_be_unregistered, vms_seen,
                                             "unregister_vm",
                                             iterations,
                                             dry_run, power_off, unregister, delete, vm, dc, content, filename + " / " + vm_moid + " / " + vm.config.name)
                        # this should not happen
                        elif (
                                vm.config.hardware.memoryMB == 128 and vm.config.hardware.numCPU == 1 and power_state == 'poweredOff' and not is_vvol and has_no_nic):
                            log.warn("- PLEASE CHECK MANUALLY - possible orphan shadow vm on eph storage: %s", path)
                            gauge_value_eph_shadow_vms += 1
                        # this neither
                        else:
                            log.warn(
                                "- PLEASE CHECK MANUALLY - this vm seems to be neither a former openstack vm nor an orphan shadow vm: %s",
                                path)
                            gauge_value_complete_orphans += 1

                    # there is no vm anymore for the file path - planned action is to delete the file
                    elif not vmxmarked.get(path, False):
                        vmxmarked[path] = True
                        if path.lower().startswith("[eph"):
                            if path.endswith(".renamed_by_vcenter_nanny/"):
                                # if already renamed finally delete
                                now_or_later(str(path), files_to_be_deleted, files_seen, "delete_ds_path",
                                             iterations, dry_run, power_off, unregister, delete, vm, dc, content,
                                             filename)
                            else:
                                # first rename the file before deleting them later
                                now_or_later(str(path), files_to_be_renamed, files_seen, "rename_ds_path",
                                             iterations, dry_run, power_off, unregister, delete, vm, dc, content,
                                             filename)
                        else:
                            # vvol storage
                            # for vvols we have to mark based on the full path, as we work on them file by file
                            # and not on a directory base
                            vvolmarked[fullpath] = True
                            if fullpath.endswith(".renamed_by_vcenter_nanny/"):
                                now_or_later(str(fullpath), files_to_be_deleted, files_seen, "delete_ds_path",
                                             iterations, dry_run, power_off, unregister, delete, vm, dc, content,
                                             filename)
                            else:
                                now_or_later(str(fullpath), files_to_be_renamed, files_seen, "rename_ds_path",
                                             iterations, dry_run, power_off, unregister, delete, vm, dc, content,
                                             filename)

                    if len(tasks) % 8 == 0:
                        try:
                            WaitForTasks(tasks[-8:], si=service_instance)
                        except vmodl.fault.ManagedObjectNotFound as e:
                            log.warn("- PLEASE CHECK MANUALLY - problems running vcenter tasks: %s - they will run next time then", str(e))
                            gauge_value_vcenter_task_problems += 1

                # in case of a vmdk or vmx.renamed_by_vcenter_nanny
                # eph storage case - we work on directories
                elif path.lower().startswith("[eph") and not vmxmarked.get(path, False) and not vmdkmarked.get(path,
                                                                                                               False):
                    # mark to not redo it for other vmdks as we are working on the dir at once
                    vmdkmarked[path] = True
                    if path.endswith(".renamed_by_vcenter_nanny/"):
                        now_or_later(str(path), files_to_be_deleted, files_seen, "delete_ds_path",
                                     iterations, dry_run, power_off, unregister, delete, None, dc, content, filename)
                    else:
                        now_or_later(str(path), files_to_be_renamed, files_seen, "rename_ds_path",
                                     iterations, dry_run, power_off, unregister, delete, None, dc, content, filename)
                # vvol storage case - we work file by file as we can't rename or delete the vvol folders
                elif (path.lower().startswith("[vvol") or path.lower().startswith("[vmfs")) and not vvolmarked.get(fullpath, False):
                    # vvol storage
                    if fullpath.endswith(".renamed_by_vcenter_nanny"):
                        now_or_later(str(fullpath), files_to_be_deleted, files_seen, "delete_ds_path",
                                     iterations, dry_run, power_off, unregister, delete, None, dc, content, filename)
                    else:
                        now_or_later(str(fullpath), files_to_be_renamed, files_seen, "rename_ds_path",
                                     iterations, dry_run, power_off, unregister, delete, None, dc, content, filename)

                if len(tasks) % 8 == 0:
                    try:
                        WaitForTasks(tasks[-8:], si=service_instance)
                    except vmodl.fault.ManagedObjectNotFound as e:
                        log.warn("- PLEASE CHECK MANUALLY - problems running vcenter tasks: %s - they will run next time then", str(e))
                        gauge_value_vcenter_task_problems += 1


    # cleanup detached ports and/or volumes if requested
    if detach_ghost_ports or detach_ghost_volumes:
        # ghost volumes and ports should not appear often, so limit the maximum of them to delete
        # to avoid the risk of accidentally detaching too many of them due to some failure somewhere else
        if len(ghost_port_detach_candidates) > detach_ghost_limit:
            log.warn("- PLEASE CHECK MANUALLY - number of instances with ghost ports to be deleted is larger than --detach-ghost-limit=%s - denying to delete the ghost ports", str(detach_ghost_limit))
        if len(ghost_volume_detach_candidates) > detach_ghost_limit:
            log.warn("- PLEASE CHECK MANUALLY - number of instances with ghost volumes to be deleted is larger than --detach-ghost-limit=%s - denying to delete the ghost volumes", str(detach_ghost_limit))
        # build a dict of all uuids from the missing and not_missing ones
        all_uuids = dict()
        all_uuids.update(missing)
        all_uuids.update(not_missing)
        # go through all uuids we know
        for item, locationlist in six.iteritems(all_uuids):
            # if any of them has a ghost volume or port attached do something about it
            if ghost_port_detach_candidates.get(item) or ghost_volume_detach_candidates.get(item):
                # find the corresponding .vmx file and vm
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
                            # if this vm is a ghost port detach candidate
                            if ghost_port_detach_candidates.get(item):
                                # only do something if we are below detach_ghost_limit for the ports
                                if len(ghost_port_detach_candidates) <= detach_ghost_limit:
                                    # in case we have multiple ghost ports
                                    for ghost_port_detach_candidate in ghost_port_detach_candidates.get(item):
                                        # double check that the port is still a ghost port to avoid accidentally deleting stuff due to timing issues
                                        if not any(True for _ in conn.network.ports(mac_address = ghost_port_detach_candidate)):
                                            if detach_ghost_port(service_instance, vm, ghost_port_detach_candidate, dry_run):
                                                gauge_value_ghost_ports_detached += 1
                                                # here we do not need to worry about multiple ghost ports per instance
                                                # as the instance is orphan or not, independent of the numer of ghost ports
                                                ghost_port_detached[item] = 1
                                            else:
                                                gauge_value_ghost_ports_detach_errors += 1
                                                ghost_port_detached[item] = 0
                                        else:
                                            log.warn("- looks like the port with the mac address %s on instance %s has only been temporary assumed to be a ghost port - not doing anything with it ...", ghost_port_detach_candidate, item)
                                            gauge_value_ghost_ports_ignored += 1
                            # if this vm is a ghost volume detach candidate
                            elif ghost_volume_detach_candidates.get(item):
                                # only do something if we are below detach_ghost_limit for the volumes
                                if len(ghost_volume_detach_candidates) <= detach_ghost_limit:
                                    # in case we have multiple ghost volumes
                                    for ghost_volume_detach_candidate in ghost_volume_detach_candidates.get(item):
                                        # double check that the volume is really a ghost volume, i.e. even now not know to openstack
                                        try:
                                            # this will give a ResourceNotFound in case the volume does not exist in openstack
                                            conn.block_storage.get_volume(ghost_volume_detach_candidate)
                                            log.warn("- looks like the volume with the uuid %s on instance %s has only been temporary assumed to be a ghost volume - not doing anything with it ...", ghost_volume_detach_candidate, item)
                                            gauge_value_ghost_volumes_ignored += 1
                                        except exceptions.ResourceNotFound:
                                            conn.block_store.volumes(details=False, all_projects=1)
                                            if detach_ghost_volume(service_instance, vm, ghost_volume_detach_candidate, dry_run):
                                                gauge_value_ghost_volumes_detached += 1
                                                # here we do not need to worry about multiple ghost volumes per instance
                                                # as the instance is orphan or not, independent of the numer of ghost volumes
                                                ghost_volume_detached[item] = 1
                                            else:
                                                gauge_value_ghost_volumes_detach_errors += 1
                                                ghost_volume_detached[item] = 0
        for i in ghost_port_detach_candidates:
            if not (ghost_port_detached.get(i) == 0 or ghost_port_detached.get(i) == 1):
                # use len here to get the proper count in case we have multiple ports for one instance
                gauge_value_ghost_ports_ignored += len(ghost_port_detach_candidates[i])
                if known.get(i) == 'server':
                    log.warn("- PLEASE CHECK MANUALLY - cannot detach ghost port(s) from instance %s - most probably it is an orphan at vcenter level or detaching is denied due to too many ghost ports - it is NOT an orphan in openstack - ignoring it", i)
                else:
                    log.warn("- PLEASE CHECK MANUALLY - cannot detach ghost port(s) from instance %s - most probably it is an orphan at vcenter level or detaching is denied due to too many ghost ports - it is an orphan in openstack - ignoring it", i)
        for i in ghost_volume_detach_candidates:
            if not (ghost_volume_detached.get(i) == 0 or ghost_volume_detached.get(i) == 1):
                # use len here to get the proper count in case we have multiple ports for one instance
                gauge_value_ghost_volumes_ignored += len(ghost_volume_detach_candidates[i])
                if known.get(i) == 'server':
                    log.warn("- PLEASE CHECK MANUALLY - cannot detach ghost volume(s) from instance %s - most probably it is an orphan at vcenter level or detaching is denied due to too many ghost volumes - it is NOT an orphan in openstack - ignoring it", i)
                else:
                    log.warn("- PLEASE CHECK MANUALLY - cannot detach ghost volume(s) from instance %s - most probably it is an orphan at vcenter level or detaching is denied due to too many ghost volumes - it is an orphan in openstack - ignoring it", i)

    # send the counters to the prometheus exporter - ugly for now, will change
    for kind in [ "plan", "dry_run", "done"]:
        gauge_suspend_vm.labels(kind).set(float(gauge_value[(kind, "suspend_vm")]))
        gauge_power_off_vm.labels(kind).set(float(gauge_value[(kind, "power_off_vm")]))
        gauge_unregister_vm.labels(kind).set(float(gauge_value[(kind, "unregister_vm")]))
        gauge_rename_ds_path.labels(kind).set(float(gauge_value[(kind, "rename_ds_path")]))
        gauge_delete_ds_path.labels(kind).set(float(gauge_value[(kind, "delete_ds_path")]))
    gauge_ghost_volumes.set(float(gauge_value_ghost_volumes))
    gauge_ghost_volumes_ignored.set(float(gauge_value_ghost_volumes_ignored))
    gauge_ghost_volumes_detached.set(float(gauge_value_ghost_volumes_detached))
    gauge_ghost_volumes_detach_errors.set(float(gauge_value_ghost_volumes_detach_errors))
    gauge_ghost_ports.set(float(gauge_value_ghost_ports))
    gauge_ghost_ports_ignored.set(float(gauge_value_ghost_ports_ignored))
    gauge_ghost_ports_detached.set(float(gauge_value_ghost_ports_detached))
    gauge_ghost_ports_detach_errors.set(float(gauge_value_ghost_ports_detach_errors))
    gauge_template_mounts.set(float(gauge_value_template_mounts))
    gauge_template_mounts.set(float(gauge_value_template_ports))
    gauge_eph_shadow_vms.set(float(gauge_value_eph_shadow_vms))
    gauge_datastore_no_access.set(float(gauge_value_datastore_no_access))
    gauge_empty_vvol_folders.set(float(gauge_value_empty_vvol_folders))
    gauge_vcenter_task_problems.set(float(gauge_value_vcenter_task_problems))
    gauge_unknown_vcenter_templates.set(float(gauge_value_unknown_vcenter_templates))
    gauge_complete_orphans.set(float(gauge_value_complete_orphans))
    gauge_big_vm_disable_drs.set(float(gauge_value_big_vm_disable_drs))
    gauge_big_vm_memory_shares_high.set(float(gauge_value_big_vm_memory_shares_high))

    # reset the dict of vms or files we plan to do something with for all machines we did not see or which disappeared
    reset_to_be_dict(vms_to_be_suspended, vms_seen)
    reset_to_be_dict(vms_to_be_poweredoff, vms_seen)
    reset_to_be_dict(vms_to_be_unregistered, vms_seen)
    reset_to_be_dict(files_to_be_deleted, files_seen)
    reset_to_be_dict(files_to_be_renamed, files_seen)


# main volume attachment sync function - maybe this will be folded into the cleanup function above as well in the future
def sync_volume_attachments(host, username, password, dry_run, service_instance, view_ref, vcenter_name):

    # openstack connection
    conn = connection.Connection(auth_url=os.getenv('OS_AUTH_URL'),
                                 project_name=os.getenv('OS_PROJECT_NAME'),
                                 project_domain_name=os.getenv('OS_PROJECT_DOMAIN_NAME'),
                                 username=os.getenv('OS_USERNAME'),
                                 user_domain_name=os.getenv('OS_USER_DOMAIN_NAME'),
                                 password=os.getenv('OS_PASSWORD'))

    os_volumes_attached_at_server = dict()
    os_servers_with_attached_volume = dict()
    os_all_servers = []
    os_all_volumes = []
    vc_all_servers = []
    vc_all_volumes = []

    gauge_value_volume_attachment_inconsistencies = 0

    # get all servers, volumes, snapshots and images from openstack to compare the resources we find on the vcenter against
    try:

        # we are not getting the vc volumes and instances are running on from the project tags
        # as there might be cases there this is not true (i.e. blackbox tests etc.)
        # service = "keystone"
        # temporary_project_list = list(conn.identity.projects())
        # if not temporary_project_list:
        #     raise RuntimeError('- PLEASE CHECK MANUALLY - did not get any projects back from the keystone api - this should in theory never happen ...')

        service = "cinder"
        temporary_volume_list = list(conn.block_store.volumes(details=True, all_projects=1))
        if not temporary_volume_list:
            raise RuntimeError('- PLEASE CHECK MANUALLY - did not get any cinder volumes back from the cinder api - this should in theory never happen ...')
        service = "nova"
        temporary_server_list = list(conn.compute.servers(details=True, all_projects=1))
        if not temporary_server_list:
            raise RuntimeError('- PLEASE CHECK MANUALLY - did not get any nova instances back from the nova api - this should in theory never happen ...')
        temporary_aggregate_list = list(conn.compute.aggregates())
        if not temporary_aggregate_list:
            raise RuntimeError('- PLEASE CHECK MANUALLY - did not get any nova aggregates back from the nova api - this should in theory never happen ...')

        # we are not getting the vc volumes and instances are running on from the project tags
        # as there might be cases there this is not true (i.e. blackbox tests etc.)
        # # build a dict of the projects and their vcenters used to find the proper shard
        # project_in_shard = dict()
        # # build the az name from vc_short_name and vc_region_name because we have the az defined for
        # # volumes and instances and want to compare against that later - az = region name + letter (qa-de-1a)
        # az = str(vc_region_name(host)) + str(vc_short_name(host).split('-')[1])
        # for project in temporary_project_list:
        #     try:
        #         # check if the vcenter this nanny is connected to is in the shards tag list for each
        #         # project - if yes then assign the constructed az name to it in the dict to compare
        #         # against the az of the instances and volumes later - otherwise set it to the special
        #         # string "no_shards" if there is no project.tags defined (i.e. no shards enabled here)
        #         if project.tags and (vc_short_name(host) in project.tags):
        #             project_in_shard[project.id] = az
        #         if not project.tags:
        #             project_in_shard[project.id] = 'no_shard'
        #         log.debug("==> project %s - tags: %s)", project.id, str(project.tags))
        #     except Exception as e:
        #         log.debug("==> project %s most probably has no tags defined (exception %s)", project.id, str(e))

        # build dicts to map servers and volumes to the vc they are running on to filter by it later
        hosts_per_vc = dict()
        for aggregate in temporary_aggregate_list:
            if aggregate.name:
                match = re.search(r"^vc-[a-z]-[0-9]$", aggregate.name)
                if match:
                    hosts_per_vc[aggregate.name] = aggregate.hosts

        host_from_server_uuid = dict()
        for server in temporary_server_list:
            if server.compute_host:
                host_from_server_uuid[server.id] = server.compute_host

        vc_from_server_uuid = dict()
        for server in host_from_server_uuid:
            for vcenter in hosts_per_vc:
                if host_from_server_uuid[server] in hosts_per_vc[vcenter]:
                    vc_from_server_uuid[server] = vcenter

        vc_from_volume_uuid = dict()
        for volume in temporary_volume_list:
            if volume.host:
                match = re.search(r"(vc-[a-z]-[0-9])", volume.host)
                if match:
                    vc_from_volume_uuid[volume.id] = match.groups(1)[0]

        for volume in temporary_volume_list:

            # we are not getting the vc volumes and instances are running on from the project tags
            # as there might be cases there this is not true (i.e. blackbox tests etc.)
            # # we only care about volumes from the vcenter (shard) this nanny is taking care of
            # # compare the az of the volume to the az value based on the shard tags above
            # log.debug('==> p: %s - p-sh: %s - v: %s - v-az: %s - vc: %s', volume.project_id, project_in_shard.get(volume.project_id), volume.id, volume.availability_zone.lower(), vcenter_name)
            # if (project_in_shard.get(volume.project_id) and (volume.availability_zone.lower() ==  project_in_shard.get(volume.project_id))) \
            #     or ((project_in_shard.get(volume.project_id) == 'no_shard') and (volume.availability_zone.lower() == vcenter_name)):

            # we only care about instances from the vcenter (shard) this nanny is taking care of
            log.debug('==> p: %s - v: %s - v-vc: %s - vc-sn: %s', volume.project_id, volume.id, vc_from_volume_uuid.get(volume.id), vc_short_name(host))
            if vc_from_volume_uuid.get(volume.id) == vc_short_name(host):
                os_all_volumes.append(volume.id)
                log.debug("==> os_all_volumes added: %s",str(volume.id))
                if volume.attachments:
                    for attachment in volume.attachments:
                        if os_servers_with_attached_volume.get(volume.id):
                            os_servers_with_attached_volume[volume.id].append(attachment['server_id'])
                        else:
                            os_servers_with_attached_volume[volume.id] = [attachment['server_id']]
            else:
                log.debug("==> os_all_volumes not added: %s",str(volume.id))
        for server in temporary_server_list:

            # we are not getting the vc volumes and instances are running on from the project tags
            # as there might be cases there this is not true (i.e. blackbox tests etc.)
            # # compare the az of the server to the az value based on the shard tags above
            # log.debug('==> p: %s - p-sh: %s - s: %s - s-az: %s - vc: %s', server.project_id, project_in_shard.get(server.project_id), server.id, server.availability_zone.lower(), vcenter_name)
            # if (project_in_shard.get(server.project_id) and (server.availability_zone.lower() ==  project_in_shard.get(server.project_id))) \
            #     or ((project_in_shard.get(server.project_id) == 'no_shard') and (server.availability_zone.lower() == vcenter_name)):

            # we only care about instances from the vcenter (shard) this nanny is taking care of
            log.debug('==> p: %s - s: %s - s-vc: %s - vc-sn: %s', server.project_id, server.id, vc_from_server_uuid.get(server.id), vc_short_name(host))
            if vc_from_server_uuid.get(server.id) == vc_short_name(host):
                os_all_servers.append(server.id)
                log.debug("==> os_all_servers added: %s",str(server.id))
                if server.attached_volumes:
                    for attachment in server.attached_volumes:
                        if os_volumes_attached_at_server.get(server.id):
                            os_volumes_attached_at_server[server.id].append(attachment['id'])
                        else:
                            os_volumes_attached_at_server[server.id] = [attachment['id']]
            else:
                log.debug("==> os_all_servers not added: %s",str(server.id))

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

    # the properties we want to collect - some of them are not yet used, but will at a later
    # development stage of this script to validate the volume attachments with cinder and nova
    vm_properties = [
        "config.hardware.device",
        "config.name",
        "config.uuid",
        "config.instanceUuid",
        "config.template",
        "config.annotation"
    ]

    # collect the properties for all vms
    data = collect_properties(service_instance, view_ref, vim.VirtualMachine,
                              vm_properties, True)
    # in case we have problems getting the properties from the vcenter, start over from the beginning
    if data is None:
        return

    # create a dict of volumes mounted to vms to compare the volumes we plan to delete against
    # to find possible ghost volumes
    vc_server_uuid_with_mounted_volume = dict()
    vc_server_name_with_mounted_volume = dict()
    has_volume_attachments = dict()
    vcenter_instances_without_mounts = dict()
    # iterate over the list of vms
    for k in data:
        # only work with results, which have an instance uuid defined and are openstack vms (i.e. have an annotation set)
        if k.get('config.instanceUuid') and openstack_re.match(k.get('config.annotation', 'no_annotation')) and not k.get('config.template'):
            # build a list of all openstack volumes in the vcenter to later compare it to the volumes in openstack
            vc_all_servers.append(k['config.instanceUuid'])
            # debug code
            # log.info("%s - %s", k.get('config.instanceUuid'), k.get('config.name'))
            # # check if this instance is a vcenter template
            # if k.get('config.template'):
            #     template[k['config.instanceUuid']] = k['config.template']
            # log.debug("==> uuid: %s - template: %s", str(k['config.instanceUuid']), str(k['config.template']))
            # get the config.hardware.device property out of the data dict and iterate over its elements
            # for j in k['config.hardware.device']:
            # this check seems to be required as in one bb i got a key error otherwise - looks like a vm without that property
            if k.get('config.hardware.device'):
                for j in k.get('config.hardware.device'):
                    # we are only interested in disks for ghost volumes ...
                    # old test was: if 2000 <= j.key < 3000:
                    if isinstance(j, vim.vm.device.VirtualDisk):
                        # we only care for vvols - in the past we checked starting with 2001 as 2000 usual was the eph
                        # storage, but it looks like eph can also be on another id and 2000 could be a vvol as well ...
                        if j.backing.fileName.lower().startswith('[vvol_') or j.backing.fileName.lower().startswith('[vmfs_'):
                            # map attached volume id to instance uuid - used later
                            vc_server_uuid_with_mounted_volume[j.backing.uuid] = k['config.instanceUuid']
                            # map attached volume id to instance name - used later for more detailed logging
                            vc_server_name_with_mounted_volume[j.backing.uuid] = k['config.name']
                            log.debug("==> mount - instance: %s - volume: %s", str(k['config.instanceUuid']), str(j.backing.uuid))
                            has_volume_attachments[k['config.instanceUuid']] = True
            else:
                log.warn("- PLEASE CHECK MANUALLY - instance without hardware - this should not happen!")
            if not has_volume_attachments.get(k['config.instanceUuid']):
                vcenter_instances_without_mounts[k['config.instanceUuid']] = k['config.name']

        # build a list of all volumes in the vcenter
        if k.get('config.instanceUuid') and not k.get('config.template'):
            if k.get('config.hardware.device'):
                for j in k.get('config.hardware.device'):
                    # we are only interested in disks ...
                    # old test was: if 2000 <= j.key < 3000:
                    if isinstance(j, vim.vm.device.VirtualDisk):
                        # we only care for vvols - in the past we checked starting with 2001 as 2000 usual was the eph
                        # storage, but it looks like eph can also be on another id and 2000 could be a vvol as well ...
                        if j.backing.fileName.lower().startswith('[vvol_') or j.backing.fileName.lower().startswith('[vmfs_'):
                            # build a list of all openstack volumes in the vcenter to later compare it to the volumes in openstack
                            # it looks like we have to put both the uuid of the shadow vm and the uuid of the backing
                            # storage onto the list, as otherwise we would miss out some volumes really existing in the vcenter
                            vc_all_volumes.append(j.backing.uuid)
                            vc_all_volumes.append(k.get('config.instanceUuid'))
                            # vc_all_volumes.append(k.get('config.instanceUuid'))
                            log.debug("==> shadow vm mount - instance: %s - volume / backing uuid: %s", str(k['config.instanceUuid']), str(j.backing.uuid))
            else:
                log.warn("- PLEASE CHECK MANUALLY - instance without hardware - this should not happen!")

    log.info("- going through the vcenter and comparing volume mounts to nova and cinder")
    # run through all attached volumes in the vcenter
    for i in vc_server_uuid_with_mounted_volume:
        # the cinder attachment check only makes sense for volumes, which actually exist in openstack
        if i in os_all_volumes:
            cinder_is_attached = False
            # for each volume attached in cinder, check if it is also attached according to the vcenter
            if os_servers_with_attached_volume.get(i):
                log.debug("==> volume: %s", str(i))
                for j in os_servers_with_attached_volume[i]:
                    log.debug("==> server: %s", str(j))
                    if j == vc_server_uuid_with_mounted_volume[i]:
                        cinder_is_attached = True
                    # check if we have attachments in cinder for the other servers the volume j is attached to, which are not attached in the vcenter
                    elif not os_volumes_attached_at_server.get(j):
                        log.warn("- PLEASE CHECK MANUALLY - instance: %s - in cinder attached volume uuid: %s - but not attached in vcenter", os_servers_with_attached_volume[i], j)
                if cinder_is_attached:
                    log.debug("==> instance: %s [%s] - volume: %s - cinder: yes", vc_server_uuid_with_mounted_volume[i], vc_server_name_with_mounted_volume[i], i)
                else:
                    # the cinder attachment check warning only makes sense for instances, which actually exist in openstack
                    # otherwise the cinder nanny will take care to clean them up
                    if vc_server_uuid_with_mounted_volume[i] in os_all_servers:
                        log.warn("- PLEASE CHECK MANUALLY - instance: %s [%s] - volume: %s - cinder: no", vc_server_uuid_with_mounted_volume[i], vc_server_name_with_mounted_volume[i], i)
                        gauge_value_volume_attachment_inconsistencies += 1
            else:
                # no attachment defined at all for this volume in cinder
                if vc_server_uuid_with_mounted_volume[i] in os_all_servers:
                    log.warn("- PLEASE CHECK MANUALLY - instance: %s [%s] - volume: %s - cinder: no attachments at all for this volume found", vc_server_uuid_with_mounted_volume[i], vc_server_name_with_mounted_volume[i], i)
                    gauge_value_volume_attachment_inconsistencies += 1
        else:
            # volume does not exist in openstack
            log.warn("- PLEASE CHECK MANUALLY - volume: %s attached to %s [%s] does not exist in openstack", i, vc_server_uuid_with_mounted_volume[i], vc_server_name_with_mounted_volume[i])
            gauge_value_volume_attachment_inconsistencies += 1
        # the nova attachment check only makes sense for instances, which actually exist in openstack
        if vc_server_uuid_with_mounted_volume[i] in os_all_servers:
            nova_is_attached = False
            # for each volume attached in nova, check if it is also attached according to the vcenter
            if os_volumes_attached_at_server.get(vc_server_uuid_with_mounted_volume[i]):
                log.debug("==> server: %s", str(vc_server_uuid_with_mounted_volume[i]))
                for j in os_volumes_attached_at_server[vc_server_uuid_with_mounted_volume[i]]:
                    log.debug("==> volume: %s", str(j))
                    if j == i:
                        nova_is_attached = True
                    # check if we have attachments in nova for the other volumes attached to server vc_server_uuid_with_mounted_volume[i], which are not attached in the vcenter
                    elif not os_servers_with_attached_volume.get(j):
                        log.warn("- PLEASE CHECK MANUALLY - instance: %s - in nova attached volume uuid: %s - but not attached in vcenter", vc_server_uuid_with_mounted_volume[i], j)
                if nova_is_attached:
                    log.debug("==> instance: %s [%s] - volume: %s - nova: yes", vc_server_uuid_with_mounted_volume[i], vc_server_name_with_mounted_volume[i], i)
                else:
                    # the nova attachment check warning only makes sense for volumes, which actually exist in openstack
                    # otherwise the nova nanny will take care to clean them up
                    if i in os_all_volumes:
                        log.warn("- PLEASE CHECK MANUALLY - instance: %s [%s] - volume: %s - nova: no", vc_server_uuid_with_mounted_volume[i], vc_server_name_with_mounted_volume[i], i)
                        gauge_value_volume_attachment_inconsistencies += 1
            else:
                # no attachment defined at all for this instance in nova
                if i in os_all_volumes:
                    log.warn("- PLEASE CHECK MANUALLY - instance: %s [%s] - volume: %s - nova: no attachments at all on this server found", vc_server_uuid_with_mounted_volume[i], vc_server_name_with_mounted_volume[i], i)
                    gauge_value_volume_attachment_inconsistencies += 1
        else:
            log.warn("- PLEASE CHECK MANUALLY - instance: %s [%s] with attached volume %s does not exist in openstack", vc_server_uuid_with_mounted_volume[i], vc_server_name_with_mounted_volume[i], i)
            gauge_value_volume_attachment_inconsistencies += 1

    log.info("- going through all vcenter instances without volume attachments")
    for i in vcenter_instances_without_mounts:
        if os_volumes_attached_at_server.get(i):
            # complain if a server without attachments in the vcenter has attachments according to nova
            for j in os_volumes_attached_at_server[i]:
                log.warn("- PLEASE CHECK MANUALLY - instance: %s [%s] - no volumes attached - nova: volume %s seems to be attached anyway", i, vcenter_instances_without_mounts[i], j)
                gauge_value_volume_attachment_inconsistencies += 1
        else:
            log.debug("==> instance: %s [%s] - no volumes attached - nova: no attachments - good", i, vcenter_instances_without_mounts[i])
        cinder_is_attached = False
        for j in os_servers_with_attached_volume:
            # complain if a volume without attachments in the vcenter has attachments according to cinder
            for k in os_servers_with_attached_volume[j]:
                if k == i:
                    cinder_is_attached = True
                    log.warn("- PLEASE CHECK MANUALLY - instance: %s [%s] - no volumes attached - cinder: volume %s seems to be attached anyway", i, vcenter_instances_without_mounts[i], j)
                    gauge_value_volume_attachment_inconsistencies += 1
        if not cinder_is_attached:
            log.debug("==> instance: %s [%s] - no volumes attached - cinder: no attachments - good", i, vcenter_instances_without_mounts[i])

    # log.info("- checking if all openstack servers exist in the vcenter")
    # for i in os_all_servers:
    #     # this is to convert the unicode entries to ascii for the compare to work - should maybe find a better way
    #     if i.encode('ascii') not in vc_all_servers:
    #         log.warn("- PLEASE CHECK MANUALLY - instance %s exists in openstack, but not in the vcenter", i)

    # log.info("- checking if all openstack volumes exist in the vcenter")
    # for i in os_all_volumes:
    #     # this is to convert the unicode entries to ascii for the compare to work - should maybe find a better way
    #     if i.encode('ascii') not in vc_all_volumes:
    #         log.warn("- PLEASE CHECK MANUALLY - volume %s exists in openstack, but not in the vcenter", i)

    gauge_volume_attachment_inconsistencies.set(float(gauge_value_volume_attachment_inconsistencies))


if __name__ == '__main__':
    while True:
        run_me()
