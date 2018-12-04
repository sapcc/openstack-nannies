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
import os
import re
import ssl
import time

from pyVim.connect import SmartConnect, Disconnect
# from pyVim.task import WaitForTask, WaitForTasks
from pyVmomi import vim, vmodl

from openstack import connection, exceptions

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')

# compile a regex for trying to filter out openstack generated vms
#  they all have the "name:" field set
openstack_re = re.compile("^name")

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

def run_me(host, username, password, interval, iterations, dry_run):

    while True:

        log.info("INFO: starting new loop run")

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

            else:
                atexit.register(Disconnect, service_instance)

                content = service_instance.content
                dc = content.rootFolder.childEntity[0]

                # iterate through all vms and get the config.hardware.device properties (and some other)
                # get vm containerview
                # TODO: destroy the view again - most probably not required, as we close the connection at the end of each loop
                view_ref = content.viewManager.CreateContainerView(
                    container=content.rootFolder,
                    type=[vim.VirtualMachine],
                    recursive=True
                )

                # do the work
                sync_volume_attachments(host, username, password, interval, iterations, dry_run, service_instance, content, dc, view_ref)

                # disconnect from vcenter
                Disconnect(service_instance)

        else:
            raise Exception("maybe too old python version with ssl problems?")

        # wait the interval time
        log.info("INFO: waiting %s minutes before starting the next loop run", str(interval))
        time.sleep(60 * int(interval))

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

# main volume attachment sync function
def sync_volume_attachments(host, username, password, interval, iterations, dry_run, service_instance, content, dc, view_ref):

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
    vcenter_mounted_uuid = dict()
    vcenter_mounted_name = dict()
    has_volume_attachments = dict()
    vcenter_instances_without_mounts = dict()
    # iterate over the list of vms
    for k in data:
        # only work with results, which have an instance uuid defined and are openstack vms (i.e. have an annotation set)
        if k.get('config.instanceUuid') and openstack_re.match(k.get('config.annotation')) and not k.get('config.template'):
            # debug code
            # log.info("%s - %s", k.get('config.instanceUuid'), k.get('config.name'))
            # # check if this instance is a vcenter template
            # if k.get('config.template'):
            #     template[k['config.instanceUuid']] = k['config.template']
            # log.debug("uuid: %s - template: %s", str(k['config.instanceUuid']), str(k['config.template']))
            # get the config.hardware.device property out of the data dict and iterate over its elements
            # for j in k['config.hardware.device']:
            # this check seems to be required as in one bb i got a key error otherwise - looks like a vm without that property
            if k.get('config.hardware.device'):
                for j in k.get('config.hardware.device'):
                    # we are only interested in disks for ghost volumes ...
                    # TODO: maybe? if isinstance(k.get('config.hardware.device'), vim.vm.device.VirtualDisk):
                    if 2001 <= j.key < 3000:
                        vcenter_mounted_uuid[j.backing.uuid] = k['config.instanceUuid']
                        vcenter_mounted_name[j.backing.uuid] = k['config.name']
                        log.debug("==> mount - instance: %s - volume: %s", str(k['config.instanceUuid']), str(j.backing.uuid))
                        has_volume_attachments[k['config.instanceUuid']] = True
            else:
                log.warn("WARNIHG: instance without hardware - this should not happen!")
            if not has_volume_attachments.get(k['config.instanceUuid']):
                vcenter_instances_without_mounts[k['config.instanceUuid']] = k['config.name']

    log.info("- going through the vcenter and comparing volume mounts to nova and cinder")
    for i in vcenter_mounted_uuid:
        if i in all_volumes:
            cinder_is_attached = False
            if volumes_attached_at.get(i):
                for j in volumes_attached_at[i]:
                    if j == vcenter_mounted_uuid[i]:
                        cinder_is_attached = True
                if cinder_is_attached:
                    log.debug("- instance: %s [%s] - volume: %s - cinder: yes", vcenter_mounted_uuid[i], vcenter_mounted_name[i], i)
                else:
                    if vcenter_mounted_uuid[i] in all_servers:
                        log.warn("- instance: %s [%s] - volume: %s - cinder: no", vcenter_mounted_uuid[i], vcenter_mounted_name[i], i)
            else:
                if vcenter_mounted_uuid[i] in all_servers:
                    log.warn("- instance: %s [%s] - volume: %s - cinder: no attachments at all for this volume found", vcenter_mounted_uuid[i], vcenter_mounted_name[i], i)
        else:
            log.warn("- volume: %s attached to %s [%s] does not exist in openstack", i, vcenter_mounted_uuid[i], vcenter_mounted_name[i])
        if vcenter_mounted_uuid[i] in all_servers:
            nova_is_attached = False
            if servers_attached_volumes.get(vcenter_mounted_uuid[i]):
                for j in servers_attached_volumes[vcenter_mounted_uuid[i]]:
                    if j == i:
                        nova_is_attached = True
                if nova_is_attached:
                    log.debug("- instance: %s [%s] - volume: %s - nova: yes", vcenter_mounted_uuid[i], vcenter_mounted_name[i], i)
                else:
                    if i in all_volumes:
                        log.warn("- instance: %s [%s] - volume: %s - nova: no", vcenter_mounted_uuid[i], vcenter_mounted_name[i], i)
            else:
                if i in all_volumes:
                    log.warn("- instance: %s [%s] - volume: %s - nova: no attachments at all on this server found", vcenter_mounted_uuid[i], vcenter_mounted_name[i], i)
        else:
            log.warn("- instance: %s [%s] with attached volume %s does not exist in openstack", vcenter_mounted_uuid[i], vcenter_mounted_name[i], i)

    log.info("- going through all vcenter instances without volume attachments")
    for i in vcenter_instances_without_mounts:
        if servers_attached_volumes.get(i):
            for j in servers_attached_volumes[i]:
                log.warn("- instance: %s [%s] - no volumes attached - nova: volume %s seems to be attached anyway", i, vcenter_instances_without_mounts[i], j)
        else:
            log.debug("- instance: %s [%s] - no volumes attached - nova: no attachments - good", i, vcenter_instances_without_mounts[i])
        cinder_is_attached = False
        for j in volumes_attached_at:
            for k in volumes_attached_at[j]:
                if k == i:
                    cinder_is_attached = True
                    log.warn("- instance: %s [%s] - no volumes attached - cinder: volume %s seems to be attached anyway", i, vcenter_instances_without_mounts[i], j)
        if not cinder_is_attached:
            log.debug("- instance: %s [%s] - no volumes attached - cinder: no attachments - good", i, vcenter_instances_without_mounts[i])

    # log.info("going through all volumes and checking their attachments:")
    # for i in volumes_attached_at:
    #     for j in volumes_attached_at[i]:
    #         is_attached = False
    #         if servers_attached_volumes.get(j):
    #             for m in servers_attached_volumes[j]:
    #                 if m == i:
    #                     is_attached = True
    #         if is_attached:
    #             log.debug("good: volume %s is attached to server %s", i, j)
    #         else:
    #             if j in all_servers:
    #                 log.warn("bad: volume %s is not attached to server %s", i, j)
    #             else:
    #                 log.warn("bad: volume %s is attached to non existing server %s", i, j)

    # log.info("going through all servers and checking attached volumes:")
    # for k in servers_attached_volumes:
    #     for l in servers_attached_volumes[k]:
    #         is_attached = False
    #         if volumes_attached_at.get(l):
    #             for n in volumes_attached_at[l]:
    #                 if n == k:
    #                     is_attached = True
    #         if is_attached:
    #             log.debug("good: volume %s is attached to server %s", l, k)
    #         else:
    #             if l in all_volumes:
    #                 log.warn("bad: volume %s is not attached to server %s", l, k)
    #             else:
    #                 log.warn("bad: non existing volume %s is attached to server %s", l, k)

if __name__ == '__main__':
    while True:
        run_me()
