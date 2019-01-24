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
import sys

from pyVim.connect import SmartConnect, Disconnect
# TODO - not yet needed
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
# dry run option not doing anything harmful
@click.option('--dry-run', is_flag=True)
class ConsistencyCheck:
    def __init__(self, host, username, password, dry_run):

        self.host = host
        self.username = username
        self.password = password
        self.dry_run = dry_run

        self.os_volumes_attached_at_server = dict()
        self.os_servers_with_attached_volume = dict()
        self.os_all_servers = []
        self.os_all_volumes = []
        self.os_volume_status = []
        self.vc_all_servers = []
        self.vc_all_volumes = []

        # create a dict of volumes mounted to vms to compare the volumes we plan to delete against
        # to find possible ghost volumes
        self.vc_server_uuid_with_mounted_volume = dict()
        self.vc_server_name_with_mounted_volume = dict()
        self.vcenter_instances_without_mounts = dict()

        # some dummy initializations
        self.vc_service_instance = None
        self.vc_content = None
        self.vc_dc = None
        self.vc_view_ref = None
        self.vc_data = None
        self.os_conn = None
        self.vcenter_name = None
        self.volume_query = None

        self.run_me()

    # connect to vcenter
    def vc_connect(self):

        if hasattr(ssl, '_create_unverified_context'):
            context = ssl._create_unverified_context()

            try:
                self.vc_service_instance = SmartConnect(host=self.host,
                                            user=self.username,
                                            pwd=self.password,
                                            port=443,
                                            sslContext=context)
            except Exception as e:
                log.warn("problems connecting to vcenter: %s", str(e))
                sys.exit(1)

            else:
                atexit.register(Disconnect, self.vc_service_instance)

        else:
            raise Exception("maybe too old python version with ssl problems?")

    # disconnect from vcenter
    def vc_disconnect(self):

        Disconnect(self.vc_service_instance)

    # get vcenter viewref
    def vc_get_viewref(self):

        self.vc_content = self.vc_service_instance.content
        self.vc_dc = self.vc_content.rootFolder.childEntity[0]
        self.vcenter_name = self.vc_dc.name.lower()
        self.vc_view_ref = self.vc_content.viewManager.CreateContainerView(
            container=self.vc_content.rootFolder,
            type=[vim.VirtualMachine],
            recursive=True
        )

    # Shamelessly borrowed from:
    # https://github.com/dnaeon/py-vconnector/blob/master/src/vconnector/core.py
    def vc_collect_properties(self, obj_type, path_set=None, include_mors=False):
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

        collector = self.vc_service_instance.content.propertyCollector

        # Create object specification to define the starting point of
        # inventory navigation
        obj_spec = vmodl.query.PropertyCollector.ObjectSpec()
        obj_spec.obj = self.vc_view_ref
        obj_spec.skip = True

        # Create a traversal specification to identify the path for collection
        traversal_spec = vmodl.query.PropertyCollector.TraversalSpec()
        traversal_spec.name = 'traverseEntities'
        traversal_spec.path = 'view'
        traversal_spec.skip = False
        traversal_spec.type = self.vc_view_ref.__class__
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
            log.warn("problems retrieving properties from vcenter: %s - retrying in next loop run", str(e))
            return data

        for obj in props:
            properties = {}
            for prop in obj.propSet:
                properties[prop.name] = prop.val

            if include_mors:
                properties['obj'] = obj.obj

            data.append(properties)
        return data

    # get all servers and all volumes from the vcenter
    def vc_get_info(self):

        has_volume_attachments = dict()

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
        self.vc_data = self.vc_collect_properties(vim.VirtualMachine, vm_properties, True)

        # iterate over the list of vms
        for k in self.vc_data:
            # only work with results, which have an instance uuid defined and are openstack vms (i.e. have an annotation set)
            if k.get('config.instanceUuid') and openstack_re.match(k.get('config.annotation')) and not k.get('config.template'):
                # build a list of all openstack volumes in the vcenter to later compare it to the volumes in openstack
                self.vc_all_servers.append(k['config.instanceUuid'])
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
                        if 2000 <= j.key < 3000:
                            # we only care for vvols - in the past we checked starting with 2001 as 2000 usual was the eph
                            # storage, but it looks like eph can also be on another id and 2000 could be a vvol as well ...
                            if j.backing.fileName.lower().startswith('[vvol_'):
                                # map attached volume id to instance uuid - used later
                                self.vc_server_uuid_with_mounted_volume[j.backing.uuid] = k['config.instanceUuid']
                                # map attached volume id to instance name - used later for more detailed logging
                                self.vc_server_name_with_mounted_volume[j.backing.uuid] = k['config.name']
                                log.debug("==> mount - instance: %s - volume: %s", str(k['config.instanceUuid']), str(j.backing.uuid))
                                has_volume_attachments[k['config.instanceUuid']] = True
                else:
                    log.warn("- PLEASE CHECK MANUALLY - instance without hardware - this should not happen!")
                if not has_volume_attachments.get(k['config.instanceUuid']):
                    self.vcenter_instances_without_mounts[k['config.instanceUuid']] = k['config.name']

            # build a list of all volumes in the vcenter
            if k.get('config.instanceUuid') and not k.get('config.template'):
                if k.get('config.hardware.device'):
                    for j in k.get('config.hardware.device'):
                        # we are only interested in disks ...
                        # TODO: maybe? if isinstance(k.get('config.hardware.device'), vim.vm.device.VirtualDisk):
                        if 2000 <= j.key < 3000:
                            # we only care for vvols - in the past we checked starting with 2001 as 2000 usual was the eph
                            # storage, but it looks like eph can also be on another id and 2000 could be a vvol as well ...
                            if j.backing.fileName.lower().startswith('[vvol_'):
                                # build a list of all openstack volumes in the vcenter to later compare it to the volumes in openstack
                                # it looks like we have to put both the uuid of the shadow vm and the uuid of the backing
                                # storage onto the list, as otherwise we would miss out some volumes really existing in the vcenter
                                self.vc_all_volumes.append(j.backing.uuid)
                                self.vc_all_volumes.append(k.get('config.instanceUuid'))
                                # vc_all_volumes.append(k.get('config.instanceUuid'))
                                log.debug("==> shadow vm mount - instance: %s - volume / backing uuid: %s", str(k['config.instanceUuid']), str(j.backing.uuid))
                else:
                    log.warn("- PLEASE CHECK MANUALLY - instance without hardware - this should not happen!")

    # openstack connection
    def os_connect(self):

        try:
            self.os_conn = connection.Connection(auth_url=os.getenv('OS_AUTH_URL'),
                                 project_name=os.getenv('OS_PROJECT_NAME'),
                                 project_domain_name=os.getenv('OS_PROJECT_DOMAIN_NAME'),
                                 username=os.getenv('OS_USERNAME'),
                                 user_domain_name=os.getenv('OS_USER_DOMAIN_NAME'),
                                 password=os.getenv('OS_PASSWORD'))

        except Exception as e:
                log.warn("problems connecting to openstack: %s", str(e))

    # disconnect from openstack
    def os_disconnect(self):
        self.os_conn.close()

    # get all servers and all volumes from openstack
    def os_get_info(self):

        try:
            service = "nova"
            for server in self.os_conn.compute.servers(details=True, all_projects=1):
                # we only care about servers from the vcenter this nanny is taking care of
                if server.availability_zone.lower() == self.vcenter_name:
                    self.os_all_servers.append(server.id)
                    if server.attached_volumes:
                        for attachment in server.attached_volumes:
                            if self.os_volumes_attached_at_server.get(server.id):
                                self.os_volumes_attached_at_server[server.id].append(attachment['id'].encode('ascii'))
                            else:
                                self.os_volumes_attached_at_server[server.id] = [attachment['id'].encode('ascii')]
            service = "cinder"
            for volume in self.os_conn.block_store.volumes(details=True, all_projects=1):
                # we only care about volumes from the vcenter this nanny is taking care of
                if volume.availability_zone.lower() == self.vcenter_name:
                    self.os_all_volumes.append(volume.id)
                    self.os_volume_status = volume.status
                    if volume.attachments:
                        for attachment in volume.attachments:
                            if self.os_servers_with_attached_volume.get(volume.id):
                                self.os_servers_with_attached_volume[volume.id].append(attachment['server_id'].encode('ascii'))
                            else:
                                self.os_servers_with_attached_volume[volume.id] = [attachment['server_id'].encode('ascii')]

        except exceptions.HttpException as e:
            log.warn(
                "problems retrieving information from openstack %s: %s", service, str(e))
            sys.exit(1)
        except exceptions.SDKException as e:
            log.warn(
                "problems retrieving information from openstack %s: %s", service, str(e))
            sys.exit(1)

    def volume_uuid_query_loop(self):
        while True:
            try:
                self.volume_query=str(raw_input('please enter a volume uuid (ctrl-c to exit): '))
            except KeyboardInterrupt:
                print ""
                log.info("got keyboard interrupt ... good bye")
                break
            except Exception as e:
                log.error("there was a problem with your input: %s",  str(e))
                sys.exit(1)
            self.print_volume_information()

    def print_volume_information(self):
        log.info("volume uuid: %s", self.volume_query)
        if self.volume_query in self.os_all_volumes:
            log.info("- this volume exists in cinder: Yes")
            log.info("- volume status in cinder: %s", self.os_volume_status)
        else:
            log.info("- this volume exists in cinder: No")
        if self.os_servers_with_attached_volume.get(self.volume_query):
            for i in self.os_servers_with_attached_volume[self.volume_query]:
                log.info("os server with this volume attached (cinder): %s", i)
                if i in self.os_all_servers:
                    log.info("- this instance exists in nova: Yes")
                else:
                    log.info("- this instance exists in nova: No")
        else:
            log.info("os server with this volume attached (cinder): None")
        is_attached_in_nova = False
        for i in self.os_all_servers:
            if self.os_volumes_attached_at_server.get(i):
                if self.volume_query in self.os_volumes_attached_at_server[i]:
                    log.info("os server with this volume attached (nova): %s", i)
                    is_attached_in_nova = True
                    if i in self.os_all_servers:
                        log.info("- this instance exists in nova: Yes")
                    else:
                        log.info("- this instance exists in nova: No")
        if is_attached_in_nova is False:
                log.info("os server with this volume attached (nova): None")
        log.info("vc server with this volume attached (uuid/name): %s / %s", self.vc_server_uuid_with_mounted_volume.get(self.volume_query), self.vc_server_name_with_mounted_volume.get(self.volume_query))

    def run_me(self):
        log.info("connecting to vcenter")
        self.vc_connect()
        log.info("getting information from vcenter")
        self.vc_get_viewref()
        self.vc_get_info()
        log.info("connecting to openstack")
        self.os_connect()
        log.info("getting information from openstack (this may take a moment)")
        self.os_get_info()
        self.volume_uuid_query_loop()
        self.vc_disconnect()
        self.os_disconnect()

if __name__ == '__main__':
    c = ConsistencyCheck()
