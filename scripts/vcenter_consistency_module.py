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
from pyVmomi import vim, vmodl

from openstack import connection, exceptions

# sqlalchemy stuff
from sqlalchemy import and_
from sqlalchemy import func
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import join
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import false
from sqlalchemy.ext.declarative import declarative_base

# prometheus export functionality
from prometheus_client import start_http_server, Gauge

log = logging.getLogger('vcenter_consistency_module')

# compile a regex for trying to filter out openstack generated vms
#  they all have the "name:" field set
openstack_re = re.compile("^name")

class ConsistencyCheck:
    def __init__(self, vchost, vcusername, vcpassword, cinderpassword, novapassword, region, dry_run, prometheus_port):

        self.vchost = vchost
        self.vcusername = vcusername
        self.vcpassword = vcpassword
        self.cinderpassword = cinderpassword
        self.novapassword = novapassword
        self.region = region
        self.dry_run = dry_run
        self.prometheus_port = prometheus_port

        self.nova_os_all_servers = []
        self.cinder_os_all_volumes = []
        self.vc_all_servers = []
        self.vc_all_volumes = []

        # initialize some dicts - those have a volume uuid as key
        self.nova_os_servers_with_attached_volume = dict()
        self.cinder_os_servers_with_attached_volume = dict()
        self.vc_server_uuid_with_mounted_volume = dict()
        self.vc_server_name_with_mounted_volume = dict()
        self.cinder_volume_attaching_for_too_long = dict()
        self.cinder_volume_detaching_for_too_long = dict()
        self.cinder_volume_is_in_state_reserved = dict()
        self.cinder_volume_available_with_attachments = dict()
        self.cinder_os_volume_status = dict()
        self.cinder_os_volume_project_id = dict()

        # this one has the instance uuid as key
        self.nova_os_volumes_attached_at_server = dict()
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
        # some db related stuff
        self.cinder_engine = None
        self.cinder_connection = None
        self.cinder_thisSession = None
        self.cinder_metadata = None
        self.cinder_Base = None
        self.nova_engine = None
        self.nova_connection = None
        self.nova_thisSession = None
        self.nova_metadata = None
        self.nova_Base = None

        # flag if the prometheus exporter is enabled
        self.prometheus_exporter_enabled = True

        self.gauge_cinder_volume_attaching_for_too_long = Gauge('vcenter_nanny_consistency_cinder_volume_attaching_for_too_long',
                                                  'how many volumes are in the state attaching for too long', ["volume_uuid", 'project_id'])
        self.gauge_cinder_volume_detaching_for_too_long = Gauge('vcenter_nanny_consistency_cinder_volume_detaching_for_too_long',
                                                  'how many volumes are in the state detaching for too long', ["volume_uuid", 'project_id'])
        self.gauge_cinder_volume_is_in_state_reserved = Gauge('vcenter_nanny_consistency_cinder_volume_is_in_state_reserved',
                                                  'how many volumes are in the state reserved for too long', ["volume_uuid", 'project_id'])
        self.gauge_cinder_volume_available_with_attachments = Gauge('vcenter_nanny_consistency_cinder_volume_available_with_attachments',
                                                  'how many volumes are available with attachments for too long', ["volume_uuid", 'project_id'])

        # actual values we want to send to the prometheus exporter, it is a list of the value and the project id
        self.gauge_value_cinder_volume_attaching_for_too_long = dict()
        self.gauge_value_cinder_volume_detaching_for_too_long = dict()
        self.gauge_value_cinder_volume_is_in_state_reserved = dict()
        self.gauge_value_cinder_volume_available_with_attachments = dict()
        # initialize a value without project_id, so that the mtric always exists, even if there is no problem
        self.gauge_value_cinder_volume_attaching_for_too_long['NOVOLUME_DUMMY'] = 0
        self.gauge_value_cinder_volume_detaching_for_too_long['NOVOLUME_DUMMY'] = 0
        self.gauge_value_cinder_volume_is_in_state_reserved['NOVOLUME_DUMMY'] = 0
        self.gauge_value_cinder_volume_available_with_attachments['NOVOLUME_DUMMY'] = 0

    # start prometheus exporter if needed
    def start_prometheus_exporter(self):

        # if the port is not set, we do not start the prometheus exporter - for instance in the cmdline tool
        if not self.prometheus_port:
            self.prometheus_exporter_enabled = False
            return
        else:
            # start http server for exported data
            try:
                start_http_server(self.prometheus_port)
            except Exception as e:
                logging.error(" - ERROR - failed to start prometheus exporter http server: " + str(e))

    # connect to vcenter
    def vc_connect(self):

        if hasattr(ssl, '_create_unverified_context'):
            context = ssl._create_unverified_context()

            try:
                self.vc_service_instance = SmartConnect(host=self.vchost,
                                            user=self.vcusername,
                                            pwd=self.vcpassword,
                                            port=443,
                                            sslContext=context)
            except Exception as e:
                log.warn("problems connecting to vcenter: %s", str(e))

            else:
                atexit.register(Disconnect, self.vc_service_instance)

        else:
            raise Exception("maybe too old python version with ssl problems?")

    # check if the vcenter connection is ok, so that we can use it later to exit (cmdline tool) or return (in a loop)
    def vc_connection_ok(self):

        if self.vc_service_instance:
            return True
        else:
            return False

    # disconnect from the vcenter
    def vc_disconnect(self):

        Disconnect(self.vc_service_instance)

    # get vcenter viewref
    def vc_get_viewref(self):

        try:
            self.vc_content = self.vc_service_instance.content
            self.vc_dc = self.vc_content.rootFolder.childEntity[0]
            self.vcenter_name = self.vc_dc.name.lower()
            self.vc_view_ref = self.vc_content.viewManager.CreateContainerView(
                container=self.vc_content.rootFolder,
                type=[vim.VirtualMachine],
                recursive=True
            )
        except Exception as e:
            log.warn("problems getting viewref from the vcenter: %s", str(e))
            return False

        return True


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
            log.warn("problems retrieving properties from the vcenter: %s - retrying in next loop run", str(e))
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

        # TODO better exception handling

        # clear the old lists
        self.vc_all_servers *= 0
        self.vc_all_volumes *= 0

        # clean all old dicts
        self.vc_server_uuid_with_mounted_volume.clear()
        self.vc_server_name_with_mounted_volume.clear()
        self.vcenter_instances_without_mounts.clear()

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

        # in case we do not get the properties
        if not self.vc_data:
            return False

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

        return True

    # connect to the cinder db
    def cinder_db_connect(self):

        # TODO exception handling

        # db_url = 'postgresql+psycopg2://cinder:' + self.cinderpassword + '@cinder-postgresql.monsoon3.svc.kubernetes.' + self.region + '.cloud.sap:5432/cinder?connect_timeout=10&keepalives_idle=5&keepalives_interval=5&keepalives_count=10'
        # for debugging
        db_url = 'postgresql+psycopg2://cinder:' + self.cinderpassword + '@localhost:5432/cinder?connect_timeout=10&keepalives_idle=5&keepalives_interval=5&keepalives_count=10'


        self.cinder_engine = create_engine(db_url)
        self.cinder_connection = self.cinder_engine.connect()
        Session = sessionmaker(bind=self.cinder_engine)
        self.cinder_thisSession = Session()
        self.cinder_metadata = MetaData()
        self.cinder_metadata.bind = self.cinder_engine
        self.cinder_Base = declarative_base()

    # check if the cinder db connection is ok, so that we can use it later to exit (cmdline tool) or return (in a loop)
    def cinder_db_connection_ok(self):

        if self.cinder_thisSession:
            return True
        else:
            return False

    # disconnect from the cinder db
    def cinder_db_disconnect(self):
        self.cinder_thisSession.close()
        self.cinder_connection.close()

    # connect to the nova db
    def nova_db_connect(self):

        # TODO exception handling

        # db_url = 'postgresql+psycopg2://nova:' + self.novapassword + '@nova-postgresql.monsoon3.svc.kubernetes.' + self.region + '.cloud.sap:5432/nova?connect_timeout=10&keepalives_idle=5&keepalives_interval=5&keepalives_count=10'
        # for debugging
        db_url = 'postgresql+psycopg2://nova:' + self.novapassword + '@localhost:15432/nova?connect_timeout=10&keepalives_idle=5&keepalives_interval=5&keepalives_count=10'


        self.nova_engine = create_engine(db_url)
        self.nova_connection = self.nova_engine.connect()
        Session = sessionmaker(bind=self.nova_engine)
        self.nova_thisSession = Session()
        self.nova_metadata = MetaData()
        self.nova_metadata.bind = self.nova_engine
        self.nova_Base = declarative_base()

    # check if the nova db connection is ok, so that we can use it later to exit (cmdline tool) or return (in a loop)
    def nova_db_connection_ok(self):

        if self.nova_thisSession:
            return True
        else:
            return False

    # disconnect from the nova db
    def nova_db_disconnect(self):
        self.nova_thisSession.close()
        self.nova_connection.close()

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

    # check if the openstack connection is ok, so that we can use it later to exit (cmdline tool) or return (in a loop)
    def os_connection_ok(self):

        if self.os_conn:
            return True
        else:
            return False

    # disconnect from openstack
    def os_disconnect(self):
        self.os_conn.close()

    # get all servers and all volumes from openstack
    def os_get_info(self):

        # clear old lists
        self.nova_os_all_servers *= 0
        self.cinder_os_all_volumes *= 0

        # clean old dicts
        self.nova_os_servers_with_attached_volume.clear()
        self.nova_os_volumes_attached_at_server.clear()
        self.cinder_os_servers_with_attached_volume.clear()
        self.cinder_os_volume_status.clear()
        self.cinder_os_volume_project_id.clear()

        try:
            service = "nova"
            for server in self.os_conn.compute.servers(details=True, all_projects=1):
                # we only care about servers from the vcenter this nanny is taking care of
                if server.availability_zone.lower() == self.vcenter_name:
                    self.nova_os_all_servers.append(server.id)
                    if server.attached_volumes:
                        for attachment in server.attached_volumes:
                            if self.nova_os_volumes_attached_at_server.get(server.id):
                                self.nova_os_volumes_attached_at_server[server.id].append(attachment['id'].encode('ascii'))
                            else:
                                self.nova_os_volumes_attached_at_server[server.id] = [attachment['id'].encode('ascii')]
                        self.nova_os_servers_with_attached_volume[attachment['id']] = server.id
            service = "cinder"
            for volume in self.os_conn.block_store.volumes(details=True, all_projects=1):
                # we only care about volumes from the vcenter this nanny is taking care of
                if volume.availability_zone.lower() == self.vcenter_name:
                    self.cinder_os_all_volumes.append(volume.id)
                    self.cinder_os_volume_status[volume.id] = volume.status
                    self.cinder_os_volume_project_id[volume.id] = volume.project_id.encode('ascii')
                    if volume.attachments:
                        for attachment in volume.attachments:
                            if self.cinder_os_servers_with_attached_volume.get(volume.id):
                                self.cinder_os_servers_with_attached_volume[volume.id].append(attachment['server_id'].encode('ascii'))
                            else:
                                self.cinder_os_servers_with_attached_volume[volume.id] = [attachment['server_id'].encode('ascii')]

        except exceptions.HttpException as e:
            log.warn(
                "problems retrieving information from openstack %s: %s", service, str(e))
            return False
        except exceptions.SDKException as e:
            log.warn(
                "problems retrieving information from openstack %s: %s", service, str(e))
            return False

        return True

    def volume_uuid_query_loop(self):
        while True:
            try:
                self.volume_query=str(raw_input('please enter a volume uuid (ctrl-c to exit): '))
            except KeyboardInterrupt:
                print ""
                log.info("got keyboard interrupt ... good bye")
                return
            except Exception as e:
                log.error("there was a problem with your input: %s",  str(e))
                sys.exit(1)
            self.print_volume_information()

    def print_volume_information(self):
        log.info("volume uuid: %s", self.volume_query)
        if self.volume_query in self.cinder_os_all_volumes:
            log.info("- this volume exists in cinder (for this az): Yes")
            log.info("- project id: %s", self.cinder_os_volume_project_id.get(self.volume_query))
            log.info("- volume status in cinder: %s", self.cinder_os_volume_status.get(self.volume_query))
        else:
            log.info("- this volume exists in cinder (for this az): No")
        if self.cinder_os_servers_with_attached_volume.get(self.volume_query):
            for i in self.cinder_os_servers_with_attached_volume[self.volume_query]:
                log.info("os server with this volume attached (cinder): %s", i)
                if i in self.nova_os_all_servers:
                    log.info("- this instance exists in nova: Yes")
                else:
                    log.info("- this instance exists in nova: No")
        else:
            log.info("os server with this volume attached (cinder): None")
        is_attached_in_nova = False
        for i in self.nova_os_all_servers:
            if self.nova_os_volumes_attached_at_server.get(i):
                if self.volume_query in self.nova_os_volumes_attached_at_server[i]:
                    log.info("os server with this volume attached (nova): %s", i)
                    is_attached_in_nova = True
                    if i in self.nova_os_all_servers:
                        log.info("- this instance exists in nova: Yes")
                    else:
                        log.info("- this instance exists in nova: No")
        if is_attached_in_nova is False:
                log.info("os server with this volume attached (nova): None")
        log.info("vc server with this volume attached (uuid/name): %s / %s", self.vc_server_uuid_with_mounted_volume.get(self.volume_query), self.vc_server_name_with_mounted_volume.get(self.volume_query))

    def reset_gauge_values(self):
        # this is ugly, but for now should at least give us reliable prometheus metrics, i.e.
        # metrics, which also go back to 0 in case a problem or volume disappears ...
        for i in self.gauge_value_cinder_volume_attaching_for_too_long:
            self.gauge_value_cinder_volume_attaching_for_too_long[i] = 0
        for i in self.gauge_value_cinder_volume_detaching_for_too_long:
            self.gauge_value_cinder_volume_detaching_for_too_long[i] = 0
        for i in self.gauge_value_cinder_volume_is_in_state_reserved:
            self.gauge_value_cinder_volume_is_in_state_reserved[i] = 0
        for i in self.gauge_value_cinder_volume_available_with_attachments:
            self.gauge_value_cinder_volume_available_with_attachments[i] = 0

    def discover_problems(self, iterations):
        self.discover_cinder_volume_attaching_for_too_long(iterations)
        self.discover_cinder_volume_detaching_for_too_long(iterations)
        self.discover_cinder_volume_is_in_reserved_state(iterations)
        self.discover_cinder_volume_available_with_attachments(iterations)

    # in the below discover functions we increase a counter for each occurence of the problem per volume uuid
    # if the counter reaches 'iterations' then the problem is persisting for too long and we log a warning
    # as soon as the problem is gone for a volume uuid we reset the counter for it to 0 again, as everything
    # seems to be ok again
    def discover_cinder_volume_attaching_for_too_long(self, iterations):
        for volume_uuid in self.cinder_os_all_volumes:
            if self.cinder_os_volume_status.get(volume_uuid) == 'attaching':
                if not self.cinder_volume_attaching_for_too_long.get(volume_uuid):
                    self.cinder_volume_attaching_for_too_long[volume_uuid] = 1
                elif self.cinder_volume_attaching_for_too_long.get(volume_uuid) < iterations:
                    self.cinder_volume_attaching_for_too_long[volume_uuid] += 1
                else:
                    if not self.gauge_value_cinder_volume_attaching_for_too_long.get(volume_uuid):
                        self.gauge_value_cinder_volume_attaching_for_too_long[volume_uuid] = 1
                    else:
                        self.gauge_value_cinder_volume_attaching_for_too_long[volume_uuid] += 1
                    log.warn("- PLEASE CHECK MANUALLY - volume %s in project %s is in state 'attaching' for too long", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid))
            else:
                self.cinder_volume_attaching_for_too_long[volume_uuid] = 0

    def discover_cinder_volume_detaching_for_too_long(self, iterations):
        for volume_uuid in self.cinder_os_all_volumes:
            if self.cinder_os_volume_status.get(volume_uuid) == 'detaching':
                if not self.cinder_volume_detaching_for_too_long.get(volume_uuid):
                    self.cinder_volume_detaching_for_too_long[volume_uuid] = 1
                elif self.cinder_volume_detaching_for_too_long.get(volume_uuid) < iterations:
                    self.cinder_volume_detaching_for_too_long[volume_uuid] += 1
                else:
                    if not self.gauge_value_cinder_volume_detaching_for_too_long.get(volume_uuid):
                        self.gauge_value_cinder_volume_detaching_for_too_long[volume_uuid] = 1
                    else:
                        self.gauge_value_cinder_volume_detaching_for_too_long[volume_uuid] += 1
                    log.warn("- PLEASE CHECK MANUALLY - volume %s in project %s is in state 'detaching' for too long", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid))
            else:
                self.cinder_volume_detaching_for_too_long[volume_uuid] = 0

    def discover_cinder_volume_is_in_reserved_state(self, iterations):
        for volume_uuid in self.cinder_os_all_volumes:
            if self.cinder_os_volume_status.get(volume_uuid) == 'reserved':
                if not self.cinder_volume_is_in_state_reserved.get(volume_uuid):
                    self.cinder_volume_is_in_state_reserved[volume_uuid] = 1
                elif self.cinder_volume_is_in_state_reserved.get(volume_uuid) < iterations:
                    self.cinder_volume_is_in_state_reserved[volume_uuid] += 1
                else:
                    if not self.gauge_value_cinder_volume_is_in_state_reserved.get(volume_uuid):
                        self.gauge_value_cinder_volume_is_in_state_reserved[volume_uuid] = 1
                    else:
                        self.gauge_value_cinder_volume_is_in_state_reserved[volume_uuid] += 1
                    log.warn("- PLEASE CHECK MANUALLY - volume %s in project %s is in state 'reserved' for too long", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid))
            else:
                self.cinder_volume_is_in_state_reserved[volume_uuid] = 0

    def discover_cinder_volume_available_with_attachments(self, iterations):
        for volume_uuid in self.cinder_os_all_volumes:
            if self.cinder_os_volume_status.get(volume_uuid) == 'available':
                if self.cinder_os_servers_with_attached_volume.get(volume_uuid):
                    if not self.cinder_volume_available_with_attachments.get(volume_uuid):
                        self.cinder_volume_available_with_attachments[volume_uuid] = 1
                    elif self.cinder_volume_available_with_attachments.get(volume_uuid) < iterations:
                        self.cinder_volume_available_with_attachments[volume_uuid] += 1
                    else:
                        if not self.gauge_value_cinder_volume_available_with_attachments.get(volume_uuid):
                            self.gauge_value_cinder_volume_available_with_attachments[volume_uuid] = 1
                        else:
                            self.gauge_value_cinder_volume_available_with_attachments[volume_uuid] += 1
                        log.warn("- PLEASE CHECK MANUALLY - volume %s in project %s is in state 'available' with attachments for too long", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid))
                    continue
                if self.nova_os_servers_with_attached_volume.get(volume_uuid):
                    if not self.cinder_volume_available_with_attachments.get(volume_uuid):
                        self.cinder_volume_available_with_attachments[volume_uuid] = 1
                    elif self.cinder_volume_available_with_attachments.get(volume_uuid) < iterations:
                        self.cinder_volume_available_with_attachments[volume_uuid] += 1
                    else:
                        if not self.gauge_value_cinder_volume_available_with_attachments.get(volume_uuid):
                            self.gauge_value_cinder_volume_available_with_attachments[volume_uuid] = 1
                        else:
                            self.gauge_value_cinder_volume_available_with_attachments[volume_uuid] += 1
                        log.warn("- PLEASE CHECK MANUALLY - volume %s in project %s is in state 'available' with attachments for too long", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid))
                    continue
                if self.vc_server_name_with_mounted_volume.get(volume_uuid):
                    if not self.cinder_volume_available_with_attachments.get(volume_uuid):
                        self.cinder_volume_available_with_attachments[volume_uuid] = 1
                    elif self.cinder_volume_available_with_attachments.get(volume_uuid) < iterations:
                        self.cinder_volume_available_with_attachments[volume_uuid] += 1
                    else:
                        if not self.gauge_value_cinder_volume_available_with_attachments.get(volume_uuid):
                            self.gauge_value_cinder_volume_available_with_attachments[volume_uuid] = 1
                        else:
                            self.gauge_value_cinder_volume_available_with_attachments[volume_uuid] += 1
                        log.warn("- PLEASE CHECK MANUALLY - volume %s in project %s is in state 'available' with attachments for too long", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid))
                    continue
                self.cinder_volume_available_with_attachments[volume_uuid] = 0
            else:
                self.cinder_volume_available_with_attachments[volume_uuid] = 0

    def send_gauge_values(self):
        for i in self.gauge_value_cinder_volume_attaching_for_too_long:
            self.gauge_cinder_volume_attaching_for_too_long.labels(i, self.cinder_os_volume_project_id.get(i)).set(self.gauge_value_cinder_volume_attaching_for_too_long[i])
        for i in self.gauge_value_cinder_volume_detaching_for_too_long:
            self.gauge_cinder_volume_detaching_for_too_long.labels(i, self.cinder_os_volume_project_id.get(i)).set(self.gauge_value_cinder_volume_detaching_for_too_long[i])
        for i in self.gauge_value_cinder_volume_is_in_state_reserved:
            self.gauge_cinder_volume_is_in_state_reserved.labels(i, self.cinder_os_volume_project_id.get(i)).set(self.gauge_value_cinder_volume_is_in_state_reserved[i])
        for i in self.gauge_value_cinder_volume_available_with_attachments:
            self.gauge_cinder_volume_available_with_attachments.labels(i, self.cinder_os_volume_project_id.get(i)).set(self.gauge_value_cinder_volume_available_with_attachments[i])

    def run_tool(self):
        log.info("- INFO - connecting to vcenter")
        self.vc_connect()
        # exit here in case we get problems connecting to the vcenter
        if not self.vc_connection_ok():
            log.error("- PLEASE CHECK MANUALLY - problems connecting to the vcenter - retrying in next loop run")
            sys.exit(1)
        log.info("- INFO - getting information from the vcenter")
        # exit here in case we get problems getting the viewref from the vcenter
        if not self.vc_get_viewref():
            log.error("- PLEASE CHECK MANUALLY - problems getting the viewref from the vcenter - retrying in next loop run")
            log.info("- INFO - disconnecting from the vcenter")
            self.vc_disconnect()
            sys.exit(1)
        # exit here in case we get problems getting data from openstack
        if not self.vc_get_info():
            log.error("- PLEASE CHECK MANUALLY - problems getting data from the vcenter - retrying in next loop run")
            log.info("- INFO - disconnecting from the vcenter")
            self.vc_disconnect()
            sys.exit(1)
        log.info("- INFO - connecting to openstack")
        self.os_connect()
        # exit here in case we get problems connecting to openstack
        if not self.os_connection_ok():
            log.error("- PLEASE CHECK MANUALLY - problems connecting to openstack - retrying in next loop run")
            sys.exit(1)
        log.info("- INFO - getting information from openstack (this may take a moment)")
        # exit here in case we get problems getting data from openstack
        if not self.os_get_info():
            log.error("- PLEASE CHECK MANUALLY - problems getting data from openstack - retrying in next loop run")
            log.info("- INFO - disconnecting from openstack")
            self.os_disconnect()
            sys.exit(1)
        # until this is fully implmented ...
        if self.cinderpassword:
            log.info("- INFO - connecting to the cinder db")
            self.cinder_db_connect()
            if not self.cinder_db_connection_ok():
                log.error("problems connecting to the cinder db")
                sys.exit(1)
        # until this is fully implmented ...
        if self.novapassword:
            log.info("- INFO - connecting to the nova db")
            self.nova_db_connect()
            if not self.nova_db_connection_ok():
                log.error("problems connecting to the nova db")
                sys.exit(1)
        self.volume_uuid_query_loop()
        log.info("- INFO - disconnecting from the vcenter")
        self.vc_disconnect()
        log.info("- INFO - disconnecting from openstack")
        self.os_disconnect()
        # until this is fully implmented ...
        if self.cinderpassword:
            log.info("- INFO - disconnecting from the cinder db")
            self.cinder_db_disconnect()
        # until this is fully implmented ...
        if self.novapassword:
            log.info("- INFO - disconnecting from the nova db")
            self.nova_db_disconnect()

    def run_check_loop(self, iterations):
        log.info("- INFO - connecting to vcenter")
        self.vc_connect()
        # stop this loop iteration here in case we get problems connecting to the vcenter
        if not self.vc_connection_ok():
            log.warn("- PLEASE CHECK MANUALLY - problems connecting to the vcenter - retrying in next loop run")
            return
        log.info("- INFO - getting information from the vcenter")
        # stop this loop iteration here in case we get problems getting the viewref from the vcenter
        if not self.vc_get_viewref():
            log.warn("- PLEASE CHECK MANUALLY - problems getting the viewref from the vcenter - retrying in next loop run")
            log.info("- INFO - disconnecting from the vcenter")
            self.vc_disconnect()
            return
        # stop this loop iteration here in case we get problems getting data from openstack
        if not self.vc_get_info():
            log.warn("- PLEASE CHECK MANUALLY - problems getting data from the vcenter - retrying in next loop run")
            log.info("- INFO - disconnecting from the vcenter")
            self.vc_disconnect()
            return
        log.info("- INFO - disconnecting from the vcenter")
        self.vc_disconnect()
        log.info("- INFO - connecting to openstack")
        self.os_connect()
        # stop this loop iteration here in case we get problems connecting to openstack
        if not self.os_connection_ok():
            log.warn("- PLEASE CHECK MANUALLY - problems connecting to openstack - retrying in next loop run")
            return
        log.info("- INFO - getting information from openstack (this may take a moment)")
        # stop this loop iteration here in case we get problems getting data from openstack
        if not self.os_get_info():
            log.warn("- PLEASE CHECK MANUALLY - problems getting data from openstack - retrying in next loop run")
            log.info("- INFO - disconnecting from openstack")
            self.os_disconnect()
            return
        log.info("- INFO - disconnecting from openstack")
        self.os_disconnect()
        log.info("- INFO - checking for inconsistencies")
        self.reset_gauge_values()
        self.discover_problems(iterations)
        self.send_gauge_values()

    def run_check(self, interval, iterations):
        self.start_prometheus_exporter()
        while True:
            # convert iterations from string to integer and avoid off by one error
            self.run_check_loop(int(iterations))
            # wait the interval time
            log.info("- INFO - waiting %s minutes before starting the next loop run", str(interval))
            time.sleep(60 * int(interval))
