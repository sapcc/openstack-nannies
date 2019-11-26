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
import datetime
import ConfigParser
import uuid

from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim, vmodl
from pyVim.task import WaitForTask, WaitForTasks

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

uuid_re = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', re.IGNORECASE)

# compile a regex for trying to filter out openstack generated vms
#  they all have the "name:" field set
openstack_re = re.compile("^name")

class ConsistencyCheck:
    def __init__(self, vchost, vcusername, vcpassword, novaconfig, cinderconfig, dry_run, prometheus_port, fix_limit, interactive):

        self.vchost = vchost
        self.vcusername = vcusername
        self.vcpassword = vcpassword
        self.novaconfig = novaconfig
        self.cinderconfig = cinderconfig
        self.dry_run = dry_run
        self.prometheus_port = prometheus_port
        self.interactive = interactive
        if fix_limit:
            self.max_automatic_fix = int(fix_limit)
        else:
            self.max_automatic_fix = 10

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
        self.cinder_volume_creating_for_too_long = dict()
        self.cinder_volume_deleting_for_too_long = dict()
        self.cinder_volume_is_in_state_reserved = dict()
        self.cinder_volume_available_with_attachments = dict()
        self.cinder_volume_in_use_without_some_attachments = dict()
        self.cinder_volume_in_use_without_attachments = dict()
        self.cinder_os_volume_status = dict()
        self.cinder_os_volume_project_id = dict()
        self.cinder_db_volume_attach_status = dict()
        self.cinder_db_volume_attachment_attach_status = dict()
        self.volume_attachment_fix_candidates = dict()

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
        #self.vm_handle = None
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
                                                  'how many volumes are in the state attaching for too long')
        self.gauge_cinder_volume_detaching_for_too_long = Gauge('vcenter_nanny_consistency_cinder_volume_detaching_for_too_long',
                                                  'how many volumes are in the state detaching for too long')
        self.gauge_cinder_volume_creating_for_too_long = Gauge('vcenter_nanny_consistency_cinder_volume_creating_for_too_long',
                                                  'how many volumes are in the state creating for too long')
        self.gauge_cinder_volume_deleting_for_too_long = Gauge('vcenter_nanny_consistency_cinder_volume_deleting_for_too_long',
                                                  'how many volumes are in the state deleting for too long')
        self.gauge_cinder_volume_is_in_state_reserved = Gauge('vcenter_nanny_consistency_cinder_volume_is_in_state_reserved',
                                                  'how many volumes are in the state reserved for too long')
        self.gauge_cinder_volume_available_with_attachments = Gauge('vcenter_nanny_consistency_cinder_volume_available_with_attachments',
                                                  'how many volumes are available with attachments for too long')
        self.gauge_cinder_volume_in_use_without_some_attachments = Gauge('vcenter_nanny_consistency_cinder_volume_in_use_without_some_attachments',
                                                  'how many volumes are in use without some attachments for too long')
        self.gauge_cinder_volume_in_use_without_attachments = Gauge('vcenter_nanny_consistency_cinder_volume_in_use_without_attachments',
                                                  'how many volumes are in use without any attachments for too long')
        self.gauge_cinder_volume_attachment_fix_count = Gauge('vcenter_nanny_consistency_cinder_volume_attachment_fix_count',
                                                  'how many volumes attachments need fixing')
        self.gauge_cinder_volume_attachment_max_fix_count = Gauge('vcenter_nanny_consistency_cinder_volume_attachment_max_fix_count',
                                                  'volumes attachment fixing is denied if there are more than this many attachments to fix')

        self.gauge_value_cinder_volume_attaching_for_too_long = 0
        self.gauge_value_cinder_volume_detaching_for_too_long = 0
        self.gauge_value_cinder_volume_creating_for_too_long = 0
        self.gauge_value_cinder_volume_deleting_for_too_long = 0
        self.gauge_value_cinder_volume_is_in_state_reserved = 0
        self.gauge_value_cinder_volume_available_with_attachments = 0
        self.gauge_value_cinder_volume_in_use_without_attachments = 0

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
                logging.error("- ERROR - failed to start prometheus exporter http server: %s", str(e))

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
                log.warn("problems connecting to the vcenter: %s", str(e))

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

    def vc_get_instance_handle(self,instance_uuid):

        try:
            search_index = self.vc_service_instance.content.searchIndex
            vm_handle = search_index.FindByUuid(None,instance_uuid, True, True)

        except Exception as e:
            log.warn("- PLEASE CHECK MANUALLY - Problem during instance search in vcenter %s", str(e))
            return False

        return vm_handle

    def vc_detach_volume_instance(self,vm_handle,volume_uuid):

        try:
            #finding volume_handle here
            volume_to_detach = None
            for dev in vm_handle.config.hardware.device:
                if isinstance(dev, vim.vm.device.VirtualDisk) \
                        and dev.backing.uuid == volume_uuid:
                    volume_to_detach = dev

            if not volume_to_detach:
                log.warn(
                    "- PLEASE CHECK MANUALLY - the volume %s on server %s does not seem to exist", volume_uuid, vm_handle.config.instanceUuid)
            if self.dry_run:
                log.info("- dry-run mode: detaching volume %s from server %s [%s]", volume_uuid, vm_handle.config.instanceUuid, vm_handle.config.name)
                return True

            else:
                log.info("- detaching volume  %s from server %s [%s]", volume_uuid, vm_handle.config.instanceUuid, vm_handle.config.name)
                volume_to_detach_spec = vim.vm.device.VirtualDeviceSpec()
                volume_to_detach_spec.operation = \
                    vim.vm.device.VirtualDeviceSpec.Operation.remove
                volume_to_detach_spec.device = volume_to_detach

                spec = vim.vm.ConfigSpec()
                spec.deviceChange = [volume_to_detach_spec]
                task = vm_handle.ReconfigVM_Task(spec=spec)
                try:
                    WaitForTask(task, si=self.vc_service_instance)
                except vmodl.fault.HostNotConnected:
                    log.warn("- PLEASE CHECK MANUALLY - cannot detach volume from server %s - the esx host it is running on is disconnected", vm_handle.config.instanceUuid)
                    return False
                except vim.fault.InvalidPowerState as e:
                    log.warn("- PLEASE CHECK MANUALLY - cannot detach volume from server %s - %s", vm_handle.config.instanceUuid, str(e.msg))
                    return False
                except vim.fault.GenericVmConfigFault as e:
                    log.warn("- PLEASE CHECK MANUALLY - cannot detach volume from server %s - %s", vm_handle.config.instanceUuid, str(e.msg))
                    return False
                return True

        except Exception as e:
                log.info("- PLEASE CHECK MANUALLY - error detaching volume %s from server %s - %s", volume_uuid, vm_handle.config.instanceUuid, str(e))

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
                # get the config.hardware.device property out of the data dict and iterate over its elements
                # this check seems to be required as in one bb i got a key error otherwise - looks like a vm without that property
                if k.get('config.hardware.device'):
                    for j in k.get('config.hardware.device'):
                        # we are only interested in disks for ghost volumes ...
                        # TODO: maybe? if isinstance(k.get('config.hardware.device'), vim.vm.device.VirtualDisk):
                        if 2000 <= j.key < 3000:
                            # we only care for vvols - in the past we checked starting with 2001 as 2000 usual was the eph
                            # storage, but it looks like eph can also be on another id and 2000 could be a vvol as well ...
                            if j.backing.fileName.lower().startswith('[vvol_'):
                                # warn about any volumes without a uuid set in the backing store settings
                                if not j.backing.uuid:
                                    log.warn("- PLEASE CHECK MANUALLY - volume without uuid in backing store settings: %s", str(j.backing.fileName))
                                    # TODO: extract uuid from filename if possible, maybe enabled by a switch
                                # check if the backing uuid setting is proper and potentially set it porperly:
                                # - the uuid should be the same as the uuid in the backing filename
                                # - if not the uuid should be set to the uuid extracted from the filename
                                #   as long as it is not the uuid in the name of the instance, because in that
                                #   case it might be a volume being move between bbs or similar ...
                                filename_uuid_search_result = uuid_re.search(j.backing.fileName)
                                instancename_uuid_search_result = uuid_re.search(k['config.name'])
                                if filename_uuid_search_result.group(0):
                                    if  j.backing.uuid != filename_uuid_search_result.group(0):
                                        log.warn("- PLEASE CHECK MANUALLY - volume uuid mismatch: uuid='%s', filename='%s'", str(j.backing.uuid), str(j.backing.fileName))
                                        if filename_uuid_search_result.group(0) != instancename_uuid_search_result.group(0):
                                            # check that the volume uuid we derived from the filename is in cinder
                                            if self.cinder_os_volume_status.get(str(filename_uuid_search_result.group(0))):
                                                log.warn("- plan (dry-run only for now): setting volume uuid %s to uuid %s obtained from filename '%s' (instance uuid='%s')", str(j.backing.uuid), str(filename_uuid_search_result.group(0)), str(j.backing.fileName), str(instancename_uuid_search_result.group(0)))
                                                # TODO: set uuid field to uuid values extraceted from filename
                                            else:
                                                log.warn("- PLEASE CHECK MANUALLY - volume uuid %s obtained from filename '%s' does not exist in cinder - not setting volume uuid to it for safety", str(filename_uuid_search_result.group(0)), str(j.backing.fileName))
                                        else:
                                            log.warn("- PLEASE CHECK MANUALLY - instance uuid %s is equal to volume uuid %s obtained from filename '%s' - not touching this one for safety", str(instancename_uuid_search_result.group(0)), str(filename_uuid_search_result.group(0)), str(j.backing.fileName))
                                else:
                                    log.warn("- PLEASE CHECK MANUALLY - no volume uuid found in filename='%s'", str(j.backing.fileName))
                                # check for volumes with a size of 0 which should not happen in a perfect world
                                if j.capacityInBytes == 0:
                                    log.warn("- PLEASE CHECK MANUALLY - volume with zero size: uuid='%s' filename='%s'", str(j.backing.uuid), str(j.backing.fileName))
                                    # TODO: add reload in that case, maybe enabled by a switch
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

    # return the database connection string from the config file
    def get_db_url(self, config_file):

        parser = ConfigParser.SafeConfigParser()
        try:
            parser.read(config_file)
            db_url = parser.get('database', 'connection', raw=True)
        except:
            log.info("ERROR: check configuration file %s", str(config_file))
            sys.exit(2)
        return db_url

    # connect to the cinder db
    def cinder_db_connect(self):

        try:
            db_url = self.get_db_url(self.cinderconfig)

            self.cinder_engine = create_engine(db_url)
            self.cinder_connection = self.cinder_engine.connect()
            Session = sessionmaker(bind=self.cinder_engine)
            self.cinder_thisSession = Session()
            self.cinder_metadata = MetaData()
            self.cinder_metadata.bind = self.cinder_engine
            self.cinder_Base = declarative_base()

        except Exception as e:
            log.warn("- WARNING - problems connecting to the cinder db - %s", str(e))
            return False

        return True

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


    def cinder_db_get_info(self):
        self.cinder_db_get_volume_attach_status()
        self.cinder_db_get_volume_attachment_attach_status()

    def cinder_db_get_volume_attach_status(self):

        cinder_db_volumes_t = Table('volumes', self.cinder_metadata, autoload=True)
        cinder_db_volume_attach_status_q = select(columns=[cinder_db_volumes_t.c.id, cinder_db_volumes_t.c.attach_status],whereclause=and_(cinder_db_volumes_t.c.deleted == 0))

        # build a dict indexed by volume_uuid (=.c.id) and with the value of attach_status
        for (volume_uuid, attach_status) in cinder_db_volume_attach_status_q.execute():
            self.cinder_db_volume_attach_status[volume_uuid] = attach_status

    def cinder_db_get_volume_attachment_attach_status(self):

        cinder_db_volume_attachment_t = Table('volume_attachment', self.cinder_metadata, autoload=True)
        cinder_db_volume_attachment_attach_status_q = select(columns=[cinder_db_volume_attachment_t.c.volume_id, cinder_db_volume_attachment_t.c.attach_status],whereclause=and_(cinder_db_volume_attachment_t.c.deleted == 0))

        # build a dict indexed by volume_uuid (=.c.volume_id) and with the value of attach_status
        for (volume_uuid, attach_status) in cinder_db_volume_attachment_attach_status_q.execute():
            self.cinder_db_volume_attachment_attach_status[volume_uuid] = attach_status

    def cinder_db_get_volume_attachment_ids(self):

        cinder_db_volume_attachment_t = Table('volume_attachment', self.cinder_metadata, autoload=True)
        # get even the deleted ones, as a newly inserted entry might clash with them as well
        # cinder_db_volume_attachment_ids_q = select(columns=[cinder_db_volume_attachment_t.c.id],whereclause=and_(cinder_db_volume_attachment_t.c.deleted == 0))
        cinder_db_volume_attachment_ids_q = select(columns=[cinder_db_volume_attachment_t.c.id])

        # build a list of volume attachment ids
        volume_attachment_ids=[]
        for (attachment_id) in cinder_db_volume_attachment_ids_q.execute():
            volume_attachment_ids.append(attachment_id[0].encode('ascii'))

        return volume_attachment_ids

    def cinder_db_update_volume_status(self, volume_uuid, new_status, new_attach_status):

        try:
            now = datetime.datetime.utcnow()
            cinder_db_volumes_t = Table('volumes', self.cinder_metadata, autoload=True)
            cinder_db_update_volume_attach_status_q = cinder_db_volumes_t.update().where(and_(cinder_db_volumes_t.c.id == volume_uuid, cinder_db_volumes_t.c.deleted == 0)).values(updated_at=now, status=new_status, attach_status=new_attach_status)
            cinder_db_update_volume_attach_status_q.execute()
        except Exception as e:
            log.warn("- WARNING - there was an error setting the status / attach_status of volume %s to %s / %s in the cinder db - %s", volume_uuid, new_status, new_attach_status, str(e))

    def cinder_db_delete_volume_attachement(self, volume_uuid):

        try:
            now = datetime.datetime.utcnow()
            cinder_db_volume_attachment_t = Table('volume_attachment', self.cinder_metadata, autoload=True)
            cinder_db_delete_volume_attachment_q = cinder_db_volume_attachment_t.update().where(and_(cinder_db_volume_attachment_t.c.volume_id == volume_uuid, cinder_db_volume_attachment_t.c.deleted == 0)).values(updated_at=now, deleted_at=now, deleted=True)
            cinder_db_delete_volume_attachment_q.execute()
        except Exception as e:
            log.warn("- WARNING - there was an error deleting the volume_attachment for the volume %s in the cinder db", volume_uuid)

    def cinder_db_delete_volume(self, volume_uuid):

        try:
            now = datetime.datetime.utcnow()
            cinder_db_volumes_t = Table('volumes', self.cinder_metadata, autoload=True)
            cinder_db_delete_volume_q = cinder_db_volumes_t.update().where(and_(cinder_db_volumes_t.c.id == volume_uuid, cinder_db_volumes_t.c.deleted == 0)).values(updated_at=now, deleted_at=now, deleted=1)
            cinder_db_delete_volume_q.execute()
        except Exception as e:
            log.warn("- WARNING - there was an error deleting the volume %s in the cinder db", volume_uuid)

    def cinder_db_insert_volume_attachment(self, fix_uuid, attachment_info):

        nova_attachment_id = attachment_info['attachment_id']

        if nova_attachment_id == None:
            # generate a new attachment_id (uuid) and make sure this one is not yet used
            while True:
                nova_attachment_id = str(uuid.uuid4())
                if nova_attachment_id not in self.cinder_db_get_volume_attachment_ids():
                    break

            # after we generated a new uuid, replace the missing attachment_id in the nova db with it
            log.info("- INFO - inserting a missing attachment_id entry for the volume %s into the nova db block_device_mapping table", fix_uuid)
            self.nova_db_add_volume_attachment_id(fix_uuid, nova_attachment_id)

        # first double check, that the attachment id has not yet been reused meanwhile (in the case it comes from the nova db)
        if nova_attachment_id not in self.cinder_db_get_volume_attachment_ids():

            try:
                now = datetime.datetime.utcnow()
                cinder_db_volume_attachment_t = Table('volume_attachment', self.cinder_metadata, autoload=True)
                cinder_db_insert_volume_attachment_q = cinder_db_volume_attachment_t.insert().values(created_at=now, updated_at=now, deleted=0, id=nova_attachment_id, volume_id=fix_uuid, instance_uuid=attachment_info['instance_uuid'], mountpoint=attachment_info['device_name'], attach_time=now, attach_mode='rw', attach_status='attached')
                cinder_db_insert_volume_attachment_q.execute()
            except Exception as e:
                log.error("- ERROR - there was an error inserting the volume attachment for the volume %s into the cinder db - %s", fix_uuid, str(e))

        else:
            log.error("- ERROR - the attachment id %s seems to already exist (maybe even as flagged deleted), giving up - please check by hand", fix_uuid)

    # connect to the nova db
    def nova_db_connect(self):

        # novadbstring = ''
        # if self.novadbname != 'nova-postgresql':
        #     novadbstring = self.novadbname.replace('_','-') + '-postgresql'

        try:
            db_url = self.get_db_url(self.novaconfig)
            
            self.nova_engine = create_engine(db_url)
            self.nova_connection = self.nova_engine.connect()
            Session = sessionmaker(bind=self.nova_engine)
            self.nova_thisSession = Session()
            self.nova_metadata = MetaData()
            self.nova_metadata.bind = self.nova_engine
            self.nova_Base = declarative_base()

        except Exception as e:
            log.warn(" - WARNING - problems connecting to the nova db - %s", str(e))
            return False

        return True

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


    def nova_db_get_attachment_info(self, volume_uuid):

        attachment_info = dict()

        nova_db_block_device_mapping_t = Table('block_device_mapping', self.nova_metadata, autoload=True)
        # maybe we can even live without the connection_info
        nova_db_get_attachment_info_q = select(columns=[nova_db_block_device_mapping_t.c.attachment_id, nova_db_block_device_mapping_t.c.device_name, nova_db_block_device_mapping_t.c.connection_info, nova_db_block_device_mapping_t.c.instance_uuid],whereclause=and_(nova_db_block_device_mapping_t.c.deleted == 0, nova_db_block_device_mapping_t.c.volume_id == volume_uuid))

        for (attachment_id, device_name, connection_info, instance_uuid) in nova_db_get_attachment_info_q.execute():
            attachment_info['attachment_id'] = attachment_id
            attachment_info['device_name'] = device_name
            attachment_info['connection_info'] = connection_info
            attachment_info['instance_uuid'] = instance_uuid

        return attachment_info

    # this function will generate a new uuid and add it to a block_device_mapping in case there is one missing
    # and the corresponding cinder entry is missing too
    def nova_db_add_volume_attachment_id(self, volume_uuid, new_attachment_id):

        try:
            now = datetime.datetime.utcnow()
            nova_db_block_device_mapping_t = Table('block_device_mapping', self.nova_metadata, autoload=True)
            nova_db_add_volume_attachment_id_q = nova_db_block_device_mapping_t.update().where(and_(nova_db_block_device_mapping_t.c.volume_id == volume_uuid, nova_db_block_device_mapping_t.c.deleted == 0)).values(updated_at=now, attachment_id=new_attachment_id)
            nova_db_add_volume_attachment_id_q.execute()
        except Exception as e:
            log.warn("- WARNING - there was an error adding an attachment_id to the block_device_mapping for the volume %s in the nova db", volume_uuid)

    def nova_db_delete_block_device_mapping(self, volume_uuid):

        try:
            now = datetime.datetime.utcnow()
            nova_db_block_device_mapping_t = Table('block_device_mapping', self.nova_metadata, autoload=True)
            nova_db_delete_block_device_mapping_q = nova_db_block_device_mapping_t.update().where(and_(nova_db_block_device_mapping_t.c.volume_id == volume_uuid, nova_db_block_device_mapping_t.c.deleted == 0)).values(updated_at=now, deleted_at=now, deleted=nova_db_block_device_mapping_t.c.id)
            nova_db_delete_block_device_mapping_q.execute()
        except Exception as e:
            log.warn("- WARNING - there was an error deleting the block device mapping for the volume %s in the nova db - %s", volume_uuid, str(e))

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
            temporary_server_list = self.os_conn.compute.servers(details=True, all_projects=1)
            if not temporary_server_list:
                raise RuntimeError('- PLEASE CHECK MANUALLY - did not get any nova instances back from the nova api - this should in theory never happen ...')
            for server in temporary_server_list:
                # we only care about servers from the vcenter this nanny is taking care of
                if server.availability_zone.lower() == self.vcenter_name:
                    self.nova_os_all_servers.append(server.id)
                    if server.attached_volumes:
                        for attachment in server.attached_volumes:
                            if self.nova_os_volumes_attached_at_server.get(server.id):
                                self.nova_os_volumes_attached_at_server[server.id].append(attachment['id'].encode('ascii'))
                            else:
                                self.nova_os_volumes_attached_at_server[server.id] = [attachment['id'].encode('ascii')]
                            self.nova_os_servers_with_attached_volume[attachment['id'].encode('ascii')] = server.id.encode('ascii')
            service = "cinder"
            temporary_volume_list = self.os_conn.block_store.volumes(details=True, all_projects=1)
            if not temporary_volume_list:
                raise RuntimeError('- PLEASE CHECK MANUALLY - did not get any cinder volumes back from the cinder api - this should in theory never happen ...')
            for volume in temporary_volume_list:
                # we only care about volumes from the vcenter this nanny is taking care of
                if volume.availability_zone.lower() == self.vcenter_name:
                    self.cinder_os_all_volumes.append(volume.id.encode('ascii'))
                    self.cinder_os_volume_status[volume.id.encode('ascii')] = volume.status.encode('ascii')
                    self.cinder_os_volume_project_id[volume.id.encode('ascii')] = volume.project_id.encode('ascii')
                    if volume.attachments:
                        for attachment in volume.attachments:
                            if self.cinder_os_servers_with_attached_volume.get(volume.id.encode('ascii')):
                                self.cinder_os_servers_with_attached_volume[volume.id.encode('ascii')].append(attachment['server_id'].encode('ascii'))
                            else:
                                self.cinder_os_servers_with_attached_volume[volume.id.encode('ascii')] = [attachment['server_id'].encode('ascii')]

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
            self.problem_fixes()

    def problem_fixes(self):
        # only offer fixes if the volume uuid entered is in the az this code is running against
        if self.volume_query in self.cinder_os_all_volumes:
            if self.problem_fix_volume_status_nothing_attached():
                return True
            if self.problem_fix_volume_status_all_attached():
                return True
            if self.problem_fix_only_partially_attached():
                return True
            # offer this fix in interactive mode only for now
            if self.interactive and self.problem_fix_sync_cinder_status():
                return True
            log.warning("- PLEASE CHECK MANUALLY - looks like everything is good - otherwise i have no idea how to fix this particualr case, then please check by hand")
        else:
            # TODO we should handle his in more detail, as we might have entries for meanwhile no longer existing volumes
            log.info("- PLEASE CHECK MANUALLY - the volume %s does not exist in this az, so no fix options offered", self.volume_query)

    def problem_fix_volume_status_nothing_attached(self):

        # TODO maybe even consider checking and setting the cinder_db_volume_attach_status
        # TODO maybe rethink if the in-use state should be ommited below, maybe add the error state as well?
        if (self.cinder_os_volume_status.get(self.volume_query) in ['in-use', 'attaching', 'detaching', 'creating', 'deleting', 'reserved']):
            if self.cinder_os_servers_with_attached_volume.get(self.volume_query):
                return False
            if self.nova_os_servers_with_attached_volume.get(self.volume_query):
                return False
            if self.vc_server_uuid_with_mounted_volume.get(self.volume_query):
                return False
            if self.interactive:
                if self.cinder_os_volume_status.get(self.volume_query) in ['creating', 'deleting']:
                    log.info("the state of the volume %s should be set to deleted to fix the problem", self.volume_query)
                else:
                    log.info("the state of the volume %s should be set to available / detached to fix the problem", self.volume_query)
                if self.ask_user_yes_no():
                    if self.cinder_os_volume_status.get(self.volume_query) in ['creating', 'deleting']:
                        log.info("- setting the state of the volume %s to deleted as requested", self.volume_query)
                        self.cinder_db_delete_volume(self.volume_query)
                    else:
                        log.info("- setting the state of the volume %s to available / detached as requested", self.volume_query)
                        self.cinder_db_update_volume_status(self.volume_query, 'available', 'detached')
                else:
                    log.info("- not fixing the problem as requested")
            else:
                if self.dry_run:
                    if self.cinder_os_volume_status.get(self.volume_query) in ['creating', 'deleting']:
                        log.info("- dry-run: setting the state of the volume %s to deleted", self.volume_query)
                    else:
                        log.info("- dry-run: setting the state of the volume %s to available / detached", self.volume_query)
                else:
                    if self.cinder_os_volume_status.get(self.volume_query) in ['creating', 'deleting']:
                        log.info("- action: setting the state of the volume %s to deleted", self.volume_query)
                        # the vcenter nanny will later take care to clean up the remaining volume on the vcenter
                        self.cinder_db_delete_volume(self.volume_query)
                    else:
                        log.info("- action: setting the state of the volume %s to available / detached", self.volume_query)
                        self.cinder_db_update_volume_status(self.volume_query, 'available', 'detached')

            return True
        else:
            log.debug("problem_fix_volume_status_nothing_attached does not apply")
            return False

    def problem_fix_volume_status_all_attached(self):

        # TODO maybe even consider checking and setting the cinder_db_volume_attach_status
        # TODO maybe rethink if the available state should be ommited below, maybe add the error state as well?
        if (self.cinder_os_volume_status.get(self.volume_query) in ['available', 'attaching', 'detaching', 'creating', 'deleting', 'reserved']):
            if not self.cinder_os_servers_with_attached_volume.get(self.volume_query):
                return False
            if not self.nova_os_servers_with_attached_volume.get(self.volume_query):
                return False
            if not self.vc_server_uuid_with_mounted_volume.get(self.volume_query):
                return False
            if self.interactive:
                log.info("the state of the volume %s should be set to in-use / attached to fix the problem", self.volume_query)
                if self.ask_user_yes_no():
                    log.info("- setting the state of the volume %s to in-use / attached as requested", self.volume_query)
                    self.cinder_db_update_volume_status(self.volume_query, 'in-use', 'attached')
                else:
                    log.info("- not fixing the problem as requested")
            else:
                if self.dry_run:
                    log.info("- dry-run: setting the state of the volume %s to in-use", self.volume_query)
                else:
                    log.info("- action: setting the state of the volume %s to in-use", self.volume_query)
                    self.cinder_db_update_volume_status(self.volume_query, 'in-use', 'attached')
            return True

        else:
            log.debug("problem_fix_volume_status_all_attached does not apply")
            return False

    def problem_fix_only_partially_attached(self):

        # TODO maybe even consider checking and setting the cinder_db_volume_attach_status
        # TODO maybe add the error state as well?
        if (self.cinder_os_volume_status.get(self.volume_query) in ['in-use', 'available', 'attaching', 'detaching', 'creating', 'deleting', 'reserved']):
            something_attached = False
            something_not_attached = False
            if self.cinder_os_servers_with_attached_volume.get(self.volume_query):
                something_attached = True
            if self.nova_os_servers_with_attached_volume.get(self.volume_query):
                something_attached = True
            if self.vc_server_uuid_with_mounted_volume.get(self.volume_query):
                something_attached = True
            if not self.cinder_os_servers_with_attached_volume.get(self.volume_query):
                something_not_attached = True
            if not self.nova_os_servers_with_attached_volume.get(self.volume_query):
                something_not_attached = True
            if not self.vc_server_uuid_with_mounted_volume.get(self.volume_query):
                something_not_attached = True
            if something_attached and something_not_attached:
                if self.interactive:
                    if self.cinder_os_servers_with_attached_volume.get(self.volume_query):
                        # below the self.cinder_os_servers_with_attached_volume.get(self.volume_query)[0] should maybe be replaced with proper handling of the corresponding list
                        log.info("the volume %s should be detached from server %s in cinder to fix the problem", self.volume_query, self.cinder_os_servers_with_attached_volume.get(self.volume_query)[0])
                    if self.nova_os_servers_with_attached_volume.get(self.volume_query):
                        log.info("the volume %s should be detached from server %s in nova to fix the problem", self.volume_query, self.nova_os_servers_with_attached_volume.get(self.volume_query))
                    if self.vc_server_uuid_with_mounted_volume.get(self.volume_query):
                        log.info("the volume %s should be detached from server %s in the vcenter to fix the problem", self.volume_query, self.vc_server_uuid_with_mounted_volume.get(self.volume_query))
                    if self.cinder_os_volume_status.get(self.volume_query) in ['creating', 'deleting']:
                        log.info("the state of the volume %s should be set to deleted to fix the problem", self.volume_query)
                    else:
                        log.info("the state of the volume %s should be set to available / detached to fix the problem", self.volume_query)
                    if self.ask_user_yes_no():
                        if self.cinder_os_servers_with_attached_volume.get(self.volume_query):
                            # below the self.cinder_os_servers_with_attached_volume.get(self.volume_query)[0] should maybe be replaced with proper handling of the corresponding list
                            log.info("- detaching the volume %s from server %s in cinder as requested", self.volume_query, self.cinder_os_servers_with_attached_volume.get(self.volume_query)[0])
                            self.cinder_db_delete_volume_attachement(self.volume_query)
                        if self.nova_os_servers_with_attached_volume.get(self.volume_query):
                            log.info("- detaching the volume %s from server %s in nova as requested", self.volume_query, self.nova_os_servers_with_attached_volume.get(self.volume_query))
                            self.nova_db_delete_block_device_mapping(self.volume_query)
                        if self.vc_server_uuid_with_mounted_volume.get(self.volume_query):
                            vm_handle = self.vc_get_instance_handle(self.vc_server_uuid_with_mounted_volume.get(self.volume_query))
                            if vm_handle:
                                log.info("- detaching volume %s from server %s in the vcenter as requested", self.volume_query, self.vc_server_uuid_with_mounted_volume.get(self.volume_query))
                                self.vc_detach_volume_instance(vm_handle, self.volume_query)
                        if self.cinder_os_volume_status.get(self.volume_query) in ['creating', 'deleting']:
                            log.info("- setting the state of the volume %s to deleted as requested", self.volume_query)
                            self.cinder_db_delete_volume(self.volume_query)
                        else:
                            log.info("- setting the state of the volume %s to available / detached as requested", self.volume_query)
                            self.cinder_db_update_volume_status(self.volume_query, 'available', 'detached')
                    else:
                        log.info("- not fixing the problem as requested")
                else:
                    if self.cinder_os_servers_with_attached_volume.get(self.volume_query):
                        if self.dry_run:
                            log.info("- dry-run: detaching the volume %s from server %s in cinder", self.volume_query, self.cinder_os_servers_with_attached_volume.get(self.volume_query)[0])
                        else:
                            log.info("- action: detaching the volume %s from server %s in cinder", self.volume_query, self.cinder_os_servers_with_attached_volume.get(self.volume_query)[0])
                            self.cinder_db_delete_volume_attachement(self.volume_query)
                    if self.nova_os_servers_with_attached_volume.get(self.volume_query):
                        if self.dry_run:
                            log.info("- dry-run: detaching the volume %s from server %s in nova", self.volume_query, self.nova_os_servers_with_attached_volume.get(self.volume_query))
                        else:
                            log.info("- action: detaching the volume %s from server %s in nova", self.volume_query, self.nova_os_servers_with_attached_volume.get(self.volume_query))
                            self.nova_db_delete_block_device_mapping(self.volume_query)
                    if self.vc_server_uuid_with_mounted_volume.get(self.volume_query):
                        vm_handle = self.vc_get_instance_handle(self.vc_server_uuid_with_mounted_volume.get(self.volume_query))
                        if vm_handle:
                            if self.dry_run:
                                log.info("- dry-run: detaching volume %s from server %s in the vcenter", self.volume_query, self.vc_server_uuid_with_mounted_volume.get(self.volume_query))
                            else:
                                log.info("- action: detaching volume %s from server %s in the vcenter", self.volume_query, self.vc_server_uuid_with_mounted_volume.get(self.volume_query))
                                self.vc_detach_volume_instance(vm_handle, self.volume_query)
                        if self.dry_run:
                            if self.cinder_os_volume_status.get(self.volume_query) in ['creating', 'deleting']:
                                log.info("- dry-run: setting the state of the volume %s to deleted", self.volume_query)
                            else:
                                log.info("- dry-run: setting the state of the volume %s to available / detached", self.volume_query)
                        else:
                            if self.cinder_os_volume_status.get(self.volume_query) in ['creating', 'deleting']:
                                log.info("- action: setting the state of the volume %s to deleted", self.volume_query)
                                # the vcenter nanny will later take care to clean up the remaining volume on the vcenter
                                self.cinder_db_delete_volume(self.volume_query)
                            else:
                                log.info("- action: setting the state of the volume %s to available / detached", self.volume_query)
                                self.cinder_db_update_volume_status(self.volume_query, 'available', 'detached')
            else:
                log.debug("problem_fix_only_partially_attached does not apply")
                return False

            return True

        else:
            log.debug("problem_fix_only_partially_attached does not apply")
            return False

    def problem_fix_sync_cinder_status(self):

        if (self.cinder_os_volume_status.get(self.volume_query) == 'available') and (self.cinder_db_volume_attach_status.get(self.volume_query) == 'attached'):
            log.info("the status and attach_status of the volume %s in the cinder db is available and attached", self.volume_query)
            log.info("- the attach_status should be set to detached")
            if self.ask_user_yes_no():
                log.info("- setting the attach_status of the volume %s to detached as requested", self.volume_query)
                self.cinder_db_update_volume_status(self.volume_query, 'available', 'detached')
            return True
        if (self.cinder_os_volume_status.get(self.volume_query) == 'in-use') and (self.cinder_db_volume_attach_status.get(self.volume_query) == 'detached'):
            log.info("the status and attach_status of the volume %s in the cinder db is in-use and detached", self.volume_query)
            log.info("- the attach_status should be set to attached")
            if self.ask_user_yes_no():
                log.info("- setting the attach_status of the volume %s to attached as requested", self.volume_query)
                self.cinder_db_update_volume_status(self.volume_query, 'in-use', 'attached')
            return True
        return False

    def ask_user_yes_no(self):
        while True:
            yesno=str(raw_input('do you want to do the above action(s) (y/n): '))
            if yesno == 'y':
                return True
            elif yesno == 'n':
                return False
            else:
                log.warn("wrong input - please answer again (y/n)")

    def print_volume_information(self):
        log.info("volume uuid: %s", self.volume_query)
        if self.volume_query in self.cinder_os_all_volumes:
            log.info("- this volume exists in cinder (for this az): Yes")
            log.info("- project id: %s", self.cinder_os_volume_project_id.get(self.volume_query))
            log.info("- volume status in cinder: %s", self.cinder_os_volume_status.get(self.volume_query))
            log.info("- volume attach_status in cinder db: %s", self.cinder_db_volume_attach_status.get(self.volume_query))
        else:
            log.info("- this volume exists in cinder (for this az): No")
        if self.cinder_os_servers_with_attached_volume.get(self.volume_query):
            for i in self.cinder_os_servers_with_attached_volume[self.volume_query]:
                log.info("os server with this volume attached (cinder): %s", i)
                if i in self.nova_os_all_servers:
                    log.info("- this instance exists in nova: Yes")
                else:
                    log.info("- this instance exists in nova: No")
                log.info("- volume_attachment attach_status in cinder db: %s", self.cinder_db_volume_attachment_attach_status.get(self.volume_query))
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
        self.gauge_value_cinder_volume_attaching_for_too_long = 0
        self.gauge_value_cinder_volume_detaching_for_too_long = 0
        self.gauge_value_cinder_volume_creating_for_too_long = 0
        self.gauge_value_cinder_volume_deleting_for_too_long = 0
        self.gauge_value_cinder_volume_is_in_state_reserved = 0
        self.gauge_value_cinder_volume_available_with_attachments = 0
        self.gauge_value_cinder_volume_in_use_without_some_attachments = 0
        self.gauge_value_cinder_volume_in_use_without_attachments = 0


    def discover_problems(self, iterations):
        self.discover_cinder_volume_attaching_for_too_long(iterations)
        self.discover_cinder_volume_detaching_for_too_long(iterations)
        self.discover_cinder_volume_creating_for_too_long(iterations)
        self.discover_cinder_volume_deleting_for_too_long(iterations)
        self.discover_cinder_volume_is_in_reserved_state(iterations)
        self.discover_cinder_volume_available_with_attachments(iterations)
        self.discover_cinder_volume_in_use_without_attachments(iterations)
        self.discover_cinder_volume_in_use_without_some_attachments(iterations)

    # in the below discover functions we increase a counter for each occurence of the problem per volume uuid
    # if the counter reaches 'iterations' then the problem is persisting for too long and we log a warning or fix it
    # as soon as the problem is gone for a volume uuid we reset the counter for it to 0 again, as everything
    # seems to be ok again
    def discover_cinder_volume_attaching_for_too_long(self, iterations):
        for volume_uuid in self.cinder_os_all_volumes:
            if self.cinder_os_volume_status.get(volume_uuid) == 'attaching':
                if not self.cinder_volume_attaching_for_too_long.get(volume_uuid):
                    self.cinder_volume_attaching_for_too_long[volume_uuid] = 1
                    log.debug("- plan: fix volume %s in project %s in state 'attaching' for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_attaching_for_too_long[volume_uuid], iterations)
                elif self.cinder_volume_attaching_for_too_long.get(volume_uuid) < iterations:
                    self.cinder_volume_attaching_for_too_long[volume_uuid] += 1
                    log.info("- plan: fix volume %s in project %s in state 'attaching' for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_attaching_for_too_long[volume_uuid], iterations)
                else:
                    self.gauge_value_cinder_volume_attaching_for_too_long += 1
                    # record this as a candidate for automatic fixing, not sure yet if we will need the value later at all ...
                    self.volume_attachment_fix_candidates[volume_uuid] = 'attaching'
            else:
                self.cinder_volume_attaching_for_too_long[volume_uuid] = 0

    def discover_cinder_volume_detaching_for_too_long(self, iterations):
        for volume_uuid in self.cinder_os_all_volumes:
            if self.cinder_os_volume_status.get(volume_uuid) == 'detaching':
                if not self.cinder_volume_detaching_for_too_long.get(volume_uuid):
                    self.cinder_volume_detaching_for_too_long[volume_uuid] = 1
                    log.debug("- plan: fix volume %s in project %s in state 'detaching' for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_detaching_for_too_long[volume_uuid], iterations)
                elif self.cinder_volume_detaching_for_too_long.get(volume_uuid) < iterations:
                    self.cinder_volume_detaching_for_too_long[volume_uuid] += 1
                    log.info("- plan: fix volume %s in project %s in state 'detaching' for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_detaching_for_too_long[volume_uuid], iterations)
                else:
                    self.gauge_value_cinder_volume_detaching_for_too_long += 1
                    self.volume_attachment_fix_candidates[volume_uuid] = 'detaching'
            else:
                self.cinder_volume_detaching_for_too_long[volume_uuid] = 0

    def discover_cinder_volume_creating_for_too_long(self, iterations):
        for volume_uuid in self.cinder_os_all_volumes:
            if self.cinder_os_volume_status.get(volume_uuid) == 'creating':
                if not self.cinder_volume_creating_for_too_long.get(volume_uuid):
                    self.cinder_volume_creating_for_too_long[volume_uuid] = 1
                    log.debug("- plan: fix volume %s in project %s in state 'creating' for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_creating_for_too_long[volume_uuid], iterations)
                elif self.cinder_volume_creating_for_too_long.get(volume_uuid) < iterations:
                    self.cinder_volume_creating_for_too_long[volume_uuid] += 1
                    log.info("- plan: fix volume %s in project %s in state 'creating' for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_creating_for_too_long[volume_uuid], iterations)
                else:
                    self.gauge_value_cinder_volume_creating_for_too_long += 1
                    self.volume_attachment_fix_candidates[volume_uuid] = 'creating'
            else:
                self.cinder_volume_creating_for_too_long[volume_uuid] = 0

    def discover_cinder_volume_deleting_for_too_long(self, iterations):
        for volume_uuid in self.cinder_os_all_volumes:
            if self.cinder_os_volume_status.get(volume_uuid) == 'deleting':
                if not self.cinder_volume_deleting_for_too_long.get(volume_uuid):
                    self.cinder_volume_deleting_for_too_long[volume_uuid] = 1
                    log.debug("- plan: fix volume %s in project %s in state 'deleting' for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_deleting_for_too_long[volume_uuid], iterations)
                elif self.cinder_volume_deleting_for_too_long.get(volume_uuid) < iterations:
                    self.cinder_volume_deleting_for_too_long[volume_uuid] += 1
                    log.info("- plan: fix volume %s in project %s in state 'deleting' for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_deleting_for_too_long[volume_uuid], iterations)
                else:
                    self.gauge_value_cinder_volume_deleting_for_too_long += 1
                    self.volume_attachment_fix_candidates[volume_uuid] = 'deleting'
            else:
                self.cinder_volume_deleting_for_too_long[volume_uuid] = 0

    def discover_cinder_volume_is_in_reserved_state(self, iterations):
        for volume_uuid in self.cinder_os_all_volumes:
            if self.cinder_os_volume_status.get(volume_uuid) == 'reserved':
                if not self.cinder_volume_is_in_state_reserved.get(volume_uuid):
                    self.cinder_volume_is_in_state_reserved[volume_uuid] = 1
                    log.debug("- plan: fix volume %s in project %s in state 'reserved' for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_is_in_state_reserved[volume_uuid], iterations)
                elif self.cinder_volume_is_in_state_reserved.get(volume_uuid) < iterations:
                    self.cinder_volume_is_in_state_reserved[volume_uuid] += 1
                    log.info("- plan: fix volume %s in project %s in state 'reserved' for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_is_in_state_reserved[volume_uuid], iterations)
                else:
                    self.gauge_value_cinder_volume_is_in_state_reserved += 1
                    self.volume_attachment_fix_candidates[volume_uuid] = 'reserved'
            else:
                self.cinder_volume_is_in_state_reserved[volume_uuid] = 0

    def discover_cinder_volume_available_with_attachments(self, iterations):
        for volume_uuid in self.cinder_os_all_volumes:
            if self.cinder_os_volume_status.get(volume_uuid) == 'available':
                if self.cinder_os_servers_with_attached_volume.get(volume_uuid):
                    if not self.cinder_volume_available_with_attachments.get(volume_uuid):
                        self.cinder_volume_available_with_attachments[volume_uuid] = 1
                        log.debug("- plan: fix volume %s in project %s in state 'available' with attachments for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_available_with_attachments[volume_uuid], iterations)
                    elif self.cinder_volume_available_with_attachments.get(volume_uuid) < iterations:
                        self.cinder_volume_available_with_attachments[volume_uuid] += 1
                        log.info("- plan: fix volume %s in project %s in state 'available' with attachments for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_available_with_attachments[volume_uuid], iterations)
                    else:
                        self.gauge_value_cinder_volume_available_with_attachments += 1
                        self.volume_attachment_fix_candidates[volume_uuid] = 'available with attachments'
                    continue
                if self.nova_os_servers_with_attached_volume.get(volume_uuid):
                    if not self.cinder_volume_available_with_attachments.get(volume_uuid):
                        self.cinder_volume_available_with_attachments[volume_uuid] = 1
                        log.debug("- plan: fix volume %s in project %s in state 'available' with attachments for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_available_with_attachments[volume_uuid], iterations)
                    elif self.cinder_volume_available_with_attachments.get(volume_uuid) < iterations:
                        self.cinder_volume_available_with_attachments[volume_uuid] += 1
                        log.info("- plan: fix volume %s in project %s in state 'available' with attachments for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_available_with_attachments[volume_uuid], iterations)
                    else:
                        self.gauge_value_cinder_volume_available_with_attachments += 1
                        self.volume_attachment_fix_candidates[volume_uuid] = 'available with attachments'
                    continue
                if self.vc_server_name_with_mounted_volume.get(volume_uuid):
                    if not self.cinder_volume_available_with_attachments.get(volume_uuid):
                        self.cinder_volume_available_with_attachments[volume_uuid] = 1
                        log.debug("- plan: fix volume %s in project %s in state 'available' with attachments for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_available_with_attachments[volume_uuid], iterations)
                    elif self.cinder_volume_available_with_attachments.get(volume_uuid) < iterations:
                        self.cinder_volume_available_with_attachments[volume_uuid] += 1
                        log.info("- plan: fix volume %s in project %s in state 'available' with attachments for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_available_with_attachments[volume_uuid], iterations)
                    else:
                        self.gauge_value_cinder_volume_available_with_attachments += 1
                        self.volume_attachment_fix_candidates[volume_uuid] = 'available with attachments'
                    continue
                self.cinder_volume_available_with_attachments[volume_uuid] = 0
            else:
                self.cinder_volume_available_with_attachments[volume_uuid] = 0

    def discover_cinder_volume_in_use_without_attachments(self, iterations):
        for volume_uuid in self.cinder_os_all_volumes:
            if self.cinder_os_volume_status.get(volume_uuid) == 'in-use':
                if not self.cinder_os_servers_with_attached_volume.get(volume_uuid):
                    if not self.nova_os_servers_with_attached_volume.get(volume_uuid):
                        if not self.vc_server_name_with_mounted_volume.get(volume_uuid):
                            if not self.cinder_volume_in_use_without_attachments.get(volume_uuid):
                                self.cinder_volume_in_use_without_attachments[volume_uuid] = 1
                                log.debug("- plan: fix volume %s in project %s in state 'in-use' without attachments for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_in_use_without_attachments[volume_uuid], iterations)
                            elif self.cinder_volume_in_use_without_attachments.get(volume_uuid) < iterations:
                                self.cinder_volume_in_use_without_attachments[volume_uuid] += 1
                                log.info("- plan: fix volume %s in project %s in state 'in-use' without attachments for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_in_use_without_attachments[volume_uuid], iterations)
                            else:
                                self.gauge_value_cinder_volume_in_use_without_attachments += 1
                                self.volume_attachment_fix_candidates[volume_uuid] = 'in use without attachments'
                        else:
                            self.cinder_volume_in_use_without_attachments[volume_uuid] = 0
                    else:
                        self.cinder_volume_in_use_without_attachments[volume_uuid] = 0
                else:
                    self.cinder_volume_in_use_without_attachments[volume_uuid] = 0
            else:
                self.cinder_volume_in_use_without_attachments[volume_uuid] = 0

    def discover_cinder_volume_in_use_without_some_attachments(self, iterations):
        for volume_uuid in self.cinder_os_all_volumes:
            # we only have to check for some missing attachments if we did not yet find out that all are missing :)
            if self.cinder_os_volume_status.get(volume_uuid) == 'in-use' and self.cinder_volume_in_use_without_attachments[volume_uuid] == 0:
                if not self.vc_server_name_with_mounted_volume.get(volume_uuid):
                    if not self.cinder_volume_in_use_without_some_attachments.get(volume_uuid):
                        self.cinder_volume_in_use_without_some_attachments[volume_uuid] = 1
                        log.debug("- PLEASE CHECK MANUALLY - volume %s in project %s is in state 'in-use' without some attachments (no vc) for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_in_use_without_some_attachments[volume_uuid], iterations)
                    elif self.cinder_volume_in_use_without_some_attachments.get(volume_uuid) < iterations:
                        self.cinder_volume_in_use_without_some_attachments[volume_uuid] += 1
                        log.info("- PLEASE CHECK MANUALLY - volume %s in project %s is in state 'in-use' without some attachments (no vc) for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_in_use_without_some_attachments[volume_uuid], iterations)
                    else:
                        self.gauge_value_cinder_volume_in_use_without_some_attachments += 1
                    continue
                if not self.nova_os_servers_with_attached_volume.get(volume_uuid):
                    if not self.cinder_volume_in_use_without_some_attachments.get(volume_uuid):
                        self.cinder_volume_in_use_without_some_attachments[volume_uuid] = 1
                        log.debug("- PLEASE CHECK MANUALLY - volume %s in project %s is in state 'in-use' without some attachments (no nova) for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_in_use_without_some_attachments[volume_uuid], iterations)
                    elif self.cinder_volume_in_use_without_some_attachments.get(volume_uuid) < iterations:
                        self.cinder_volume_in_use_without_some_attachments[volume_uuid] += 1
                        log.info("- PLEASE CHECK MANUALLY - volume %s in project %s is in state 'in-use' without some attachments (no nova) for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_in_use_without_some_attachments[volume_uuid], iterations)
                    else:
                        self.gauge_value_cinder_volume_in_use_without_some_attachments += 1
                    continue
                if not self.cinder_os_servers_with_attached_volume.get(volume_uuid):
                    if not self.cinder_volume_in_use_without_some_attachments.get(volume_uuid):
                        self.cinder_volume_in_use_without_some_attachments[volume_uuid] = 1
                        log.debug("- PLEASE CHECK MANUALLY - volume %s in project %s is in state 'in-use' without some attachments (no cinder) for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_in_use_without_some_attachments[volume_uuid], iterations)
                    elif self.cinder_volume_in_use_without_some_attachments.get(volume_uuid) < iterations:
                        self.cinder_volume_in_use_without_some_attachments[volume_uuid] += 1
                        log.info("- PLEASE CHECK MANUALLY - volume %s in project %s is in state 'in-use' without some attachments (no cinder) for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_in_use_without_some_attachments[volume_uuid], iterations)
                    else:
                        self.gauge_value_cinder_volume_in_use_without_some_attachments += 1
                    continue
                self.cinder_volume_in_use_without_some_attachments[volume_uuid] = 0
            else:
                self.cinder_volume_in_use_without_some_attachments[volume_uuid] = 0

    def send_gauge_values(self):
        self.gauge_cinder_volume_attaching_for_too_long.set(self.gauge_value_cinder_volume_attaching_for_too_long)
        self.gauge_cinder_volume_detaching_for_too_long.set(self.gauge_value_cinder_volume_detaching_for_too_long)
        self.gauge_cinder_volume_creating_for_too_long.set(self.gauge_value_cinder_volume_creating_for_too_long)
        self.gauge_cinder_volume_deleting_for_too_long.set(self.gauge_value_cinder_volume_deleting_for_too_long)
        self.gauge_cinder_volume_is_in_state_reserved.set(self.gauge_value_cinder_volume_is_in_state_reserved)
        self.gauge_cinder_volume_available_with_attachments.set(self.gauge_value_cinder_volume_available_with_attachments)
        self.gauge_cinder_volume_in_use_without_some_attachments.set(self.gauge_value_cinder_volume_in_use_without_some_attachments)
        self.gauge_cinder_volume_in_use_without_attachments.set(self.gauge_value_cinder_volume_in_use_without_attachments)
        self.gauge_cinder_volume_attachment_fix_count.set(len(self.volume_attachment_fix_candidates))
        self.gauge_cinder_volume_attachment_max_fix_count.set(self.max_automatic_fix)

    def run_tool(self):
        log.info("- INFO - connecting to the vcenter")
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
        log.info("- INFO - connecting to the cinder db")
        self.cinder_db_connect()
        if not self.cinder_db_connection_ok():
            log.error("problems connecting to the cinder db")
            sys.exit(1)
        log.info("- INFO - getting information from the cinder db")
        self.cinder_db_get_info()
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
        log.info("- INFO - disconnecting from the cinder db")
        self.cinder_db_disconnect()
        log.info("- INFO - disconnecting from the nova db")
        self.nova_db_disconnect()

    def run_check_loop(self, iterations):
        if self.dry_run:
            log.info("- INFO - running in dry run mode")
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
        self.reset_gauge_values()
        # clean the list of canditate volume uuids which are supposed to be fixed automatically
        self.volume_attachment_fix_candidates.clear()
        log.info("- INFO - connecting to the cinder db")
        self.cinder_db_connect()
        if not self.cinder_db_connection_ok():
            log.error("- PLEASE CHECK MANUALLY - problems connecting to the cinder db - retrying in next loop run")
            return
        log.info("- INFO - connecting to the nova db")
        self.nova_db_connect()
        if not self.nova_db_connection_ok():
            log.error("- PLEASE CHECK MANUALLY - problems connecting to the nova db - retrying in next loop run")
            return
        # this will check for and log the inconsistent volume attachments and build a dict of affected volume uuids
        log.info("- INFO - checking for inconsistencies")
        self.discover_problems(iterations)
        # send the metrics how many inconsistencies we have found
        self.send_gauge_values()
        # if the dict is lower than a certain threshold (to avoid too big accidental damage), fix them automatically
        if len(self.volume_attachment_fix_candidates) <= self.max_automatic_fix:
            log.info("- DEBUG - number of fix candidates: %s - max number for automatic fixing: %s", str(len(self.volume_attachment_fix_candidates)), str(self.max_automatic_fix))
            # TODO this is ugly - maybe better replace the self.volume_query in all the offer_problem fix
            # functions with a regular (non self) paramater
            for self.volume_query in self.volume_attachment_fix_candidates:
                self.problem_fixes()
        else:
            # TODO create a metric for this case we may alert on
            log.warn("- PLEASE CHECK MANUALLY - too many (more than %s) volume attachment inconsistencies - denying to fix them automatically", str(self.max_automatic_fix))
        log.info("- INFO - disconnecting from the cinder db")
        self.cinder_db_disconnect()
        log.info("- INFO - disconnecting from the nova db")
        self.nova_db_disconnect()
        log.info("- INFO - disconnecting from the vcenter")
        self.vc_disconnect()

    def run_check(self, interval, iterations):
        self.start_prometheus_exporter()
        while True:
            log.info("INFO: starting new loop run")
            # convert iterations from string to integer and avoid off by one error
            self.run_check_loop(int(iterations))
            # wait the interval time
            log.info("INFO: waiting %s minutes before starting the next loop run", str(interval))
            time.sleep(60 * int(interval))
