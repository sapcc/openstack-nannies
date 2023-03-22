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
import configparser
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

# search string to find instance names with a valid openstack uuid in them
uuid_re = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', re.IGNORECASE)
# search string to find instance instance names with a valid openstack uuid in them (i.e. with
# a name='uuid' and not name ='real name (uuid)', which would be regular instances)
filename_uuid_re = re.compile('/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', re.IGNORECASE)

# compile a regex for trying to filter out openstack generated vms
# they all have the "name:" field set
openstack_re = re.compile("^name")

class ConsistencyCheck:
    def __init__(self, vchost, vcusername, vcpassword, novaconfig, cinderconfig, dry_run, prometheus_port, fix_limit, interactive):

        self.vchost = vchost
        self.vcusername = vcusername
        self.vcpassword = vcpassword
        self.novaconfig = novaconfig
        self.cinderconfig = cinderconfig
        self.dry_run = dry_run
        # backup of the original cmdline dry-run setting as we might overwrite it later temporarily
        self.cmdline_dry_run = dry_run
        self.prometheus_port = prometheus_port
        self.interactive = interactive
        if fix_limit:
            self.max_automatic_fix = int(fix_limit)
        else:
            self.max_automatic_fix = 10

        self.nova_os_all_servers = []
        self.cinder_os_all_volumes = []
        self.vc_all_volumes = []

        # initialize some dicts - those have a volume uuid as key
        self.nova_os_servers_with_attached_volume = dict()
        self.cinder_os_servers_with_attached_volume = dict()
        self.vc_server_uuid_with_mounted_volume = dict()
        # fnb = file name based, i.e. uuid extracted from the filename
        self.vc_server_uuid_with_mounted_volume_fnb = dict()
        self.vc_server_name_with_mounted_volume = dict()
        self.vc_server_name_with_mounted_volume_fnb = dict()
        self.vc_vmdk_filename_for_backing_uuid = dict()
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
        self.instance_reload_candidates = set()

        # this one has the instance uuid as key
        self.nova_os_volumes_attached_at_server = dict()
        self.vcenter_instances_without_mounts = dict()
        self.old_vcenter_instance_without_backinguuid_for_volume = dict()
        self.old_vcenter_instance_without_extraconfig_for_volume = dict()

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
        self.gauge_vcenter_instance_name_mismatch = Gauge('vcenter_nanny_consistency_vcenter_instance_name_mismatch',
                                                  'how many shadow vms have a mismatch between name and config.name in the vcenter')
        self.gauge_vcenter_volume_backinguuid_mismatch = Gauge('vcenter_nanny_consistency_vcenter_volume_backinguuid_mismatch',
                                                  'how many volumes have a volume backing uuid mismatch in the vcenter')
        self.gauge_vcenter_volume_uuid_mismatch = Gauge('vcenter_nanny_consistency_vcenter_volume_uuid_mismatch',
                                                  'how many shadow vms have a uuid to name mismatch in the vcenter')
        self.gauge_vcenter_volume_uuid_adjustment = Gauge('vcenter_nanny_consistency_vcenter_volume_uuid_adjustment',
                                                  'how many volumes got a uuid mismatch adjusted in the vcenter')
        self.gauge_vcenter_volume_uuid_missing = Gauge('vcenter_nanny_consistency_vcenter_volume_uuid_missing',
                                                  'how many volumes are missing a uuid in the vcenter backing store config')
        self.gauge_vcenter_backinguuid_extraconfig_missing = Gauge('vcenter_nanny_consistency_vcenter_backinguuid_extraconfig_missing',
                                                  'how many volumes with a backing uuid have no mapping in extraConfig')
        self.gauge_vcenter_extraconfig_backinguuid_missing = Gauge('vcenter_nanny_consistency_vcenter_extraconfig_backinguuid_missing',
                                                  'how many volumes are missing a backing uuid for volumes in extraConfig')
        self.gauge_vcenter_volume_zero_size = Gauge('vcenter_nanny_consistency_vcenter_volume_zero_size',
                                                  'how many volumes have a size of zero in the vcenter')
        self.gauge_vcenter_instance_state_gray = Gauge('vcenter_nanny_consistency_vcenter_instance_status_gray',
                                                  'how many instances have a gray status in the vcenter')
        self.gauge_no_autofix = Gauge('vcenter_nanny_consistency_no_autofix',
                                                  'the number of volume inconsistencies not fixable automatically')
        self.gauge_bb_not_in_aggregate = Gauge('vcenter_nanny_consistency_vcenter_bb_not_in_aggregate',
                                                  'the number of bb not in an aggregate')

        self.gauge_value_cinder_volume_attaching_for_too_long = 0
        self.gauge_value_cinder_volume_detaching_for_too_long = 0
        self.gauge_value_cinder_volume_creating_for_too_long = 0
        self.gauge_value_cinder_volume_deleting_for_too_long = 0
        self.gauge_value_cinder_volume_is_in_state_reserved = 0
        self.gauge_value_cinder_volume_available_with_attachments = 0
        self.gauge_value_cinder_volume_in_use_without_attachments = 0
        self.gauge_value_vcenter_instance_name_mismatch = 0
        self.gauge_value_vcenter_volume_backinguuid_mismatch = 0
        self.gauge_value_vcenter_volume_uuid_mismatch = 0
        self.gauge_value_vcenter_volume_uuid_adjustment = 0
        self.gauge_value_vcenter_volume_uuid_missing = 0
        self.gauge_value_vcenter_backinguuid_extraconfig_missing = 0
        self.gauge_value_vcenter_extraconfig_backinguuid_missing = 0
        self.gauge_value_vcenter_volume_zero_size = 0
        self.gauge_value_vcenter_instance_state_gray = 0
        self.gauge_value_no_autofix = 0
        self.gauge_value_bb_not_in_aggregate = 0

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

    def vc_short_name(self):
        # return a shortened vc hostname - i.e. vc-a-0 from vc-a-0.cc.region.some-domain.com for example
        return self.vchost.split(".")[0]

    def vc_region_name(self):
        # return the region name extracted from the vc hostname - i.e. region from vc-a-0.cc.region.some-domain.com for example
        return self.vchost.split(".")[2]

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

    def vc_reload_instance(self,instance_uuid):

        vm_handle = self.vc_get_instance_handle(instance_uuid)
        try:
            vm_handle.Reload()

        except Exception as e:
            log.warn("- PLEASE CHECK MANUALLY - Problem during instance reload in vcenter %s", str(e))
            return False

        return True

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
                    "- PLEASE CHECK MANUALLY - vc_detach_volume_instance: the volume %s on server %s does not seem to exist", volume_uuid, vm_handle.config.instanceUuid)
            if self.dry_run:
                log.info("- dry-run: detaching volume %s from server %s [%s]", volume_uuid, vm_handle.config.instanceUuid, vm_handle.config.name)
                return True

            else:
                log.info("- action: detaching volume  %s from server %s [%s]", volume_uuid, vm_handle.config.instanceUuid, vm_handle.config.name)
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

    # TODO: this function is not used yet, as it will only work if the instance is turned off
    def vc_rename_volume_backing_uuid(self,instance_uuid,old_volume_uuid,new_volume_uuid):

        vm_handle = self.vc_get_instance_handle(instance_uuid)
        try:
            #finding volume_handle here
            volume_to_rename = None
            for dev in vm_handle.config.hardware.device:
                if isinstance(dev, vim.vm.device.VirtualDisk) \
                        and dev.backing.uuid == old_volume_uuid:
                    volume_to_rename = dev

            if not volume_to_rename:
                log.warn(
                    "- PLEASE CHECK MANUALLY - vc_rename_volume_backing_uuid: the volume %s on server %s does not seem to exist", volume_uuid, vm_handle.config.instanceUuid)
            if self.dry_run:
                log.info("- dry-run: renaming volume backing uuid from %s to %s [attached to instance %s]", old_volume_uuid, new_volume_uuid, vm_handle.config.instanceUuid)
                return True

            else:
                log.info("- action: renaming volume backing uuid from %s to %s [attached to instance %s]", old_volume_uuid, new_volume_uuid, vm_handle.config.instanceUuid)
                volume_to_rename_spec = vim.vm.device.VirtualDeviceSpec()
                volume_to_rename_spec.operation = \
                    vim.vm.device.VirtualDeviceSpec.Operation.edit
                volume_to_rename_spec.device = volume_to_rename

                volume_to_rename_spec.device.backing = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
                volume_to_rename_spec.device.backing.uuid = new_volume_uuid

                spec = vim.vm.ConfigSpec()
                spec.deviceChange = [volume_to_rename_spec]
                task = vm_handle.ReconfigVM_Task(spec=spec)
                try:
                    WaitForTask(task, si=self.vc_service_instance)
                except vmodl.fault.HostNotConnected:
                    log.warn("- PLEASE CHECK MANUALLY - cannot rename volume backing uuid %s on server %s - the esx host it is running on is disconnected", old_volume_uuid, vm_handle.config.instanceUuid)
                    return False
                except vim.fault.InvalidPowerState as e:
                    log.warn("- PLEASE CHECK MANUALLY - cannot rename volume backing uuid %s on server %s - %s", old_volume_uuid, vm_handle.config.instanceUuid, str(e.msg))
                    return False
                except vim.fault.GenericVmConfigFault as e:
                    log.warn("- PLEASE CHECK MANUALLY - cannot rename volume backing uuid %s on server %s - %s", old_volume_uuid, vm_handle.config.instanceUuid, str(e.msg))
                    return False
                return True

        except Exception as e:
                log.info("- PLEASE CHECK MANUALLY - error renaming volume backing uuid %s on server %s - %s", old_volume_uuid, vm_handle.config.instanceUuid, str(e.msg))

    # this will no longer be used as it is not safe in all situations
    def vc_rename_instance_uuid(self,instance_uuid,uuid_from_instance_name):

        vm_handle = self.vc_get_instance_handle(instance_uuid)

        if self.dry_run:
            log.info("- dry-run: renaming instanceUuid %s to uuid %s extracted from instance name ('%s')", instance_uuid, uuid_from_instance_name, vm_handle.config.name)
            return True

        else:
            log.info("- action: renaming instanceUuid %s to uuid %s extracted from instance name ('%s')", instance_uuid, uuid_from_instance_name, vm_handle.config.name)
            spec = vim.vm.ConfigSpec()
            spec.instanceUuid = uuid_from_instance_name
            task = vm_handle.ReconfigVM_Task(spec=spec)
            try:
                WaitForTask(task, si=self.vc_service_instance)
            except vmodl.fault.HostNotConnected:
                log.warn("- PLEASE CHECK MANUALLY - cannot rename instanceUuid %s to uuid %s - the esx host it is running on is disconnected", old_volume_uuid, vm_handle.config.instanceUuid)
                return False
            except vim.fault.InvalidPowerState as e:
                log.warn("- PLEASE CHECK MANUALLY - cannot rename instanceUuid %s to uuid %s - %s", instance_uuid, uuid_from_instance_name, str(e.msg))
                return False
            except vim.fault.GenericVmConfigFault as e:
                log.warn("- PLEASE CHECK MANUALLY - cannot rename instanceUuid %s to uuid %s - %s", instance_uuid, uuid_from_instance_name, str(e.msg))
                return False
            except Exception as e:
                log.info("- PLEASE CHECK MANUALLY - cannot rename instanceUuid %s to uuid %s - %s", instance_uuid, uuid_from_instance_name, str(e.msg))
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

    # get all servers and all volumes from the vcenter and do some basic consistency checks already along the way
    def vc_get_info(self):

        # TODO better exception handling

        # clear the old lists
        self.vc_all_volumes *= 0

        # clean all old dicts
        self.vc_server_uuid_with_mounted_volume.clear()
        self.vc_server_uuid_with_mounted_volume_fnb.clear()
        self.vc_server_name_with_mounted_volume.clear()
        self.vc_server_name_with_mounted_volume_fnb.clear()
        self.vc_vmdk_filename_for_backing_uuid.clear()
        self.vcenter_instances_without_mounts.clear()

        # keep a dict of volumes without backing uuid or entraconfig entry in this loop run
        # key is the volume uuid and value the instance
        new_vcenter_instance_without_backinguuid_for_volume = dict()
        new_vcenter_instance_without_extraconfig_for_volume = dict()

        has_volume_attachments = dict()

        # the properties we want to collect - some of them are not yet used, but will at a later
        # development stage of this script to validate the volume attachments with cinder and nova
        vm_properties = [
            "config.hardware.device",
            "config.name",
            "config.uuid",
            "config.instanceUuid",
            "config.template",
            "config.annotation",
            "config.extraConfig",
            "overallStatus",
            "name"
        ]

        # collect the properties for all vms
        self.vc_data = self.vc_collect_properties(vim.VirtualMachine, vm_properties, True)

        # in case we do not get the properties
        if not self.vc_data:
            return False

        # iterate over the list of vms
        for k in self.vc_data:
            # only work with results, which have an instance uuid defined and are openstack vms (i.e. have an annotation set)
            if k.get('config.instanceUuid') and not k.get('config.template'):
                # clear the backing uuid list for each new relevant instance
                backing_uuid_list = []
                # check if name and config.name properties are the same an warn if not
                if k.get('name') != k.get('config.name'):
                    log.warn("- PLEASE CHECK MANUALLY - name property '%s' differs from config.name property '%s'", k.get('name'), k.get('config.name'))
                    self.gauge_value_vcenter_instance_name_mismatch += 1
                # get the config.hardware.device property out of the data dict and iterate over its elements
                # this check seems to be required as in one bb i got a key error otherwise - looks like a vm without that property
                if k.get('config.hardware.device'):
                    for j in k.get('config.hardware.device'):
                        # we are only interested in disks for ghost volumes ...
                        # old test was: if 2000 <= j.key < 3000:
                        if not isinstance(j, vim.vm.device.VirtualDisk):
                            continue
                        # we only care for vvols - in the past we checked starting with 2001 as 2000 usual was the eph
                        # storage, but it looks like eph can also be on another id and 2000 could be a vvol as well ...
                        if not j.backing.fileName.lower().startswith('[vvol_') and not j.backing.fileName.lower().startswith('[vmfs_'):
                            continue
                        # try to find an openstack uuid in the filename
                        filename_uuid_search_result = filename_uuid_re.search(j.backing.fileName)
                        # warn about any volumes without a uuid set in the backing store config
                        if not j.backing.uuid:
                            self.gauge_value_vcenter_volume_uuid_missing += 1
                            # check if we can extract a uuid from the vcenter filename
                            if filename_uuid_search_result and filename_uuid_search_result.group(1):
                                # if yes - is that uuid known to cinder then
                                if filename_uuid_search_result.group(1) in self.cinder_os_all_volumes:
                                    # do the consistency check logging only in non interactive mode
                                    if not self.interactive:
                                        log.warn("- PLEASE CHECK MANUALLY - volume on instance '%s' without uuid in backing store config - shadow vm uuid extracted from its vcenter filename '%s' is %s", str(k.get('config.name')), str(j.backing.fileName), str(filename_uuid_search_result.group(1)))
                                    # if yes - use that uuid later
                                    my_volume_uuid = filename_uuid_search_result.group(1)
                                # if no - we do not have any useable uuid
                                else:
                                    # do the consistency check logging only in non interactive mode
                                    if not self.interactive:
                                        log.warn("- PLEASE CHECK MANUALLY - volume on instance '%s' without uuid in backing store config - shadow vm uuid extracted from its vcenter filename '%s' is %s but is not in cinder", str(k.get('config.name')), str(j.backing.fileName), str(filename_uuid_search_result.group(1)))
                                    my_volume_uuid = None
                            # if we cannot extract a uuid from the vcenter filename we do not have any useable uuid
                            else:
                                # do the consistency check logging only in non interactive mode
                                if not self.interactive:
                                    log.warn("- PLEASE CHECK MANUALLY - volume on instance '%s' without uuid in backing store config - shadow vm uuid extraction from its vcenter filename '%s' failed", str(k.get('config.name')), str(j.backing.fileName))
                                my_volume_uuid = None
                                # in this case force dry run mode as we seem to have volumes we cannot trust anymore
                                log.error("- PLEASE CHECK MANUALLY - volume on instance %s without uuid in backing store config and wrong filename %s on datastore - forcing dry-run mode!", str(k.get('config.name')), str(j.backing.fileName))
                                self.gauge_value_vcenter_volume_uuid_missing += 1
                                self.dry_run = True
                        # so we have a uuid in the backing store config
                        else:
                            # build a list of all backing uuids for that instance to later compare it to the
                            # volume uuids from the extraConfig volume properties
                            backing_uuid_list.append(j.backing.uuid)

                            # check if we can extract a uuid from the filename too and if both differ and are both in cinder something is wrong
                            if filename_uuid_search_result and (j.backing.uuid != filename_uuid_search_result.group(1)) and (j.backing.uuid in self.cinder_os_all_volumes) \
                                and (filename_uuid_search_result.group(1) in self.cinder_os_all_volumes):
                                # do the consistency check logging only in non interactive mode
                                if not self.interactive:
                                    log.warn("- PLEASE CHECK MANUALLY - volume on instance '%s' with uuid %s in backing store config in cinder and different shadow vm uuid %s extracted from its vcenter filename '%s' in cinder too", str(k.get('config.name')), str(j.backing.uuid), str(filename_uuid_search_result.group(1)), str(j.backing.fileName))
                                # set my_volume_uuid to the backing uuid as after some manual moves of volumes in
                                # the vcenter the filename might have changed and we can trust the backing uuid more
                                # in this case
                                my_volume_uuid = j.backing.uuid
                            # ok - they are either equal or we only have a backing uuid - check if it is in cinder and if yes - use it
                            elif j.backing.uuid in self.cinder_os_all_volumes:
                                my_volume_uuid = j.backing.uuid
                            # hmmm - so no backing uuid - lets see if we can extract a uuid from the filename and if it is in cinder - if yes - use it
                            elif filename_uuid_search_result and (filename_uuid_search_result.group(1) in self.cinder_os_all_volumes):
                                if not self.interactive:
                                    log.warn("- PLEASE CHECK MANUALLY - volume on instance '%s' with uuid in backing store config %s not in cinder - shadow vm uuid extracted from its vcenter filename '%s' is %s", str(k.get('config.name')), str(j.backing.uuid), str(j.backing.fileName), str(filename_uuid_search_result.group(1)))
                                my_volume_uuid = filename_uuid_search_result.group(1)
                            # looks like we simply cannot find a useable uuid
                            else:
                                # do the consistency check logging only in non interactive mode
                                if not self.interactive:
                                    log.warn("- PLEASE CHECK MANUALLY - volume on instance '%s' with uuid %s in backing store config not in cinder and no shadow vm uuid extractable from its vcenter filename", str(k.get('config.name')), str(j.backing.uuid))
                                my_volume_uuid = None

                        # check if the backing uuid setting is proper: it should be the same as the uuid extracted from the filename:
                        # TODO: in theory this should also be applied to shadow vms, i.e. without a config.annotation
                        # do the consistency check logging only in non interactive mode
                        if not self.interactive:
                            if filename_uuid_search_result and filename_uuid_search_result.group(1):
                                if j.backing.uuid != filename_uuid_search_result.group(1):
                                    log.warn("- PLEASE CHECK MANUALLY - volume backing uuid mismatch: backing uuid=%s, filename='%s', instance name='%s'", str(j.backing.uuid), str(j.backing.fileName), str(k['config.name']))
                                    self.gauge_value_vcenter_volume_backinguuid_mismatch += 1
                            else:
                                log.warn("- PLEASE CHECK MANUALLY - no shadow vm uuid found in filename='%s' on instance '%s'", str(j.backing.fileName), str(k['config.name']))

                            # check for volumes with a size of 0 which should not happen in a perfect world
                            if my_volume_uuid and (j.capacityInBytes == 0):
                                log.warn("- PLEASE CHECK MANUALLY - volume %s on instance '%s' with zero size - filename is '%s'", str(my_volume_uuid), str(k['config.name']), str(j.backing.fileName))
                                # build a candidate list of instances to reload to get rid of their buggy zero volume sizes
                                # disable the automatic reload for now
                                #self.instance_reload_candidates.add(k['config.instanceUuid'])
                                self.gauge_value_vcenter_volume_zero_size += 1

                        # this section now collects some information we need later for consistency checking
                        # and detailed logging in case of inconsistencies
                        # if we can extract a uuid from the vmdk filename via disk backing entry then
                        # note it down in case it is a valid openstack volume uuid
                        if filename_uuid_search_result and openstack_re.match(k.get('config.annotation', 'no_annotation')):
                            if filename_uuid_search_result.group(1) in self.cinder_os_all_volumes:
                                self.vc_server_uuid_with_mounted_volume_fnb[filename_uuid_search_result.group(1)] = k['config.instanceUuid']
                                self.vc_server_name_with_mounted_volume_fnb[filename_uuid_search_result.group(1)] = k['config.name']
                        # save the filename from the backing node here as well
                        if j.backing.uuid:
                                    self.vc_vmdk_filename_for_backing_uuid[str(j.backing.uuid)] = j.backing.fileName

                        # we no longer use this code path as my_volume_uuid might not be reliable oin some cases
                        # # if we have my_volume_uuid, which is either the uuid from the backing config or (if that does not exist or is not
                        # # in cinder) will be extracted from the filename, then we assume this volume uuid to be attached to the instance
                        # # and we only care about openstack instances (with annotations) here
                        # if my_volume_uuid and openstack_re.match(k.get('config.annotation', 'no_annotation')):
                        #     # map attached volume id to instance uuid - used later
                        #     self.vc_server_uuid_with_mounted_volume[my_volume_uuid] = k['config.instanceUuid']
                        #     # map attached volume id to instance name - used later for more detailed logging
                        #     self.vc_server_name_with_mounted_volume[my_volume_uuid] = k['config.name']
                        #     log.debug("==> mount - instance: %s - volume: %s", str(k['config.instanceUuid']), str(my_volume_uuid))
                        #     has_volume_attachments[k['config.instanceUuid']] = True

                    # try to find an openstack uuid in the instance name
                    instancename_uuid_search_result = uuid_re.search(k['name'])
                    # check for vms with overallStatus gray and put the on the reload candidates list as well
                    if k.get('overallStatus') == 'gray':
                        # do the consistency check logging only in non interactive mode
                        if not self.interactive:
                            log.warn("- PLEASE CHECK MANUALLY - instance %s (name='%s') with overallStatus gray", str(k['config.instanceUuid']), str(k['name']))
                            # build a candidate list of instances to reload to get rid of their gray overallStatus
                            # i have just learend that reloading gray instances the way i do it will not work, so stick with the alert for now
                            #self.instance_reload_candidates.add(k['config.instanceUuid'])
                            self.gauge_value_vcenter_instance_state_gray += 1

                    # check if the instanceUuid setting is proper: it should be the same as the uuid in the name
                    # only consider instances which have an openstack uuid in their name
                    if instancename_uuid_search_result and instancename_uuid_search_result.group(0):
                        if k['config.instanceUuid'] != instancename_uuid_search_result.group(0):
                            #log.warn("- PLEASE CHECK MANUALLY - instanceUuid to instance name mismatch for shadow vm: instanceUuid=%s,\
                            # uuid from instance name='%s'", k['config.instanceUuid'], instancename_uuid_search_result.group(0))
                            self.gauge_value_vcenter_volume_uuid_mismatch += 1
                            if self.cinder_os_volume_status.get(str(instancename_uuid_search_result.group(0))):
                                if not self.cinder_os_volume_status.get(str(k['config.instanceUuid'])):
                                    if not self.interactive:
                                        log.warn("- PLEASE CHECK MANUALLY - instanceUuid to instance name mismatch for shadow vm with instanceUuid not in cinder: instanceUuid=%s, uuid from instance name=%s, instance name='%s'", k['config.instanceUuid'], instancename_uuid_search_result.group(0), str(k['config.name']))
                                    pass
                                    # we no longer do the below renaming as it seems to be not safe in some situations
                                    # # do the instance uuid fixing only in non interactive mode
                                    # if not self.interactive:
                                    #     my_volume_status = self.cinder_db_get_volume_status(my_volume_uuid)
                                    #     if my_volume_uuid and (my_volume_status in ['backing-up','restoring-backup','maintenance']):
                                    #         log.info("- plan: renaming instanceUuid %s to uuid %s extracted from instance name ('%s') - delayed as the attached volume %s is in state '%s'", \
                                    #             str(k['config.instanceUuid']),str(instancename_uuid_search_result.group(0)), str(k['config.name']), str(my_volume_uuid), \
                                    #                 str(my_volume_status))
                                    #     else:
                                    #         self.vc_rename_instance_uuid(str(k['config.instanceUuid']),str(instancename_uuid_search_result.group(0)))
                                    #         self.gauge_value_vcenter_volume_uuid_adjustment += 1
                                else:
                                    # do the consistency check logging only in non interactive mode
                                    if not self.interactive:
                                        log.warn("- PLEASE CHECK MANUALLY - instanceUuid to instance name mismatch for shadow vm with instanceUuid still in cinder: instanceUuid=%s, uuid from instance name=%s, instance name='%s'", k['config.instanceUuid'], instancename_uuid_search_result.group(0), str(k['config.name']))
                            else:
                                # do the consistency check logging only in non interactive mode
                                if not self.interactive:
                                    log.warn("- PLEASE CHECK MANUALLY - instanceUuid to instance name mismatch for shadow vm with instance name uuid not in cinder: instanceUuid=%s, uuid from instance name=%s, instance name='%s'", k['config.instanceUuid'], instancename_uuid_search_result.group(0), str(k['config.name']))
                else:
                    log.warn("- PLEASE CHECK MANUALLY - instance without hardware - this should not happen!")
                # get the volume attachment which nova has written into the extraConfig
                # this should always point to the proper shadow vm even if thigs are not ok anymore
                # here (i.e. for comparing against openstack) the uuid in the key is relevant
                for j in k.get('config.extraConfig', []):
                    match = re.search(r"^volume-(.*)", j.key)
                    if not match:
                        continue
                    # important: we are checking the value of the extraConfig entry against the backing uuid here!
                    # i.e. for checking against the vcenter the value is relevant
                    if str(j.value) not in backing_uuid_list:
                        # mark it for the double check in the next loop run
                        new_vcenter_instance_without_backinguuid_for_volume[j.value] = k['config.instanceUuid']
                        # check that we did see this during the last run already
                        if self.old_vcenter_instance_without_backinguuid_for_volume.get(j.value) == k['config.instanceUuid']:
                            self.gauge_value_vcenter_extraconfig_backinguuid_missing += 1
                            if not self.interactive:
                                log.warn("- PLEASE CHECK MANUALLY - no backing uuid found for extraConfig volume uuid value {} on instance {}".format(str(j.value),str(k['config.instanceUuid'])))
                    else:
                        # remove all volume uuids we were able to map in extraConfig
                        backing_uuid_list.remove(str(j.value))
                    # map attached volume id to instance uuid - used later
                    self.vc_server_uuid_with_mounted_volume[str(match.group(1))] = k['config.instanceUuid']
                    # map attached volume id to instance name - used later for more detailed logging
                    self.vc_server_name_with_mounted_volume[str(match.group(1))] = k['config.name']
                    has_volume_attachments[k['config.instanceUuid']] = True
                    # some debugging code just in case
                    log.debug("==> key: {} - value: {} - match: {} - instance: {}".format(str(j.key), str(j.value), str(match.group(1)),str(k['config.instanceUuid'])))
                    # warn if key and value does not match here, which should not happen
                    if str(j.value) != str(match.group(1)) and not self.interactive:
                        log.warn("- PLEASE CHECK MANUALLY - key and value uuid not matching for extraConfig volume entry - key: {} - value: {} - instance: {}".format(str(j.key),str(j.key),str(k['config.instanceUuid'])))
                # we should not have any backing uuids left which could not be mapped against extraConfig
                if backing_uuid_list:
                    # other print the ones out which could not be mapped
                    for l in backing_uuid_list:
                        # only do this for real openstack vms and not shadow vms
                        if openstack_re.match(k.get('config.annotation', 'no_annotation')):
                            # mark it for the double check in the next loop run
                            new_vcenter_instance_without_extraconfig_for_volume[l] = k['config.instanceUuid']
                            # check that we did see this during the last run already
                            if self.old_vcenter_instance_without_extraconfig_for_volume.get(l) == k['config.instanceUuid']:
                                self.gauge_value_vcenter_backinguuid_extraconfig_missing += 1
                                if not self.interactive:
                                    log.warn("- PLEASE CHECK MANUALLY - volume with backing uuid {} on instance {} has no mapping in extraConfig".format(l,k['config.instanceUuid']))
                if not has_volume_attachments.get(k['config.instanceUuid']):
                    self.vcenter_instances_without_mounts[k['config.instanceUuid']] = k['config.name']

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
                                self.vc_all_volumes.append(j.backing.uuid)
                                self.vc_all_volumes.append(k.get('config.instanceUuid'))
                                # vc_all_volumes.append(k.get('config.instanceUuid'))
                                log.debug("==> shadow vm mount - instance: %s - volume / backing uuid: %s", str(k['config.instanceUuid']), str(j.backing.uuid))
                else:
                    log.warn("- PLEASE CHECK MANUALLY - instance without hardware - this should not happen!")

        # check if the mounts we got from the extra options differed from the ones based on filenames
        for k in self.vc_server_uuid_with_mounted_volume:
            log.debug("==> mapping - volume: {} - instance: {}".format(str(k),self.vc_server_uuid_with_mounted_volume.get(k)))
            log.debug("====> fs mapping - instance: {}".format(self.vc_server_uuid_with_mounted_volume_fnb.get(k)))
            if self.vc_server_uuid_with_mounted_volume.get(k) != self.vc_server_uuid_with_mounted_volume_fnb.get(k) and not self.interactive:
                log.warn("- PLEASE CHECK MANUALLY - volume uuid mismatch for volume (from extraConfig volume entry) {} - instance: {} - shadow vm uuid via vmdk filename in backing: {} - vmdk filename for backing uuid: {}".format(str(k),str(self.vc_server_uuid_with_mounted_volume.get(k)),str(self.vc_server_uuid_with_mounted_volume_fnb.get(k)),str(self.vc_vmdk_filename_for_backing_uuid.get(k))))

        self.old_vcenter_instance_without_backinguuid_for_volume = new_vcenter_instance_without_backinguuid_for_volume
        self.old_vcenter_instance_without_extraconfig_for_volume = new_vcenter_instance_without_extraconfig_for_volume

        return True

    # return the database connection string from the config file
    def get_db_url(self, config_file):

        parser = configparser.ConfigParser()
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

            self.cinder_engine = create_engine(db_url, pool_pre_ping=True, pool_recycle=300, echo_pool=True)
            # do not connect explicitely - the sqlalchemy pooling will take care of this for us
            #self.cinder_connection = self.cinder_engine.connect()
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
        # do not connect explicitely - the sqlalchemy pooling will take care of this for us
        #self.cinder_connection.close()

    def cinder_db_get_info(self):
        self.cinder_db_get_volume_attach_status()
        self.cinder_db_get_volume_attachment_attach_status()

    # get the volume status for one single volume
    def cinder_db_get_volume_status(self, volume_uuid):

        cinder_db_volumes_t = Table('volumes', self.cinder_metadata, autoload=True)
        cinder_db_volume_status_q = select(columns=[cinder_db_volumes_t.c.status],whereclause=and_(cinder_db_volumes_t.c.id == volume_uuid, cinder_db_volumes_t.c.deleted == 0))

        result = cinder_db_volume_status_q.execute().fetchone()
        return result['status']

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
            volume_attachment_ids.append(attachment_id[0])

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

            self.nova_engine = create_engine(db_url, pool_pre_ping=True)
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

            # we are not getting the vc volumes and instances are running on from the project tags
            # as there might be cases there this is not true (i.e. blackbox tests etc.)
            # service = "keystone"
            # log.info("- INFO - getting project information from keystone")
            # temporary_project_list = list(self.os_conn.identity.projects())
            # if not temporary_project_list:
            #     raise RuntimeError('- PLEASE CHECK MANUALLY - did not get any projects back from the keystone api - this should in theory never happen ...')

            service = "cinder"
            log.info("- INFO - getting volume information from cinder")
            temporary_volume_list = list(self.os_conn.block_store.volumes(details=True, all_projects=1))
            if not temporary_volume_list:
                raise RuntimeError('- PLEASE CHECK MANUALLY - did not get any cinder volumes back from the cinder api - this should in theory never happen ...')
            service = "nova"
            log.info("- INFO - getting server information from nova")
            temporary_server_list = list(self.os_conn.compute.servers(details=True, all_projects=1))
            if not temporary_server_list:
                raise RuntimeError('- PLEASE CHECK MANUALLY - did not get any nova instances back from the nova api - this should in theory never happen ...')
            log.info("- INFO - getting aggregate information from nova")
            temporary_aggregate_list = list(self.os_conn.compute.aggregates())
            if not temporary_aggregate_list:
                raise RuntimeError('- PLEASE CHECK MANUALLY - did not get any nova aggregates back from the nova api - this should in theory never happen ...')

            # build dicts to map servers and volumes to the vc they are running on to filter by it later
            hosts_per_vc = dict()
            all_hosts_in_vc_aggregates = set()
            for aggregate in temporary_aggregate_list:
                if aggregate.name:
                    match = re.search(r"^vc-[a-z]-[0-9]$", aggregate.name)
                    if match:
                        hosts_per_vc[aggregate.name] = aggregate.hosts
                        # this one is needed for a comparision later
                        all_hosts_in_vc_aggregates.update(aggregate.hosts)

            # build a dict of servers and the vcenter instances they are running on
            host_from_server_uuid = dict()
            for server in temporary_server_list:
                if server.compute_host:
                    host_from_server_uuid[server.id] = server.compute_host

            # make sure that all vc hosts servers are running on are defined in some vc-* aggregate
            for host in set(host_from_server_uuid.values()):
                # make sure we only consider vcenter compute hosts (i.e. no ironic etc.)
                match = re.search(r"^nova-compute-bb", host)
                if match:
                    log.debug("==> server host %s matches", str(host))
                if (host not in all_hosts_in_vc_aggregates) and match:
                    log.error("- PLEASE CHECK MANUALLY - host %s has instances on it, but is not referenced in the vc-* aggregates!", host)
                    self.gauge_value_bb_not_in_aggregate += 1

            if self.gauge_value_bb_not_in_aggregate != 0:
                log.error("- PLEASE CHECK MANUALLY - some vc hosts seem to be not connected to vc-* aggregates - forcing dry-run mode!")
                self.dry_run = True

            # determine the vc-* aggregate per server
            vc_from_server_uuid = dict()
            for server in host_from_server_uuid:
                for vcenter in hosts_per_vc:
                    if host_from_server_uuid[server] in hosts_per_vc[vcenter]:
                        vc_from_server_uuid[server] = vcenter

            # determine the vc-* aggregate per volume
            vc_from_volume_uuid = dict()
            for volume in temporary_volume_list:
                if volume.host:
                    match = re.search(r"(vc-[a-z]-[0-9])", volume.host)
                    if match:
                        vc_from_volume_uuid[volume.id] = match.groups(1)[0]

            # we are not getting the vc volumes and instances are running on from the project tags
            # as there might be cases there this is not true (i.e. blackbox tests etc.)
            # # build a dict of the projects and their vcenters used to find the proper shard
            # project_in_shard = dict()
            # # build the az name from vc_short_name and vc_region_name because we have the az defined for
            # # volumes and instances and want to compare against that later - az = region name + letter (qa-de-1a)
            # az = str(self.vc_region_name()) + str(self.vc_short_name().split('-')[1])
            # for project in temporary_project_list:
            #     try:
            #         # check if the vcenter this nanny is connected to is in the shards tag list for each
            #         # project - if yes then assign the constructed az name to it in the dict to compare
            #         # against the az of the instances and volumes later - otherwise set it to the special
            #         # string "no_shards" if there is no project.tags defined (i.e. no shards enabled here)
            #         if project.tags and (self.vc_short_name() in project.tags):
            #             project_in_shard[project.id] = az
            #         if not project.tags:
            #             project_in_shard[project.id] = 'no_shard'
            #         # this will move to debug later
            #         log.debug("==> project %s - tags: %s)", project.id, str(project.tags))
            #     except Exception as e:
            #         # this will move to debug later
            #         log.debug("==> project %s most probably has no tags defined (exception %s)", project.id, str(e))

            for volume in temporary_volume_list:

                # we are not getting the vc volumes and instances are running on from the project tags
                # as there might be cases there this is not true (i.e. blackbox tests etc.)
                # # compare the az of the volume to the az value based on the shard tags above
                # log.debug('==> p: %s - p-sh: %s - v: %s - v-az: %s - vc: %s', volume.project_id, project_in_shard.get(volume.project_id), volume.id, volume.availability_zone.lower(), self.vcenter_name)
                # if (project_in_shard.get(volume.project_id) and (volume.availability_zone.lower() ==  project_in_shard.get(volume.project_id))) \
                #     or ((project_in_shard.get(volume.project_id) == 'no_shard') and (volume.availability_zone.lower() == self.vcenter_name)):

                # we only care about volumes from the vcenter (shard) this nanny is taking care of
                log.debug('==> p: %s - v: %s - v-vc: %s - vc-sn: %s', volume.project_id, volume.id, vc_from_volume_uuid.get(volume.id), self.vc_short_name())
                if vc_from_volume_uuid.get(volume.id) == self.vc_short_name():
                    self.cinder_os_all_volumes.append(volume.id)
                    log.debug("==> os_all_volumes added: %s",str(volume.id))
                    self.cinder_os_volume_status[volume.id] = volume.status
                    self.cinder_os_volume_project_id[volume.id] = volume.project_id
                    if volume.attachments:
                        for attachment in volume.attachments:
                            if self.cinder_os_servers_with_attached_volume.get(volume.id):
                                self.cinder_os_servers_with_attached_volume[volume.id].append(attachment['server_id'])
                            else:
                                self.cinder_os_servers_with_attached_volume[volume.id] = [attachment['server_id']]
                else:
                    log.debug("==> os_all_volumes not added: %s",str(volume.id))
            for server in temporary_server_list:

                # we are not getting the vc volumes and instances are running on from the project tags
                # as there might be cases there this is not true (i.e. blackbox tests etc.)
                # # compare the az of the server to the az value based on the shard tags above
                # log.debug('==> p: %s - p-sh: %s - s: %s - s-az: %s - vc: %s', server.project_id, project_in_shard.get(server.project_id), server.id, server.availability_zone.lower(), self.vcenter_name)
                # if (project_in_shard.get(server.project_id) and (server.availability_zone.lower() ==  project_in_shard.get(server.project_id))) \
                #     or ((project_in_shard.get(server.project_id) == 'no_shard') and (server.availability_zone.lower() == self.vcenter_name)):

                # we only care about instances from the vcenter (shard) this nanny is taking care of
                log.debug('==> p: %s - s: %s - s-vc: %s - vc-sn: %s', server.project_id, server.id, vc_from_server_uuid.get(server.id), self.vc_short_name())
                if vc_from_server_uuid.get(server.id) == self.vc_short_name():
                    self.nova_os_all_servers.append(server.id)
                    log.debug("==> os_all_servers added: %s",str(server.id))
                    if server.attached_volumes:
                        for attachment in server.attached_volumes:
                            if self.nova_os_volumes_attached_at_server.get(server.id):
                                self.nova_os_volumes_attached_at_server[server.id].append(attachment['id'])
                            else:
                                self.nova_os_volumes_attached_at_server[server.id] = [attachment['id']]
                            self.nova_os_servers_with_attached_volume[attachment['id']] = server.id
                else:
                    log.debug("==> os_all_servers not added: %s",str(server.id))

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
                self.volume_query=input('please enter a volume uuid (ctrl-c to exit): ')
            except KeyboardInterrupt:
                print("")
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
            log.debug("==> problem_fix_volume_status_nothing_attached does not apply")
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
            log.debug("==> problem_fix_volume_status_all_attached does not apply")
            return False

    def problem_fix_only_partially_attached(self):

        # TODO maybe even consider checking and setting the cinder_db_volume_attach_status
        # TODO maybe add the error state as well?
        if (self.cinder_os_volume_status.get(self.volume_query) in ['in-use', 'available', 'attaching', 'detaching', 'creating', 'deleting', 'reserved']):
        # be more conservative and only remove attachments in nova and cinder if they are detached in the vcenter
        #     something_attached = False
        #     something_not_attached = False
        #     if self.cinder_os_servers_with_attached_volume.get(self.volume_query):
        #         something_attached = True
        #     if self.nova_os_servers_with_attached_volume.get(self.volume_query):
        #         something_attached = True
        #     if self.vc_server_uuid_with_mounted_volume.get(self.volume_query):
        #         something_attached = True
        #     if not self.cinder_os_servers_with_attached_volume.get(self.volume_query):
        #         something_not_attached = True
        #     if not self.nova_os_servers_with_attached_volume.get(self.volume_query):
        #         something_not_attached = True
        #     if not self.vc_server_uuid_with_mounted_volume.get(self.volume_query):
        #         something_not_attached = True
        #     if something_attached and something_not_attached:
            if ((not self.vc_server_uuid_with_mounted_volume.get(self.volume_query)) and self.cinder_os_servers_with_attached_volume.get(self.volume_query)) \
                or ((not self.vc_server_uuid_with_mounted_volume.get(self.volume_query)) and self.nova_os_servers_with_attached_volume.get(self.volume_query)):
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
            elif self.vc_server_uuid_with_mounted_volume.get(self.volume_query) \
                    and (not self.cinder_os_servers_with_attached_volume.get(self.volume_query)) \
                    and self.nova_os_servers_with_attached_volume.get(self.volume_query):
                log.info("- plan: (dry-run-for-now) - try to bring back cinder volume attachment for volume %s", self.volume_query)
                self.gauge_value_no_autofix += 1
            elif self.vc_server_uuid_with_mounted_volume.get(self.volume_query) \
                    and self.cinder_os_servers_with_attached_volume.get(self.volume_query) \
                    and (not self.nova_os_servers_with_attached_volume.get(self.volume_query)):
                log.info("- plan: (dry-run-for-now) - try to bring back nova volume attachment for volume %s", self.volume_query)
                self.gauge_value_no_autofix += 1
            elif self.vc_server_uuid_with_mounted_volume.get(self.volume_query) \
                    and (not self.cinder_os_servers_with_attached_volume.get(self.volume_query)) \
                    and (not self.nova_os_servers_with_attached_volume.get(self.volume_query)):
                log.info("- PLEASE CHECK MANUALLY - volume %s attached in vcenter, but not attached in cinder and nova", self.volume_query)
                self.gauge_value_no_autofix += 1
            else:
                log.debug("==> problem_fix_only_partially_attached does not apply")
            return True

        else:
            log.debug("==> problem_fix_only_partially_attached does not apply")
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

    def problem_fix_reload_instance(self):
        # do nothing here for now
        # for i in self.instance_reload_candidates:
        #     if self.dry_run:
        #         log.info("- dry-run: reloading instance %s to fix zero size volume attached to it", str(i))
        #     else:
        #         log.info("- action: reloading instance %s to fix zero size volume attached to it", str(i))
        #         self.vc_reload_instance(i)
        return True

    def ask_user_yes_no(self):
        while True:
            yesno=input('do you want to do the above action(s) (y/n): ')
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
        self.gauge_value_vcenter_instance_name_mismatch = 0
        self.gauge_value_vcenter_volume_backinguuid_mismatch = 0
        self.gauge_value_vcenter_volume_uuid_mismatch = 0
        self.gauge_value_vcenter_volume_uuid_adjustment = 0
        self.gauge_value_vcenter_volume_uuid_missing = 0
        self.gauge_value_vcenter_backinguuid_extraconfig_missing = 0
        self.gauge_value_vcenter_extraconfig_backinguuid_missing = 0
        self.gauge_value_vcenter_volume_zero_size = 0
        self.gauge_value_vcenter_instance_state_gray = 0
        self.gauge_value_no_autofix = 0
        self.gauge_value_bb_not_in_aggregate = 0


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
                    log.debug("==> plan: fix volume %s in project %s in state 'attaching' for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_attaching_for_too_long[volume_uuid], iterations)
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
                    log.debug("==> plan: fix volume %s in project %s in state 'detaching' for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_detaching_for_too_long[volume_uuid], iterations)
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
                    log.debug("==> plan: fix volume %s in project %s in state 'creating' for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_creating_for_too_long[volume_uuid], iterations)
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
                    log.debug("==> plan: fix volume %s in project %s in state 'deleting' for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_deleting_for_too_long[volume_uuid], iterations)
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
                    log.debug("==> plan: fix volume %s in project %s in state 'reserved' for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_is_in_state_reserved[volume_uuid], iterations)
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
                        log.debug("==> plan: fix volume %s in project %s in state 'available' with attachments for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_available_with_attachments[volume_uuid], iterations)
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
                        log.debug("==> plan: fix volume %s in project %s in state 'available' with attachments for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_available_with_attachments[volume_uuid], iterations)
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
                        log.debug("==> plan: fix volume %s in project %s in state 'available' with attachments for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_available_with_attachments[volume_uuid], iterations)
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
                                log.debug("==> plan: fix volume %s in project %s in state 'in-use' without attachments for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_in_use_without_attachments[volume_uuid], iterations)
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
                        log.debug("==> PLEASE CHECK MANUALLY - volume %s in project %s is in state 'in-use' without some attachments (no vc) for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_in_use_without_some_attachments[volume_uuid], iterations)
                    elif self.cinder_volume_in_use_without_some_attachments.get(volume_uuid) < iterations:
                        self.cinder_volume_in_use_without_some_attachments[volume_uuid] += 1
                        log.info("- PLEASE CHECK MANUALLY - volume %s in project %s is in state 'in-use' without some attachments (no vc) for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_in_use_without_some_attachments[volume_uuid], iterations)
                    else:
                        self.gauge_value_cinder_volume_in_use_without_some_attachments += 1
                    continue
                if not self.nova_os_servers_with_attached_volume.get(volume_uuid):
                    if not self.cinder_volume_in_use_without_some_attachments.get(volume_uuid):
                        self.cinder_volume_in_use_without_some_attachments[volume_uuid] = 1
                        log.debug("==> PLEASE CHECK MANUALLY - volume %s in project %s is in state 'in-use' without some attachments (no nova) for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_in_use_without_some_attachments[volume_uuid], iterations)
                    elif self.cinder_volume_in_use_without_some_attachments.get(volume_uuid) < iterations:
                        self.cinder_volume_in_use_without_some_attachments[volume_uuid] += 1
                        log.info("- PLEASE CHECK MANUALLY - volume %s in project %s is in state 'in-use' without some attachments (no nova) for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_in_use_without_some_attachments[volume_uuid], iterations)
                    else:
                        self.gauge_value_cinder_volume_in_use_without_some_attachments += 1
                    continue
                if not self.cinder_os_servers_with_attached_volume.get(volume_uuid):
                    if not self.cinder_volume_in_use_without_some_attachments.get(volume_uuid):
                        self.cinder_volume_in_use_without_some_attachments[volume_uuid] = 1
                        log.debug("==> PLEASE CHECK MANUALLY - volume %s in project %s is in state 'in-use' without some attachments (no cinder) for too long (%s/%s)", volume_uuid, self.cinder_os_volume_project_id.get(volume_uuid), self.cinder_volume_in_use_without_some_attachments[volume_uuid], iterations)
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
        self.gauge_vcenter_instance_name_mismatch.set(self.gauge_value_vcenter_instance_name_mismatch)
        self.gauge_vcenter_volume_backinguuid_mismatch.set(self.gauge_value_vcenter_volume_backinguuid_mismatch)
        self.gauge_vcenter_volume_uuid_mismatch.set(self.gauge_value_vcenter_volume_uuid_mismatch)
        self.gauge_vcenter_volume_uuid_adjustment.set(self.gauge_value_vcenter_volume_uuid_adjustment)
        self.gauge_vcenter_volume_uuid_missing.set(self.gauge_value_vcenter_volume_uuid_missing)
        self.gauge_vcenter_backinguuid_extraconfig_missing.set(self.gauge_value_vcenter_backinguuid_extraconfig_missing)
        self.gauge_vcenter_extraconfig_backinguuid_missing.set(self.gauge_value_vcenter_extraconfig_backinguuid_missing)
        self.gauge_vcenter_volume_zero_size.set(self.gauge_value_vcenter_volume_zero_size)
        self.gauge_vcenter_instance_state_gray.set(self.gauge_value_vcenter_instance_state_gray)
        self.gauge_no_autofix.set(self.gauge_value_no_autofix)
        self.gauge_bb_not_in_aggregate.set(self.gauge_value_bb_not_in_aggregate)

    def run_tool(self):
        log.info("- INFO - connecting to the cinder db")
        self.cinder_db_connect()
        if not self.cinder_db_connection_ok():
            log.error("problems connecting to the cinder db")
            sys.exit(1)
        log.info("- INFO - connecting to the vcenter")
        self.vc_connect()
        # exit here in case we get problems connecting to the vcenter
        if not self.vc_connection_ok():
            log.error("- PLEASE CHECK MANUALLY - problems connecting to the vcenter - retrying in next loop run")
            sys.exit(1)
        log.info("- INFO - getting viewref from the vcenter")
        # exit here in case we get problems getting the viewref from the vcenter
        if not self.vc_get_viewref():
            log.error("- PLEASE CHECK MANUALLY - problems getting the viewref from the vcenter - retrying in next loop run")
            log.info("- INFO - disconnecting from the vcenter")
            self.vc_disconnect()
            sys.exit(1)
        # we connect to openstack first, as we need some of those values in the vcenter connect later
        log.info("- INFO - connecting to openstack")
        self.os_connect()
        # exit here in case we get problems connecting to openstack
        if not self.os_connection_ok():
            log.error("- PLEASE CHECK MANUALLY - problems connecting to openstack - retrying in next loop run")
            sys.exit(1)
        log.info("- INFO - getting information from openstack (this may take a moment - see the next lines for details)")
        # exit here in case we get problems getting data from openstack
        if not self.os_get_info():
            log.error("- PLEASE CHECK MANUALLY - problems getting data from openstack - retrying in next loop run")
            log.info("- INFO - disconnecting from openstack")
            self.os_disconnect()
            sys.exit(1)
        log.info("- INFO - getting information from the vcenter")
        # exit here in case we get problems getting data from openstack
        if not self.vc_get_info():
            log.error("- PLEASE CHECK MANUALLY - problems getting data from the vcenter - retrying in next loop run")
            log.info("- INFO - disconnecting from the vcenter")
            self.vc_disconnect()
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
        # reset dry_run flag to cmdline value in case we have forced it on in the last loop run
        self.dry_run = self.cmdline_dry_run
        if self.dry_run:
            log.info("- INFO - running in dry run mode")
        # clean the lists of canditates for uuid rewrite and instance reload
        self.instance_reload_candidates.clear()
        # reset gauge values to zero for this new loop run
        self.reset_gauge_values()
        log.info("- INFO - connecting to the cinder db")
        self.cinder_db_connect()
        if not self.cinder_db_connection_ok():
            log.error("- PLEASE CHECK MANUALLY - problems connecting to the cinder db - retrying in next loop run")
            return
        log.info("- INFO - connecting to vcenter")
        self.vc_connect()
        # stop this loop iteration here in case we get problems connecting to the vcenter
        if not self.vc_connection_ok():
            log.warn("- PLEASE CHECK MANUALLY - problems connecting to the vcenter - retrying in next loop run")
            return
        log.info("- INFO - getting viewref from the vcenter")
        # stop this loop iteration here in case we get problems getting the viewref from the vcenter
        if not self.vc_get_viewref():
            log.warn("- PLEASE CHECK MANUALLY - problems getting the viewref from the vcenter - retrying in next loop run")
            log.info("- INFO - disconnecting from the vcenter")
            self.vc_disconnect()
            return
        # we connect to openstack first, as we need some of those values in the vcenter connect later
        log.info("- INFO - connecting to openstack")
        self.os_connect()
        # stop this loop iteration here in case we get problems connecting to openstack
        if not self.os_connection_ok():
            log.warn("- PLEASE CHECK MANUALLY - problems connecting to openstack - retrying in next loop run")
            return
        log.info("- INFO - getting information from openstack (this may take a moment - see the next lines for details)")
        # stop this loop iteration here in case we get problems getting data from openstack
        if not self.os_get_info():
            log.warn("- PLEASE CHECK MANUALLY - problems getting data from openstack - retrying in next loop run")
            log.info("- INFO - disconnecting from openstack")
            self.os_disconnect()
            return
        log.info("- INFO - disconnecting from openstack")
        self.os_disconnect()
        log.info("- INFO - getting information from the vcenter")
        # stop this loop iteration here in case we get problems getting data from openstack
        if not self.vc_get_info():
            log.warn("- PLEASE CHECK MANUALLY - problems getting data from the vcenter - retrying in next loop run")
            log.info("- INFO - disconnecting from the vcenter")
            self.vc_disconnect()
            return
        # clean the list of canditate volume uuids which are supposed to be fixed automatically
        self.volume_attachment_fix_candidates.clear()
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
            log.debug("==> number of fix candidates: %s - max number for automatic fixing: %s", str(len(self.volume_attachment_fix_candidates)), str(self.max_automatic_fix))
            # TODO this is ugly - maybe better replace the self.volume_query in all the offer_problem fix
            # functions with a regular (non self) paramater
            for self.volume_query in self.volume_attachment_fix_candidates:
                self.problem_fixes()
        else:
            # TODO create a metric for this case we may alert on
            log.warn("- PLEASE CHECK MANUALLY - too many (more than %s) volume attachment inconsistencies - denying to fix them automatically", str(self.max_automatic_fix))
        # leave this disabled for now
        # log.info("- INFO - checking for instances with zero size disks and reload them (not implemented yet)")
        # self.problem_fix_reload_instance()
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
