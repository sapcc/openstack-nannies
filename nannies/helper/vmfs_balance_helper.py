#!/usr/bin/env python3
#
# Copyright (c) 2021 SAP SE
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

# -*- coding: utf-8 -*-
import re
import logging
import os

from helper.netapp import NetAppHelper
from helper.vcenter import *

from helper.openstack import *

from pyVim.task import WaitForTask

log = logging.getLogger(__name__)


class VM:
    """
    this is for a single vm
    """

    def __init__(self, vm_element):
        self.overallstatus = vm_element.get('overallStatus', None)
        self.name = vm_element.get('name', None)
        self.instanceuuid = vm_element.get('config.instanceUuid', None)
        self.hardware = vm_element.get('config.hardware', None)
        self.annotation = vm_element.get('config.annotation', None)
        self.runtime = vm_element.get('runtime', None)
        self.handle = vm_element.get('obj', None)

    def is_shadow_vm(self):
        """
        check if a given vm is a shadow vm\n
        return true or false
        """
        if self.hardware.memoryMB == 128 and self.hardware.numCPU == 1 and \
                self.runtime.powerState == 'poweredOff' and \
                not any(isinstance(dev, vim.vm.device.VirtualEthernetCard) for dev in self.hardware.device):
            number_of_disks = sum(isinstance(
                dev, vim.vm.device.VirtualDisk) for dev in self.hardware.device)
            if number_of_disks == 0:
                log.warning(
                    "- WARN - shadow vm {} without a disk".format(self.name))
                return False
            if number_of_disks > 1:
                log.warning(
                    "- WARN - shadow vm {} with more than one disk".format(self.name))
                return False
            return True
        else:
            return False

    def get_disksizes(self):
        """
        get disk sizes of all attached disks on a vm\n
        return a list of disk sizes in bytes
        """
        # return [dev.capacityInBytes for dev in self.hardware.device if isinstance(dev, vim.vm.device.VirtualDisk)]
        disksizes = []
        # find the disk device
        for dev in self.hardware.device:
            if isinstance(dev, vim.vm.device.VirtualDisk):
                disksizes.append(dev.capacityInBytes)
        return disksizes

    def get_total_disksize(self):
        """
        get total disk sizes of all attached disks on a vm\n
        return the total disk size in bytes
        """
        # return sum(dev.capacityInBytes for dev in self.hardware.device if isInstance(dev, vim.vm.device.VirtualDisk))
        return sum(self.get_disksizes())


class VMs:
    """
    this is for all vms we get from the vcenteri
    """

    def __init__(self, vc):
        self.elements = []
        self.vvol_shadow_vms_for_naaids = {}
        self.vmfs_shadow_vms_for_datastores = {}
        for vm_element in self.get_vms_dict(vc):
            # ignore instances without a config-hardware node
            if not vm_element.get('config.hardware'):
                log.warning(
                    "- WARN - instance {} has no config.hardware - skipping it!".format(vm_element.get('name', "no name")))
                continue
            # ignore instances which do not have overallStatus green
            if vm_element.get('overallStatus') == 'gray':
                log.debug(
                    "- DEBG - instance {} has gray overallStatus - skipping it!".format(vm_element.get('name', "no name")))
                continue
            self.elements.append(VM(vm_element))
        all_shadow_vm_handles = self.get_shadow_vms(
            [vm.handle for vm in self.elements])
        self.vvol_shadow_vms_for_naaids = self.get_vvol_shadow_vms_for_naaids(
            vc, all_shadow_vm_handles)
        self.vmfs_shadow_vms_for_datastores = self.get_vmfs_shadow_vms_for_datastores(
            vc, all_shadow_vm_handles)

    def get_vms_dict(self, vc):
        """
        get info about the vms from the vcenter\n
        return a dict of vms with the vm handles as keys
        """
        log.info("- INFO -  getting vm information from the vcenter")
        vm_view = vc.find_all_of_type(vc.vim.VirtualMachine)
        vms_dict = vc.collect_properties(vm_view, vc.vim.VirtualMachine,
                                         ['name', 'config.instanceUuid', 'config.annotation', 'config.hardware', 'runtime', 'overallStatus'], include_mors=True)
        return vms_dict

    # TODO: maybe the vm_handles can go and we do the get_shadow_vms inside
    def get_vvol_shadow_vms_for_naaids(self, vc, vm_handles):
        """
        get the shadow vms related to netapp naa ids (for vvols)\n
        return a dict of vm, capacity with the naa id as key
        """
        vvol_shadow_vms_for_naaids = {}
        for vm_handle in vm_handles:
            # iterate over all devices
            for device in vm_handle.hardware.device:
                # and filter out the virtual disks
                if not isinstance(device, vc.vim.vm.device.VirtualDisk):
                    continue
                # we are only interested in vvols here
                if device.backing.fileName.lower().startswith('[vvol_') and device.backing.backingObjectId:
                    # add the vm per backingObjectId to our dict
                    vvol_shadow_vms_for_naaids[device.backing.backingObjectId] = vm_handle

        return vvol_shadow_vms_for_naaids

    # TODO: this should maybe go into the DS object
    # TODO: maybe the vm_handles can go and we do the get_shadow_vms inside
    def get_vmfs_shadow_vms_for_datastores(self, vc, vm_handles):
        """
        get the shadow vms related to a ds (for vmfs)\n
        return a dict of vm, capacity with the ds name as key
        """
        vmfs_shadow_vms_for_datastores = {}
        ds_path_re = re.compile(r"^[(?P<ds>vmfs_.*)].*$")
        for vm_handle in vm_handles:
            # iterate over all devices
            for device in vm_handle.hardware.device:
                # and filter out the virtual disks
                if not isinstance(device, vc.vim.vm.device.VirtualDisk):
                    continue
                # we are only interested in vvols here
                if device.backing.fileName.lower().startswith('[vmfs_'):
                    # extract the ds name from the filename
                    # example filename name: "[vmfs_vc_a_0_p_ssd_bb001_001] 1234-some-volume-uuid-7890/1234-some-volume-uuid-7890.vmdk"
                    ds_path_re = re.compile(r"^\[(?P<ds>.*)\].*$")
                    ds = ds_path_re.match(device.backing.fileName)
                    if ds:
                        # add vm to our list of vms per ds
                        if not vmfs_shadow_vms_for_datastores.get(ds.group('ds')):
                            vmfs_shadow_vms_for_datastores[ds.group('ds')] = [vm_handle]
                        else:
                            vmfs_shadow_vms_for_datastores[ds.group('ds')].append(vm_handle)

        return vmfs_shadow_vms_for_datastores

    def get_by_handle(self, vm_handle):
        """
        get a vm object by its vc handle name
        """
        for vm in self.elements:
            if vm.handle == vm_handle:
                return vm
        else:
            return None

    def get_by_name(self, vm_name):
        """
        get a vm object by its name
        """
        for vm in self.elements:
            if vm.name == vm_name:
                return vm
        else:
           return None

    def get_by_instanceuuid(self, vm_instanceuuid):
        """
        get a vm object by its instanceuuid
        """
        for vm in self.elements:
            if vm.instanceuuid == vm_instanceuuid:
                return vm
        else:
            return None

    def get_shadow_vms(self, vm_handles):
        """
        get all shadow vms (i.e. volumes) for a list of vm handles\n
        returns a list of shadow vms
        """
        shadow_vms = []
        # iterate over the vms
        for vm in self.elements:
            if vm.handle in vm_handles and vm.is_shadow_vm():
                shadow_vms.append(vm)
        return shadow_vms


    def remove_vms_from_project_denylist(self, vc, project_denylist):
        """
        remove (shadow) vms which are related to volumes from a volume id denylist
        """
        if not project_denylist:
            return

        log.info("- INFO -  getting volume information for volumes on projects to be excluded from balancing")
        volume_id_denylist = []
        for project_id in project_denylist:
            volumes_in_denylist_project = os_get_volumes_for_project_id(project_id)
            if volumes_in_denylist_project:
                for v in volumes_in_denylist_project:
                    log.debug(f"- DEBG -    excluding cinder volume {v.id} as it is in project {project_id} from the project denylist")
                    volume_id_denylist.append(v.id)

        # reamove the (shadow) vms which are affected by< the project denylist
        shadow_vms_without_denylist_projects = []
        for vm in self.elements:
            if vm.instanceuuid not in volume_id_denylist:
                shadow_vms_without_denylist_projects.append(vm)
            else:
                log.info(f"- INFO -    excluding volume {vm.instanceuuid} as it is in a project from the project denylist")
        self.elements = shadow_vms_without_denylist_projects

        # recalculate these to exclude the ones from the project_denylist there too
        all_shadow_vm_handles = self.get_shadow_vms(
            [vm.handle for vm in self.elements])
        self.vvol_shadow_vms_for_naaids = self.get_vvol_shadow_vms_for_naaids(
            vc, all_shadow_vm_handles)
        self.vmfs_shadow_vms_for_datastores = self.get_vmfs_shadow_vms_for_datastores(
            vc, all_shadow_vm_handles)


class DS:
    """
    this is for a single ds
    """

    def __init__(self, ds_element):
        self.name = ds_element.get('name', None)
        self.overallstatus = ds_element.get('overallStatus', None)
        self.freespace = ds_element.get('summary.freeSpace', None)
        self.capacity = ds_element.get('summary.capacity', None)
        self.used = self.capacity - self.freespace
        if self.capacity and self.capacity >= 0:
            self.usage = (1 - self.freespace /
                            self.capacity) * 100
        else:
            self.usage = None
        self.vm_handles = ds_element.get('vm', None)
        self.handle = ds_element.get('obj', None)

    def is_below_usage(self, usage):
        """
        check if the ds usage is below the max usage given in the args\n
        returns true or false
        """
        if self.usage < usage:
            return True
        else:
            return False

    def is_above_usage(self, usage):
        """
        check if the ds usage is above the min usage given in the args\n
        returns true or false
        """
        if self.usage > usage:
            return True
        else:
            return False

    def is_below_freespace(self, freespace):
        """
        check if the ds free space is above the min freespace given in the args\n
        returns true or false
        """
        if self.freespace < freespace * 1024**3:
            return True
        else:
            return False

    def add_shadow_vm(self, vm):
        """
        this adds a vm element to the ds and adjusts the space and usage values\n
        returns nothing
        """
        # remove vm size from freespace
        self.freespace -= vm.get_total_disksize()
        # add vm to vm list
        self.vm_handles.append(vm.handle)
        # recalc usage
        self.usage = (1 - self.freespace / self.capacity) * 100

    def remove_shadow_vm(self, vm):
        """
        this removes a vm element from the ds and adjusts the space and usage values\n
        returns nothing
        """
        # remove vm from vm list
        self.vm_handles.remove(vm.handle)
        # add vm size to freespace
        self.freespace += vm.get_total_disksize()
        # recalc usage
        self.usage = (1 - self.freespace / self.capacity) * 100


class DataStores:
    """
    this is for all datastores we get from the vcenter
    """

    def __init__(self, vc):
        self.elements = []
        for ds_element in self.get_datastores_dict(vc):
            # ignore datastores with zero capacity
            if ds_element.get('summary.capacity') == 0:
                log.warning(
                    "- WARN - ds {} has zero capacity - skipping it!".format(ds_element.get('name', "no name")))
                continue
            # ignore ds which do not have overallStatus green
            if ds_element.get('overallStatus') == 'gray':
                log.debug(
                    "- DEBG - ds {} has gray overallStatus - skipping it!".format(ds_element.get('name', "no name")))
                continue
            self.elements.append(DS(ds_element))

    @staticmethod
    def get_datastores_dict(vc):
        """
        get info about the datastores from the vcenter\n
        return a dict of datastores with the ds handles as keys
        """
        log.info("- INFO -  getting datastore information from the vcenter")
        ds_view = vc.find_all_of_type(vc.vim.Datastore)
        datastores_dict = vc.collect_properties(ds_view, vc.vim.Datastore,
                                                ['name', 'summary.freeSpace',
                                                 'summary.capacity', 'vm', 'overallStatus'],
                                                include_mors=True)
        return datastores_dict

    def get_by_handle(self, ds_handle):
        """
        get a ds object by its vc handle name
        """
        for ds in self.elements:
            if ds.handle == ds_handle:
                return ds
        else:
            return None

    def get_by_name(self, ds_name):
        """
        get a ds object by its name
        """
        for ds in self.elements:
            if ds.name == ds_name:
                return ds
        else:
            return None

    def vmfs_ds(self, ds_denylist=[], ds_type='ssd'):
        """
        filter for only vmfs ds and sort by size\n
        return a list of datastore elements
        """
        if ds_type == 'hdd':
            ds_name_regex_pattern = '^(?:vmfs_vc.*_hdd_).*'
        else:
            ds_name_regex_pattern = '^(?:vmfs_vc.*_ssd_).*'
        temp_list = []
        for ds in self.elements:
            # detect and handle wrongly named ds names
            ds_alt_name = re.sub(r'_vc-([a-z]+)-(\d+)_', r'_vc_\1_\2_', ds.name)
            # if ds_alt_name != ds.name:
            #     log.warning("- WARN - vc ds name {} should be {} - ignoring this ds for now - this should be fixed".format(ds.name, ds_alt_name))
            #    continue
            if not re.match(ds_name_regex_pattern, ds.name):
                continue
            if not (ds_denylist and ds.name in ds_denylist):
                temp_list.append(ds)
                continue
            if not (ds_denylist and ds_alt_name in ds_denylist):
                temp_list.append(ds)
                continue
        self.elements = temp_list

    def vvol_ds(self, ds_denylist=[]):
        """
        filter for only vvol ds and sort by size\n
        return a list of datastore elements
        """
        ds_name_regex_pattern = '^(?:vVOL_.*)'
        self.elements = [ds for ds in self.elements if re.match(
            ds_name_regex_pattern, ds.name) and not (ds_denylist and ds.name in ds_denylist)]

    def sort_by_usage(self, ds_weight=None):
        """
        sort ds by their usage, optional with a weight per ds\n
        return a list of datastore elements
        """
        if not ds_weight:
            ds_weight = {}
        self.elements.sort(key=lambda element: element.usage *
                           ds_weight.get(element.name, 1), reverse=True)

    def get_overall_capacity(self):
        """
        calculate the total capacity of all ds\n
        return the overall capacity in bytes
        """
        overall_capacity = sum(ds.capacity for ds in self.elements)
        return overall_capacity

    def get_overall_freespace(self):
        """
        calculate the total free space of all ds\n
        return the overall free space in bytes
        """
        overall_freespace = sum(ds.freespace for ds in self.elements)
        return overall_freespace

    def get_overall_average_usage(self):
        """
        calculate the average usage of all ds\n
        return the average usage in %
        """
        overall_average_usage = (
            1 - self.get_overall_freespace() / self.get_overall_capacity()) * 100
        return overall_average_usage


class NAAggr:
    """
    this is for a single netapp aggregate
    """

    def __init__(self, naaggr_element, parent):
        self.name = naaggr_element['name']
        self.host = naaggr_element['host']
        self.usage = naaggr_element['usage']
        self.capacity = naaggr_element['capacity']
#        self.luns=naaggr_element['luns']
        self.parent = parent
        self.fvols = [
            fvol for fvol in parent.na_fvol_elements if fvol.aggr == self.name]
        self.luns = []
        for fvol in self.fvols:
            luns = [lun for lun in parent.na_lun_elements if lun.fvol == fvol.name]
            self.luns.extend(luns)

    def add_shadow_vm_lun(self, lun):
        """
        this adds a lun to the aggr and adjusts the space and usage values\n
        returns nothing
        """
        # add lun size to used size
        used = self.usage * self.capacity / 100
        used += lun.used
        # add lun to lun list
        self.luns.append(lun)
        # recalc usage
        self.usage = (used / self.capacity) * 100

    def remove_shadow_vm_lun(self, lun):
        """
        this removes a lun from the aggr and adjusts the space and usage values\n
        returns nothing
        """
        # remove lun size from used size
        used = self.usage * self.capacity / 100
        used -= lun.used
        # remove lun from lun list
        self.luns.remove(lun)
        # recalc usage
        self.usage = (used / self.capacity) * 100

class NAFvol:
    """
    this is for a single netapp flexvol
    """

    def __init__(self, nafvol_element, parent):
        self.name = nafvol_element['name']
        self.host = nafvol_element['host']
        self.aggr = nafvol_element['aggr']
        self.capacity = nafvol_element['capacity']
        self.used = nafvol_element['used']
        self.usage = nafvol_element['usage']
        self.type = nafvol_element['type']
        self.parent = parent
        self.luns = [
            lun for lun in parent.na_lun_elements if lun.fvol == self.name]


class NALun:
    """
    this is for a single netapp lun
    """

    def __init__(self, nalun_element, parent):
        self.fvol = nalun_element['fvol']
        self.host = nalun_element['host']
        self.used = nalun_element['used']
        self.path = nalun_element['path']
        self.comment = nalun_element['comment']
        self.name = nalun_element['name']
        self.type = nalun_element['type']
        self.parent = parent


class NA:
    """
    this is for a single netapp
    """

    def __init__(self, na_element, na_user, na_password):
        self.na_aggr_elements = []
        self.na_fvol_elements = []
        self.na_lun_elements = []
        self.host = na_element['host']
        self.vc = na_element['vc']

        log.info("- INFO - connecting to netapp %s", self.host)
        self.nh = NetAppHelper(
            host=self.host, user=na_user, password=na_password)
        na_version = self.nh.get_single("system-get-version")
        if not na_version:
            log.warning("- WARN - giving up on this netapp for now")
            return None
        log.info("- INFO -  {} is on version {}".format(self.host,
                                                        na_version['version']))

        lun_list = self.get_lun_info(self.nh, [])
        for lun in lun_list:
            nalun_element = {}
            nalun_element['fvol'] = lun['fvol']
            nalun_element['host'] = lun['host']
            nalun_element['used'] = lun['used']
            nalun_element['path'] = lun['path']
            nalun_element['comment'] = lun['comment']
            nalun_element['name'] = lun['name']
            nalun_element['type'] = lun['type']
            nalun_element['parent'] = self
            lun_instance = NALun(nalun_element, self)
            self.na_lun_elements.append(lun_instance)

        fvol_list = self.get_fvol_info(self.nh, [])
        for fvol in fvol_list:
            nafvol_element = {}
            nafvol_element['name'] = fvol['name']
            nafvol_element['host'] = fvol['host']
            nafvol_element['aggr'] = fvol['aggr']
            nafvol_element['capacity'] = fvol['capacity']
            nafvol_element['used'] = fvol['used']
            nafvol_element['usage'] = fvol['usage']
            nafvol_element['type'] = fvol['type']
            nafvol_element['parent'] = self
            fvol_instance = NAFvol(nafvol_element, self)
            self.na_fvol_elements.append(fvol_instance)

        aggr_list = self.get_aggr_info(self.nh, [])
        for aggr in aggr_list:
            naaggr_element = {}
            naaggr_element['name'] = aggr['name']
            naaggr_element['host'] = aggr['host']
            naaggr_element['usage'] = aggr['usage']
            naaggr_element['capacity'] = aggr['capacity']
            naaggr_element['parent'] = self
            aggr_instance = NAAggr(naaggr_element, self)
            self.na_aggr_elements.append(aggr_instance)

    def get_aggr_info(self, nh, aggr_denylist):
        """
        get aggregate info from the netapp
        """
        aggr_info = []
        # get aggregates
        for aggr in nh.get_aggregate_usage():
            naaggr_element = {}
            # print info for aggr_denylisted aggregates
            if aggr['aggregate-name'] in aggr_denylist:
                log.info("- INFO -   aggregate {} is aggr_denylist'ed via cmdline"
                         .format(aggr['aggregate-name']))

            if aggr['aggr-raid-attributes']['is-root-aggregate'] == 'false' \
                    and aggr['aggregate-name'] not in aggr_denylist:
                log.debug("- DEBG -   aggregate {} of size {:.0f} gb is at {}% utilization"
                          .format(aggr['aggregate-name'],
                                  int(aggr['aggr-space-attributes']
                                      ['size-total']) / 1024**3,
                                  aggr['aggr-space-attributes']['percent-used-capacity']))
                naaggr_element['name'] = aggr['aggregate-name']
                naaggr_element['host'] = self.host
                naaggr_element['usage'] = int(
                    aggr['aggr-space-attributes']['percent-used-capacity'])
                naaggr_element['capacity'] = int(
                    aggr['aggr-space-attributes']['size-total'])
                aggr_info.append(naaggr_element)

        return aggr_info

    def get_fvol_info(self, nh, fvol_denylist):
        """
        get flexvol info from the netapp
        """
        fvol_info = []
        # get flexvols
        for fvol in nh.get_volume_usage():
            nafvol_element = {}
            # print info for fvol_denylisted flexvols
            if fvol['volume-id-attributes']['name'] in fvol_denylist:
                log.info("- INFO -   flexvol {} is fvol_denylist'ed via cmdline"
                         .format(fvol['volume-id-attributes']['name']))

            if fvol['volume-id-attributes']['name'].lower().startswith('vv'):
                nafvol_element['type'] = 'vvol'
            if fvol['volume-id-attributes']['name'].lower().startswith('vmfs'):
                nafvol_element['type'] = 'vmfs'
            if nafvol_element.get('type') \
                    and fvol['volume-id-attributes']['name'] not in fvol_denylist:
                log.debug("- DEBG -   flexvol {} on {} of size {:.0f} gb of a total size {:.0f} gb"
                          .format(fvol['volume-id-attributes']['name'],
                                  fvol['volume-id-attributes']['containing-aggregate-name'],
                                  int(fvol['volume-space-attributes']
                                      ['size-used']) / 1024**3,
                                  int(fvol['volume-space-attributes']['size-total']) / 1024**3))
                nafvol_element['name'] = fvol['volume-id-attributes']['name']
                nafvol_element['host'] = self.host
                nafvol_element['aggr'] = fvol['volume-id-attributes']['containing-aggregate-name']
                nafvol_element['capacity'] = int(
                    fvol['volume-space-attributes']['size-total'])
                nafvol_element['used'] = int(
                    fvol['volume-space-attributes']['size-used'])
                nafvol_element['usage'] = nafvol_element['used'] / \
                    nafvol_element['capacity'] * 100
                fvol_info.append(nafvol_element)

        return fvol_info

    def get_lun_info(self, nh, lun_denylist):
        """
        get lun info from the netapp
        """
        lun_info = []
        # for vvols
        naa_path_re = re.compile(r"^/vol/.*/(?P<name>naa\..*)\.vmdk$")
        # for vmfs
        ds_path_re = re.compile(r"^/vol/vmfs.*/(?P<name>vmfs_.*)$")
        # get luns
        for lun in nh.get_luns():
            nalun_element = {}
            path_match_vvol = naa_path_re.match(lun['path'])
            path_match_vmfs = ds_path_re.match(lun['path'])
            if not path_match_vvol and not path_match_vmfs:
                continue
            if path_match_vvol:
                nalun_element['type'] = 'vvol'
                path_match = path_match_vvol
            if path_match_vmfs:
                nalun_element['type'] = 'vmfs'
                path_match = path_match_vmfs

            # print info for lun_denylisted luns
            if path_match.group('name') in lun_denylist:
                log.info("- INFO -   lun {} is lun_denylist'ed via cmdline"
                         .format(path_match.group('name')))
            else:
                log.debug("- DEBG -   lun {} on flexvol {} of size {:.0f} gb"
                          .format(path_match.group('name'),
                                  lun['volume'],
                                  int(lun['size-used']) / 1024**3))
                nalun_element['fvol'] = lun['volume']
                nalun_element['host'] = self.host
                nalun_element['used'] = int(lun['size-used'])
                nalun_element['path'] = lun['path']
                nalun_element['comment'] = lun['comment']
                nalun_element['name'] = path_match.group('name')
                lun_info.append(nalun_element)

        return lun_info


class NAs:
    """
    this is for all netapps connected to the vcenter
    """

    def __init__(self, vc, na_user, na_password, region, na_denylist=[]):
        self.elements = []

        log.info("- INFO -  getting information from the netapps")

        na_hosts = self.get_na_hosts(vc, region)

        for na_host in na_hosts:
            if na_denylist and na_host.split('.')[0] in na_denylist:
                log.info(f"- INFO -  excluding netapp {na_host} as it is on the netapp denylist")
                continue
            na_element = {}
            na_element['host'] = na_host
            na_element['vc'] = vc
            self.elements.append(NA(na_element, na_user, na_password))

    def get_na_hosts(self, vc, region):
        """
        get all netapp hosts connected to a vc
        """
        na_hosts_set = set()
        for ds_element in DataStores.get_datastores_dict(vc):
            ds_name = ds_element['name'].lower()
            # vmfs case
            if ds_name.startswith("vmfs_vc"):
                # example for the pattern: vmfs_vc_a_0_p_ssd_bb123_004
                #                      or: vmfs_vc-a_0_p_ssd_bb123_004
                ds_name_regex_pattern = '^(?:vmfs_vc(-|_).*_ssd)_bb(?P<bb>\d+)_\d+$'
                m = re.match(ds_name_regex_pattern, ds_name)
                if m:
                    bbnum = int(m.group('bb'))
                    # one of our netapps is inconsistent in its naming - handle this here
                    if bbnum == 56:
                        stnpa_num = 0
                    else:
                        stnpa_num = 1
                    # e.g. stnpca1-bb123.cc.<region>.cloud.sap - those are the netapp cluster addresses (..np_c_a1..)
                    netapp_name = "stnpca{}-bb{:03d}.cc.{}.cloud.sap".format(
                        stnpa_num, bbnum, region)
                    na_hosts_set.add(netapp_name)
                    continue
                # example for the pattern: vmfs_vc_a_0_p_hdd_bb123_004
                #                      or: vmfs_vc-a_0_p_hdd_bb123_004
                ds_name_regex_pattern = '^(?:vmfs_vc(-|_).*_hdd)_bb(?P<bb>\d+)_\d+$'
                m = re.match(ds_name_regex_pattern, ds_name)
                if m:
                    bbnum = int(m.group('bb'))
                    # one of our netapps is inconsistent in its naming - handle this here
                    if bbnum == 56:
                        stnpa_num = 0
                    else:
                        stnpa_num = 1
                    # e.g. stnpca1-bb123.cc.<region>.cloud.sap - those are the netapp cluster addresses (..np_c_a1..)
                    netapp_name = "stnpca{}-bb{:03d}.cc.{}.cloud.sap".format(
                        stnpa_num, bbnum, region)
                    na_hosts_set.add(netapp_name)
                    continue
                # example for the pattern: vmfs_vc_a_0_p_ssd_stnpca1-st123_004
                #                      or: vmfs_vc-a_0_p_ssd_stnpca1-st123_004
                ds_name_regex_pattern = '^(?:vmfs_vc(-|_).*_ssd)_(?P<stname>.*)_\d+$'
                m = re.match(ds_name_regex_pattern, ds_name)
                if m:
                    # hack to only include the storage really wanted
                    is_stnpc = re.match(".*stnpc.*", m.group('stname'))
                    if not is_stnpc:
                        continue
                    # e.g. stnpca1-st123.cc.<region>.cloud.sap - those are the netapp cluster addresses (..np_c_a1..)
                    netapp_name = "{}.cc.{}.cloud.sap".format(
                        str(m.group('stname')).replace('_', '-'), region)
                    na_hosts_set.add(netapp_name)
                    continue
                # example for the pattern: vmfs_vc_a_0_p_hdd_stnpca1-st123_004
                #                      or: vmfs_vc-a_0_p_hdd_stnpca1-st123_004
                ds_name_regex_pattern = '^(?:vmfs_vc(-|_).*_hdd)_(?P<stname>.*)_\d+$'
                m = re.match(ds_name_regex_pattern, ds_name)
                if m:
                    # hack to only include the storage really wanted
                    is_stnpc = re.match(".*stnpc.*", m.group('stname'))
                    if not is_stnpc:
                        continue
                    # e.g. stnpca1-st123.cc.<region>.cloud.sap - those are the netapp cluster addresses (..np_c_a1..)
                    netapp_name = "{}.cc.{}.cloud.sap".format(
                        str(m.group('stname')).replace('_', '-'), region)
                    na_hosts_set.add(netapp_name)
                    continue
            # vvol cases
            if ds_name.startswith("vvol_bb"):
                # example for the pattern: vvol_bb123
                m = re.match("^(?:vvol)_bb(?P<bb>\d+)$", ds_name)
                if m:
                    bbnum = int(m.group('bb'))
                    # one of our netapps is inconsistent in its naming - handle this here
                    if bbnum == 56:
                        stnpa_num = 0
                    else:
                        stnpa_num = 1
                    # e.g. stnpca1-bb123.cc.<region>.cloud.sap - those are the netapp cluster addresses (..np_c_a1..)
                    netapp_name = "stnpca{}-bb{:03d}.cc.{}.cloud.sap".format(stnpa_num, bbnum, region)
                    # build a list of netapps
                    na_hosts_set.add(netapp_name)
                    continue
            if ds_name.startswith("vvol_stnpc"):
                # example for the pattern: vVOL_stnpca3_st030
                m = re.match("^(?:vvol)_(?P<stname>.*)$", ds_name)
                if m:
                    # e.g. stnpca3-st030.cc.<region>.cloud.sap - those are the netapp cluster addresses (..np_c_a3..)
                    netapp_name = "{}.cc.{}.cloud.sap".format(str(m.group('stname')).replace('_','-'), region)
                    # build a list of netapps
                    na_hosts_set.add(netapp_name)

        return sorted(na_hosts_set)

    def get_aggr_by_name(self, na_host, aggr_name):
        """
        get a aggr object by its name
        """
        for na in self.elements:
            if na.host == na_host:
                for aggr in na.na_aggr_elements:
                    if aggr.name == aggr_name:
                        return aggr
                else:
                    return None
            else:
                continue
        else:
            return None

    def get_fvol_by_name(self, na_host, fvol_name):
        """
        get a fvol object by its name
        """
        for na in self.elements:
            if na.host == na_host:
                for fvol in na.na_fvol_elements:
                    if fvol.name == fvol_name:
                        return fvol
                else:
                    return None
            else:
                continue
        else:
            return None

    def get_lun_by_name(self, na_host, lun_name):
        """
        get a lun object by its name
        """
        for na in self.elements:
            if na.host == na_host:
                for lun in na.na_lun_elements:
                    if lun.name == lun_name:
                        return lun
                else:
                    return None
            else:
                continue
        else:
            return None


def sanity_checks(least_used_ds, most_used_ds, min_usage, max_usage, min_freespace, min_max_difference):
    """
    make sure least and most used ds are still within sane limits
    """
    if most_used_ds.is_below_usage(max_usage):
        log.info("- INFO - most used ds {} with usage {:.1f}% is below the max usage limit of {:.1f}% - nothing left to be done".format(
            most_used_ds.name, most_used_ds.usage, max_usage))
        return False
    if least_used_ds.is_above_usage(min_usage):
        log.info("- INFO - least used ds {} with usage {:.1f}% is above the min usage limit of {:.1f}% - nothing can be done".format(
            least_used_ds.name, least_used_ds.usage, min_usage))
        return False
    if least_used_ds.is_below_freespace(min_freespace):
        log.info("- INFO - least used ds {} with free space {:.0f}G is below the min free space limit of {:.0f}G - nothing can be done".format(
            least_used_ds.name, least_used_ds.freespace / 1024**3, min_freespace))
        return False
    if (most_used_ds.usage - least_used_ds.usage) < min_max_difference:
        log.info("- INFO - usages of most used ds {} and least used ds {} are less than {}% apart - nothing can be done".format(
            most_used_ds.name, least_used_ds.name, min_max_difference))
        return False
    return True


def sanity_checks_lite(least_used_ds, most_used_ds, min_freespace, min_max_difference):
    """
    make sure least and most used ds are still within sane limits
    """
    if least_used_ds.is_below_freespace(min_freespace):
        log.info("- INFO - least used ds {} with free space {:.0f}G is below the min free space limit of {:.0f}G - nothing can be done".format(
            least_used_ds.name, least_used_ds.freespace / 1024**3, min_freespace))
        return False
    if (most_used_ds.usage - least_used_ds.usage) < min_max_difference:
        log.info("- INFO - usages of most used ds {} and least used ds {} are less than {}% apart - nothing can be done".format(
            most_used_ds.name, least_used_ds.name, min_max_difference))
        return False
    return True


def sanity_checks_liter(least_used_ds, most_used_ds, min_freespace, min_max_difference):
    """
    make sure least and most used ds are still within sane limits
    """
    if least_used_ds.is_below_freespace(min_freespace):
        log.info("- INFO - least used ds {} with free space {:.0f}G is below the min free space limit of {:.0f}G - nothing can be done".format(
            least_used_ds.name, least_used_ds.freespace / 1024**3, min_freespace))
        return False
    return True


def sort_vms_by_total_disksize(vms):
    """
    sort vms by disk size from adding up the sizes of their attached disks
    """
    return sorted(vms, key=lambda vm: vm.get_total_disksize(), reverse=True)


def move_vmfs_shadow_vm_from_ds_to_ds(ds1, ds2, vm):
    """
    suggest a move of a vm from one vmfs ds to another and adjust ds usage values accordingly
    """
    # remove vm from source ds
    source_usage_before = ds1.usage
    ds1.remove_shadow_vm(vm)
    source_usage_after = ds1.usage
    # add the vm to the target ds
    target_usage_before = ds2.usage
    ds2.add_shadow_vm(vm)
    target_usage_after = ds2.usage
    # for now just print out the move . later: do the actual move
    log.info(
        "- INFO - move vm {} ({:.0f}G) from ds {} to ds {}".format(vm.name, vm.get_total_disksize() / 1024**3, ds1.name, ds2.name))
    log.info(
        "- INFO -  source ds: {:.1f}% -> {:.1f}% target ds: {:.1f}% -> {:.1f}%".format(source_usage_before, source_usage_after, target_usage_before, target_usage_after))
    log.info("- CMND -  svmotion_cinder_v2.py {} {}".format(vm.name, ds2.name))


def move_vvol_shadow_vm_from_aggr_to_aggr(ds_info, aggr1, aggr2, lun, vm):
    """
    suggest a move of a vm from one na aggr to another and adjust aggr and ds usage values accordingly
    """
    # IMPORTANT: we can only keep track about the state for the aggr and the ds
    #            and not for the fvol as we do not know onto which fvol the na
    #            will move the lun
    # remove vm from source aggr
    source_aggr_usage_before = aggr1.usage
    aggr1.remove_shadow_vm_lun(lun)
    source_aggr_usage_after = aggr1.usage
    # add the vm to the target aggr
    target_aggr_usage_before = aggr2.usage
    aggr2.add_shadow_vm_lun(lun)
    target_aggr_usage_after = aggr2.usage
    # get the vc ds names based on the na aggr names
    ds1 = ds_info.get_by_name(aggr_name_to_ds_name(aggr1.host, aggr1.name))
    ds2 = ds_info.get_by_name(aggr_name_to_ds_name(aggr2.host, aggr2.name))
    # check that both really exist in the vcenter
    if not ds1:
        log.warning("- WARN - the aggr {} seems to be not connected in the vc (no ds)".format(aggr1.name))
        return
    if not ds2:
        log.warning("- WARN - the aggr {} seems to be not connected in the vc (no ds)".format(aggr2.name))
        return
    # remove vm from source ds
    source_ds_usage_before = ds1.usage
    ds1.remove_shadow_vm(vm)
    source_ds_usage_after = ds1.usage
    # add the vm to the target ds
    target_ds_usage_before = ds2.usage
    ds2.add_shadow_vm(vm)
    target_ds_usage_after = ds2.usage
    # for now just print out the move . later: do the actual move
    log.info(
        "- INFO - move vm {} ({:.0f}G) from aggr {} to aggr {}".format(vm.name, lun.used / 1024**3, aggr1.name, aggr2.name))
    log.info(
        "- INFO - move vm {} ({:.0f}G) from ds {} to ds {}".format(vm.name, vm.get_total_disksize() / 1024**3, ds1.name, ds2.name))
    log.info(
        "- INFO -  source aggr: {:.1f}% -> {:.1f}% target aggr: {:.1f}% -> {:.1f}%".format(source_aggr_usage_before, source_aggr_usage_after, target_aggr_usage_before, target_aggr_usage_after))
    log.info(
        "- INFO -  source ds: {:.1f}% -> {:.1f}% target ds: {:.1f}% -> {:.1f}%".format(source_ds_usage_before, source_ds_usage_after, target_ds_usage_before, target_ds_usage_after))
    log.info("- CMND -  svmotion_cinder_v2.py {} {}".format(vm.name, ds2.name))

def get_aggr_and_ds_stats(na_info, ds_info):
    """
    get usage stats for aggregates (netapp view) and vmfs ds on them (vc view)
    along the way create a weight per ds depending on how full the underlaying aggr is
    """
    ds_weight = {}

    for na in na_info.elements:
        log.info("- INFO -  netapp host: {}".format(na.host))
        for aggr in na.na_aggr_elements:
            log.info("- INFO -   aggregate: {}".format(aggr.name))
            log.info("- INFO -    aggregate usage: {:.2f}%".format(aggr.usage))
            ds_total_capacity = 0
            ds_total_used = 0
            # this defines how much we want to count in the weight - values are usually
            # around 50% +/- - so 500 here means count in the weight at about 10% (50/500)
            weight_level = 500
            for lun in aggr.luns:
                if re.match("^vmfs_vc.*$", lun.name) and ds_info.get_by_name(lun.name):
                    ds_total_capacity += ds_info.get_by_name(lun.name).capacity
                    ds_total_used += ds_info.get_by_name(lun.name).used
                    ds_weight[lun.name] = (aggr.usage + weight_level) / (((ds_info.get_by_name(
                        lun.name).used / ds_info.get_by_name(lun.name).capacity) * 100) + weight_level)
            if ds_total_capacity > 0:
                log.info(
                    "- INFO -    ds usage:        {:.2f}%".format(ds_total_used/ds_total_capacity*100))

    return ds_weight


def get_aggr_usage(na_info, type):
    """
    create a list of all aggregates sorted by usage and calculate the avg aggr usage for vvol or vmfs
    """
    total_capacity = 0
    total_used = 0
    aggr_count = 0
    all_aggr_list = []
    for na in na_info.elements:
        for aggr in na.na_aggr_elements:
            if type == 'vmfs':
                vmfs_luns_found = False
                for lun in aggr.luns:
                    if lun.type == 'vmfs':
                        vmfs_luns_found = True
                if not vmfs_luns_found:
                    continue
            if type == 'vvol':
                vvol_fvols_found = False
                for fvol in aggr.fvols:
                    if fvol.type == 'vvol':
                        vvol_fvols_found = True
                if not vvol_fvols_found:
                    continue
            total_capacity += aggr.capacity
            total_used += aggr.usage / 100 * aggr.capacity
            all_aggr_list.append(aggr)
            aggr_count += 1
    if all_aggr_list == []:
        return all_aggr_list, 0
    all_aggr_list_sorted = sorted(all_aggr_list, key=lambda aggr: aggr.usage)
    min_usage_aggr = all_aggr_list_sorted[0]
    max_usage_aggr = all_aggr_list_sorted[-1]
    avg_aggr_usage = total_used / total_capacity * 100
    # only vmfs and sort by size top down
    log.info("- INFO -  min aggr usage is {:.1f}% on {}"
             .format(min_usage_aggr.usage, min_usage_aggr.name))
    log.info("- INFO -  max aggr usage is {:.1f}% on {}"
             .format(max_usage_aggr.usage, max_usage_aggr.name))
    log.info("- INFO -  avg aggr usage is {:.1f}% weighted across all aggr"
             .format(avg_aggr_usage))

    return all_aggr_list_sorted, avg_aggr_usage


def aggr_name_to_ds_name(netapp_host, aggr_name):
    """
    convert a netapp aggregate name into the corresponding vcenter ds name
    """
    # example for the pattern for aggregate names: aggr_ssd_bb001_1
    m = re.match("^(?:aggr_ssd_bb)(?P<bb>\d+)_\d$", aggr_name)
    if m:
        # this one is special: vVOL_BB056
        if m.group('bb') == '56':
          ds_name = 'vVOL_BB0' + m.group('bb')
          return ds_name
        # example ds_name: vVOL_BB123
        ds_name = 'vVOL_BB' + m.group('bb')
        return ds_name
    # example for the pattern for not bb connected netapps: aggr_ssd_st001_01
    m = re.match("^(?:aggr_ssd_)(?P<stname>st.*)_\d+$", aggr_name)
    if m:
        # example ds_name: vVOL_stnpca3_st030
        ds_name = 'vVOL_' + str(netapp_host).split('.')[0].replace('-','_')
        return ds_name

def vc_mark_instance(vc, vm_info, volume_uuid, instance_uuid):
    """
    mark an instance or shadow vm as being in the process of being balanced
    by adding some special information to the instance anootations in the vc
    """
    instance_name = None
    # vm_instance_uuid is the shadow vm of the volume or the instance the volume is attached to
    # start with the instance (if it exists) to get the instance_uuid, which is needed for the shadow vm as well
    for vm_instance_uuid in instance_uuid, volume_uuid:
        # in the detached case we only have to care about the shadow vm as there is no instance it is attached to
        if not vm_instance_uuid:
            continue
        vm = vm_info.get_by_instanceuuid(vm_instance_uuid)
        # as vm_info is cached info, get and check some value to make sure the instance is still alive
        try:
            config_instance_uuid = vm.handle.config.instanceUuid
        except Exception as e:
            log.warning("- WARN - failed to get the instance uuid for the instance {} in the vc - error: {}".format(vm_instance_uuid, str(e)))
            return False
        if config_instance_uuid != vm_instance_uuid:
            log.warning("- WARN - config.instanceUuid {} does not match for instance {} - giving up".format(config_instance_uuid, vm_instance_uuid))
            return False
        if vm_instance_uuid == instance_uuid:
            instance_name = vm.handle.name
        # get the annotation fresh and not the vm_info cached version
        try:
            annotation = vm.handle.config.annotation
        except Exception as e:
            log.warning("- WARN - failed to get the annotation for the instance {} in the vc - error: {}".format(vm_instance_uuid, str(e)))
            return False
        # add own info here
        new_annotation = annotation + "\nstorage_balancing-vuuid:" + volume_uuid
        # in the attached case add some extra info about the instance the volume is attached to
        if instance_uuid:
            new_annotation = new_annotation + "\nstorage_balancing-iuuid:" + instance_uuid
            # an instance should always have a name, but just in case ...
            if instance_name:
                new_annotation = new_annotation + "\nstorage_balancing-iname:" + instance_name
        # update the annotation in the vc
        spec = vim.vm.ConfigSpec()
        spec.annotation = new_annotation
        try:
            task = vm.handle.ReconfigVM_Task(spec)
            WaitForTask(task, si=vc.api)
        except Exception as e:
            log.warning("- WARN - failed to update the annotation for the instance {} in the vc - error: {}".format(vm_instance_uuid, str(e)))
            return False
        # update the vm_info cache as well
        vm.annotation = new_annotation

    return True

def vc_unmark_instance(vc, vm_info, instance_uuid):
    """
    mark an instance or shadow vm as no longer being in the process of being balanced
    by removing some special information to the instance anootations in the vc
    """
    vm = vm_info.get_by_instanceuuid(instance_uuid)
    # as vm_info is cached info, get and check some value to make sure the instance is still alive
    try:
        config_instance_uuid = vm.handle.config.instanceUuid
    except Exception as e:
        log.warning("- WARN - failed to get the instance uuid for the instance {} in the vc - error: {}".format(instance_uuid, str(e)))
        return False
    if config_instance_uuid != instance_uuid:
        log.warning("- WARN - config.instanceUuid {} does not match for instance {} - giving up".format(config_instance_uuid, instance_uuid))
        return False
    try:
        annotation = vm.handle.config.annotation
    except Exception as e:
        log.warning("- WARN - failed to get the annotation for the instance {} in the vc - error: {}".format(instance_uuid, str(e)))
        return False
    annotation_list = annotation.splitlines()
    new_annotation_list = []
    for line in annotation_list:
        if line.startswith('storage_balancing-'):
            continue
        new_annotation_list.append(line)
    if len(new_annotation_list) > 0:
        new_annotation = '\n'.join(new_annotation_list)
    else:
        new_annotation = ''
    spec = vim.vm.ConfigSpec()
    spec.annotation = new_annotation
    try:
        task = vm.handle.ReconfigVM_Task(spec)
        WaitForTask(task, si=vc.api)
    except Exception as e:
        log.warning("- WARN - failed to update the annotation for the instance {} in the vc - error: {}".format(vm_instance_uuid, str(e)))
        return False
    # update the vm_info cache as well
    vm.annotation = new_annotation

    return True


def os_get_volumes_for_project_id(project_id):
    """
    get volumes for a certain project_id from openstack
    """
    log.info(f"- INFO -   getting cinder volume info for the project {project_id}")
    os_handle=OpenstackHelper(os.getenv('OS_REGION'), os.getenv('OS_USER_DOMAIN_NAME'),
        os.getenv('OS_PROJECT_DOMAIN_NAME'), os.getenv('OS_PROJECT_NAME'), os.getenv('OS_USERNAME'),
        os.getenv('OS_PASSWORD'))

    volume_list = list(os_handle.api.block_storage.volumes(details=True, all_projects=1, project_id=project_id))
    if not volume_list:
        log.info(f"- INFO -    did not get any cinder volumes back for project {project_id} from the cinder api")

    return volume_list