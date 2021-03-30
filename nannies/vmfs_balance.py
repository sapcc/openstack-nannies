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
import argparse
import logging
import time

from helper.netapp import NetAppHelper
from helper.vcenter import *
from helper.prometheus_exporter import *
# prometheus export functionality
from prometheus_client import start_http_server, Gauge

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')


def parse_commandline():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="dry run option not doing anything harmful")
    parser.add_argument("--vcenter-host", required=True,
                        help="Vcenter hostname")
    parser.add_argument("--vcenter-user", required=True,
                        help="Vcenter username")
    parser.add_argument("--vcenter-password", required=True,
                        help="Vcenter user password")
    parser.add_argument("--netapp-user", required=True, help="Netapp username")
    parser.add_argument("--netapp-password", required=True, help="Netapp user password")
    parser.add_argument("--region", required=True, help="(Openstack) region")
    parser.add_argument("--interval", type=int, default=1,
                        help="Interval in minutes between check runs")
    parser.add_argument("--min-usage", type=int, default=60,
                        help="Target ds usage must be below this value in % to do a move")
    parser.add_argument("--max-usage", type=int, default=0,
                        help="Source ds usage must be above this value in % to do a move")
    parser.add_argument("--min-freespace", type=int, default=2500,
                        help="Target ds free sapce should remain at least this value in gb to do a move")
    parser.add_argument("--min-max-difference", type=int, default=2,
                        help="Minimal difference between most and least ds usage above which balancing should be done")
    parser.add_argument("--autopilot", action="store_true",
                        help="Use autopilot-range instead of min-usage and max-usage for balancing decisions")
    parser.add_argument("--autopilot-range", type=int, default=5,
                        help="Corridor of +/-% around the average usage of all ds balancing should be done")
    parser.add_argument("--max-move-vms", type=int, default=5,
                        help="Maximum number of VMs to (propose to) move")
    parser.add_argument("--print-max", type=int, default=10,
                        help="Maximum number largest volumes to print per ds")
    parser.add_argument("--ds-denylist", nargs='*',
                        required=False, help="ignore those ds")
    parser.add_argument("--aggr-volume-min-size", type=int, required=False, default=0,
                        help="Minimum size (>=) in gb for a volume to move for aggr balancing")
    parser.add_argument("--aggr-volume-max-size", type=int, required=False, default=2500,
                        help="Maximum size (<=) in gb for a volume to move for aggr balancing")
    parser.add_argument("--flexvol-volume-min-size", type=int, required=False, default=0,
                        help="Minimum size (>=) in gb for a volume to move for flexvol balancing")
    parser.add_argument("--flexvol-volume-max-size", type=int, required=False, default=2500,
                        help="Maximum size (<=) in gb for a volume to move for flexvol balancing")
    parser.add_argument("--hdd", action="store_true",
                        help="balance hdd storage instead of ssd storage")
    args=parser.parse_args()
    return args


def prometheus_exporter_setup(args):
    nanny_metrics_data=PromDataClass()
    nanny_metrics=PromMetricsClass()
    nanny_metrics.set_metrics('netapp_balancing_nanny_aggregate_usage',
                              'space usage per netapp aggregate in percent', ['aggregate'])
    REGISTRY.register(CustomCollector(nanny_metrics, nanny_metrics_data))
    prometheus_http_start(int(args.prometheus_port))
    return nanny_metrics_data


class VM:
    """
    this is for a single vm
    """

    def __init__(self, vm_element):
        self.name=vm_element['name']
        self.hardware=vm_element['config.hardware']
        self.annotation=vm_element.get('config.annotation')
        self.runtime=vm_element['runtime']
        self.handle=vm_element['obj']

    def is_shadow_vm(self):
        if self.hardware.memoryMB == 128 and self.hardware.numCPU == 1 and \
                self.runtime.powerState == 'poweredOff' and \
                not any(isinstance(dev, vim.vm.device.VirtualEthernetCard) for dev in self.hardware.device):
            number_of_disks=sum(isinstance(
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
        # return [dev.capacityInBytes for dev in self.hardware.device if isinstance(dev, vim.vm.device.VirtualDisk)]
        disksizes=[]
        # find the disk device
        for dev in self.hardware.device:
            if isinstance(dev, vim.vm.device.VirtualDisk):
                disksizes.append(dev.capacityInBytes)
        return disksizes

    def get_total_disksize(self):
        # return sum(dev.capacityInBytes for dev in self.hardware.device if isInstance(dev, vim.vm.device.VirtualDisk))
        return sum(self.get_disksizes())


class VMs:
    """
    this is for all vms we get from the vcenteri
    """

    def __init__(self, vc):
        self.elements=[]
        self.vvol_shadow_vms_for_naaids={}
        self.vmfs_shadow_vms_for_datastores={}
        for vm_element in self.get_vms_dict(vc):
            # ignore instances without a config-hardware node
            if not vm_element.get('config.hardware'):
                log.debug("- WARN - instance {} has no config.hardware!".format(vm_element.get('name', "no name")))
                continue
            self.elements.append(VM(vm_element))
        all_shadow_vm_handles=self.get_shadow_vms([vm.handle for vm in self.elements])
        self.vvol_shadow_vms_for_naaids=self.get_vvol_shadow_vms_for_naaids(vc, all_shadow_vm_handles)
        self.vmfs_shadow_vms_for_datastores=self.get_vmfs_shadow_vms_for_datastores(vc, all_shadow_vm_handles)

    def get_vms_dict(self, vc):
        """
        get info about the vms from the vcenter\n
        return a dict of vms with the vm handles as keys
        """
        log.info("- INFO -  getting vm information from the vcenter")
        vm_view=vc.find_all_of_type(vc.vim.VirtualMachine)
        vms_dict=vc.collect_properties(vm_view, vc.vim.VirtualMachine,
                                         ['name', 'config.annotation', 'config.hardware', 'runtime'], include_mors=True)
        return vms_dict

    # TODO: maybe the vm_handles can go and we do the get_shadow_vms inside
    def get_vvol_shadow_vms_for_naaids(self, vc, vm_handles):
        vvol_shadow_vms_for_naaids = {}
        for vm_handle in vm_handles:
            # iterate over all devices
            for device in vm_handle.hardware.device:
                    # and filter out the virtual disks
                    if not isinstance(device, vc.vim.vm.device.VirtualDisk):
                        continue
                    # we are only interested in vvols here
                    if device.backing.fileName.lower().startswith('[vvol_') and device.backing.backingObjectId:
                        # add backingObjectId to our dict
                        vvol_shadow_vms_for_naaids[device.backing.backingObjectId]=(vm_handle.name, device.capacityInBytes)

        return vvol_shadow_vms_for_naaids

    # TODO: this should maybe go into the DS object
    # TODO: maybe the vm_handles can go and we do the get_shadow_vms inside
    def get_vmfs_shadow_vms_for_datastores(self, vc, vm_handles):
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
                        ds_path_re=re.compile(r"^\[(?P<ds>.*)\].*$")
                        ds=ds_path_re.match(device.backing.fileName)
                        if ds:
                            # add  ds name to out dict of lists
                            if not vmfs_shadow_vms_for_datastores.get(ds.group('ds')):
                                vmfs_shadow_vms_for_datastores[ds.group('ds')]=[(vm_handle.name, device.capacityInBytes)]
                            else:
                                vmfs_shadow_vms_for_datastores[ds.group('ds')].append((vm_handle.name, device.capacityInBytes))

        return vmfs_shadow_vms_for_datastores

    def get_by_handle(self, vm_handle):
        for vm in self.elements:
            if vm.handle == vm_handle:
                return vm
        else:
            return None

    def get_by_name(self, vm_name):
        for vm in self.elements:
            if vm.name == vm_name:
                return vm
        else:
            return None

    def get_shadow_vms(self, vm_handles):
        shadow_vms=[]
        # iterate over the vms
        for vm in self.elements:
            if vm.handle in vm_handles and vm.is_shadow_vm():
                shadow_vms.append(vm)
        return shadow_vms


class DS:
    """
    this is for a single ds
    """

    def __init__(self, ds_element):
        self.name=ds_element['name']
        self.freespace=ds_element['summary.freeSpace']
        self.capacity=ds_element['summary.capacity']
        self.used=ds_element['summary.capacity'] - ds_element['summary.freeSpace']
        self.usage=(1 - ds_element['summary.freeSpace'] / \
            ds_element['summary.capacity']) * 100
        self.vm_handles=ds_element['vm']
        self.ds_handle=ds_element['obj']

    def is_below_usage(self, usage):
        """
        check if the ds usage is below the max usage given in the args
        returns true or false
        """
        if self.usage < usage:
            return True
        else:
            return False

    def is_above_usage(self, usage):
        """
        check if the ds usage is above the min usage given in the args
        returns true or false
        """
        if self.usage > usage:
            return True
        else:
            return False

    def is_below_freespace(self, freespace):
        """
        check if the ds free space is above the min freespace given in the args
        returns true or false
        """
        if self.freespace < freespace * 1024**3:
            return True
        else:
            return False

    def add_shadow_vm(self, vm):
        """
        this adds a vm element to the ds and adjusts the space and usage values
        returns nothing
        """
        # remove vm size from freespace
        self.freespace -= vm.get_total_disksize()
        # add vm to vm list
        self.vm_handles.append(vm.handle)
        # recalc usage
        self.usage=(1 - self.freespace / self.capacity) * 100

    def remove_shadow_vm(self, vm):
        """
        this remove a vm element from the ds and adjusts the space and usage values
        returns nothing
        """
        # remove vm from vm list
        self.vm_handles.remove(vm.handle)
        # add vm size to freespace
        self.freespace += vm.get_total_disksize()
        # recalc usage
        self.usage=(1 - self.freespace / self.capacity) * 100


class DataStores:
    """
    this is for all datastores we get from the vcenter
    """

    def __init__(self, vc):
        self.elements=[]
        for ds_element in self.get_datastores_dict(vc):
            # ignore datastores with zero capacity
            if ds_element.get('summary.capacity') == 0:
                log.info("- WARN - ds {} has zero capacity!".format(ds_element.get('name', "no name")))
                continue
            self.elements.append(DS(ds_element))

    @staticmethod
    def get_datastores_dict(vc):
        """
        get info about the datastores from the vcenter
        return a dict of datastores with the ds handles as keys
        """
        log.info("- INFO -  getting datastore information from the vcenter")
        ds_view=vc.find_all_of_type(vc.vim.Datastore)
        datastores_dict=vc.collect_properties(ds_view, vc.vim.Datastore,
                                                ['name', 'summary.freeSpace',
                                                    'summary.capacity', 'vm'],
                                                include_mors=True)
        return datastores_dict

    def get_by_handle(self, ds_handle):
        for ds in self.elements:
            if ds.handle == ds_handle:
                return ds
        else:
            return None

    def get_by_name(self, ds_name):
        for ds in self.elements:
            if ds.name == ds_name:
                return ds
        else:
            return None

    def vmfs_ds(self, ds_denylist=[]):
        """
        filter for only vmfs ds and sort by size
        return a list of datastore elements
        """
        ds_name_regex_pattern = '^(?:vmfs_vc.*_ssd_).*'
        self.elements = [ds for ds in self.elements if re.match(
            ds_name_regex_pattern, ds.name) and not (ds_denylist and ds.name in ds_denylist)]

    def sort_by_usage(self):
        self.elements.sort(key=lambda element: element.usage, reverse=True)

    def get_overall_capacity(self):
        overall_capacity = sum(ds.capacity for ds in self.elements)
        return overall_capacity

    def get_overall_freespace(self):
        overall_freespace = sum(ds.freespace for ds in self.elements)
        return overall_freespace

    def get_overall_average_usage(self):
        overall_average_usage = (1 - self.get_overall_freespace() / self.get_overall_capacity()) * 100
        return overall_average_usage

class NAAggr:
    """
    this is for a single netapp aggregate
    """

    def __init__(self, naaggr_element, parent, fvol_list, lun_list):
        self.name=naaggr_element['name']
        self.host=naaggr_element['host']
        self.usage=naaggr_element['usage']
        self.capacity=naaggr_element['capacity']
#        self.luns=naaggr_element['luns']
        self.parent=parent
        self.fvols=[fvol for fvol in fvol_list if fvol['aggr'] == self.name]
        self.luns=[]
        for fvol in self.fvols:
            luns=[lun for lun in lun_list if lun['fvol'] == fvol['name']]
            self.luns.append(luns)

class NAFvol:
    """
    this is for a single netapp flexvol
    """

    def __init__(self, nafvol_element, parent, lun_list):
        self.name=nafvol_element['name']
        self.host=nafvol_element['host']
        self.aggr=nafvol_element['aggr']
        self.usage=nafvol_element['usage']
        self.capacity=nafvol_element['capacity']
#        self.luns=nafvol_element['luns']
        self.parent=parent
        self.luns=[lun for lun in lun_list if lun['fvol'] == self.name]

class NALun:
    """
    this is for a single netapp lun
    """

    def __init__(self, nalun_element, parent):
        self.name=nalun_element['fvol']
        self.host=nalun_element['host']
        self.used=nalun_element['used']
        self.path=nalun_element['path']
        self.comment=nalun_element['comment']
        self.naaid=nalun_element['naaid']
        self.parent=parent

class NA:
    """
    this is for a single netapp
    """

    def __init__(self, na_element, na_user, na_password):
        # not sure yet if those will come here
        #self.na_aggr_elements=[]
        #self.na_fvol_elements=[]
        #self.na_lun_elements=[]
        self.host=na_element['host']
        self.vc=na_element['vc']

        log.info("- INFO - connecting to netapp %s", self.host)
        self.nh = NetAppHelper(host=self.host, user=na_user, password=na_password)
        na_version = self.nh.get_single("system-get-version")
        log.info("- INFO -  {} is on version {}".format(self.host, na_version['version']))

        lun_list = self.get_lun_info(self.nh, [])
        for lun in lun_list:
            nalun_element={}
            nalun_element['fvol'] = lun['fvol']
            nalun_element['host'] = lun['host']
            nalun_element['used'] = lun['used']
            nalun_element['path'] = lun['path']
            nalun_element['comment'] = lun['comment']
            nalun_element['naaid'] = lun['naaid']
            nalun_element['parent'] = self
            lun_instance = NALun(nalun_element, self)

        fvol_list = self.get_fvol_info(self.nh, [])
        for fvol in fvol_list:
            nafvol_element={}
            nafvol_element['name'] = fvol['name']
            nafvol_element['host'] = fvol['host']
            nafvol_element['aggr'] = fvol['aggr']
            nafvol_element['capacity'] = fvol['capacity']
            nafvol_element['used'] = fvol['used']
            nafvol_element['usage'] = fvol['usage']
            nafvol_element['parent'] = self
            fvol_instance = NAFvol(nafvol_element, self, lun_list)

        aggr_list = self.get_aggr_info(self.nh, [])
        for aggr in aggr_list:
            naaggr_element={}
            naaggr_element['name'] = aggr['name']
            naaggr_element['host'] = aggr['host']
            naaggr_element['usage'] = aggr['usage']
            naaggr_element['capacity'] = aggr['capacity']
            naaggr_element['parent'] = self
            aggr_instance = NAAggr(naaggr_element, self, fvol_list, lun_list)

    def get_aggr_info(self, nh, aggr_denylist):
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
                log.info("- INFO -   aggregate {} of size {:.0f} gb is at {}% utilization"
                    .format(aggr['aggregate-name'],
                        int(aggr['aggr-space-attributes']['size-total']) / 1024**3,
                        aggr['aggr-space-attributes']['percent-used-capacity']))
                naaggr_element['name'] = aggr['aggregate-name']
                naaggr_element['host'] = self.host
                naaggr_element['usage'] = int(aggr['aggr-space-attributes']['percent-used-capacity'])
                naaggr_element['capacity'] = int(aggr['aggr-space-attributes']['size-total'])
                aggr_info.append(naaggr_element)

        return aggr_info

    def get_fvol_info(self, nh, fvol_denylist):
        fvol_info = []
        # get flexvols
        for fvol in nh.get_volume_usage():
            nafvol_element = {}
            # print info for fvol_denylisted flexvols
            if fvol['volume-id-attributes']['name'] in fvol_denylist:
                log.info("- INFO -   flexvol {} is fvol_denylist'ed via cmdline"
                    .format(fvol['volume-id-attributes']['name']))

            if fvol['volume-id-attributes']['name'].lower().startswith('vv') \
                    and fvol['volume-id-attributes']['name'] not in fvol_denylist:
                log.info("- INFO -   flexvol {} on {} of size {:.0f} gb of a total size {:.0f} gb"
                    .format(fvol['volume-id-attributes']['name'],
                        fvol['volume-id-attributes']['containing-aggregate-name'],
                        int(fvol['volume-space-attributes']['size-used']) / 1024**3,
                        int(fvol['volume-space-attributes']['size-total']) / 1024**3))
                nafvol_element['name'] = fvol['volume-id-attributes']['name']
                nafvol_element['host'] = self.host
                nafvol_element['aggr'] = fvol['volume-id-attributes']['containing-aggregate-name']
                nafvol_element['capacity'] = int(fvol['volume-space-attributes']['size-total'])
                nafvol_element['used'] = int(fvol['volume-space-attributes']['size-used'])
                nafvol_element['usage'] = nafvol_element['used'] / nafvol_element['capacity'] * 100
                fvol_info.append(nafvol_element)

        return fvol_info

    def get_lun_info(self, nh, lun_denylist):
        lun_info = []
        naa_path_re = re.compile(r"^/vol/.*/(?P<naa>naa\..*)\.vmdk$")
        # get luns
        for lun in nh.get_luns():
            path_match = naa_path_re.match(lun['path'])
            if not path_match:
                continue
            nalun_element = {}
            # print info for lun_denylisted luns
            if lun['volume'] in lun_denylist:
                log.info("- INFO -   lun {} is lun_denylist'ed via cmdline"
                    .format(path_match.group('naa')))

            if lun['volume'] not in lun_denylist:
                log.info("- INFO -   lun {} on flexvol {} of size {:.0f} gb"
                    .format(path_match.group('naa'),
                        lun['volume'],
                        int(lun['size-used']) / 1024**3))
                nalun_element['fvol'] = lun['volume']
                nalun_element['host'] = self.host
                nalun_element['used'] = int(lun['size-used'])
                nalun_element['path'] = lun['path']
                nalun_element['comment'] = lun['comment']
                nalun_element['naaid'] = path_match.group('naa')
                lun_info.append(nalun_element)

        return lun_info

class NAs:
    """
    this is for all netapps connected to the vcenter
    """

    def __init__(self, vc, na_user, na_password, region):
        self.elements=[]

        na_hosts = self.get_na_hosts(vc, region)

        for na_host in na_hosts:
            na_element={}
            na_element['host'] = na_host
            na_element['vc'] = vc
            self.elements.append(NA(na_element, na_user, na_password))

    def get_na_hosts(self, vc, region):
        na_hosts_set=set()
        for ds_element in DataStores.get_datastores_dict(vc):
            ds_name = ds_element['name']
            if ds_name.startswith("vmfs_vc"):
                # example for the pattern: vmfs_vc_a_0_p_ssd_bb123_004
                #                      or: vmfs_vc-a_0_p_ssd_bb123_004
                m = re.match("^(?:vmfs_vc(-|_).*_ssd)_bb(?P<bb>\d+)_\d+$", ds_name)
                if m:
                    bbnum = int(m.group('bb'))
                    # one of our netapps is inconsistent in its naming - handle this here
                    if bbnum == 56:
                        stnpa_num = 0
                    else:
                        stnpa_num = 1
                    # e.g. stnpca1-bb123.cc.<region>.cloud.sap - those are the netapp cluster addresses (..np_c_a1..)
                    netapp_name = "stnpca{}-bb{:03d}.cc.{}.cloud.sap".format(stnpa_num, bbnum, region)
                    na_hosts_set.add(netapp_name)
                    continue
                # example for the pattern: vmfs_vc_a_0_p_ssd_stnpca1-st123_004
                #                      or: vmfs_vc-a_0_p_ssd_stnpca1-st123_004
                m = re.match("^(?:vmfs_vc(-|_).*_ssd)_(?P<stname>.*)_\d+$", ds_name)
                if m:
                    # e.g. stnpca1-st123.cc.<region>.cloud.sap - those are the netapp cluster addresses (..np_c_a1..)
                    netapp_name = "{}.cc.{}.cloud.sap".format(str(m.group('stname')).replace('_','-'), region)
                    na_hosts_set.add(netapp_name)

        return sorted(na_hosts_set)

def sanity_checks(least_used_ds, most_used_ds, min_usage, max_usage, min_freespace, min_max_difference):
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

def sort_vms_by_total_disksize(vms):
    return sorted(vms, key=lambda vm: vm.get_total_disksize(), reverse=True)

def move_shadow_vm_from_ds_to_ds(ds1, ds2, vm):
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

def vmfs_balancing(ds_info, vm_info, args):

    # only vmfs ds are balanced here
    ds_info.vmfs_ds(args.ds_denylist)

    if len(ds_info.elements) == 0:
        log.warning("- WARN - no vmfs ds in this vcenter")
        return

    log.info("- CMND -  new run starting")

    ds_overall_average_usage = ds_info.get_overall_average_usage()
    log.info("- INFO -  average usage across all vmfs ds is {:.1f}% ({:.0f}G free - {:.0f}G total)"\
            .format(ds_overall_average_usage,\
                    ds_info.get_overall_freespace() / 1024**3,\
                    ds_info.get_overall_capacity() / 1024**3))

    ds_info.sort_by_usage()

    # first print out some ds and shadow vm info
    for i in ds_info.elements:
        if args.ds_denylist and i.name in args.ds_denylist:
            log.info("- INFO -   ds: {} - {:.1f}% - {:.0f}G free - ignored as it is on the deny list".format(i.name,
                   i.usage, i.freespace/1024**3))
            break
        log.info("- INFO -   ds: {} - {:.1f}% - {:.0f}G free".format(i.name,
                 i.usage, i.freespace/1024**3))
        shadow_vms=vm_info.get_shadow_vms(i.vm_handles)
        shadow_vms_sorted_by_disksize=sort_vms_by_total_disksize(shadow_vms)
        printed=0
        for j in shadow_vms_sorted_by_disksize:
            if printed < args.print_max:
                log.info(
                    "- INFO -    {} - {:.0f}G".format(j.name, j.get_total_disksize() / 1024**3))
                printed += 1

    if args.autopilot:
        min_usage = ds_overall_average_usage - args.autopilot_range
        max_usage = ds_overall_average_usage + args.autopilot_range
    else:
        min_usage = args.min_usage
        max_usage = args.max_usage

    # balancing loop
    moves_done=0
    while True:

        if moves_done > args.max_move_vms:
            break

        most_used_ds=ds_info.elements[0]
        least_used_ds=ds_info.elements[-1]

        if not sanity_checks(least_used_ds, most_used_ds, min_usage, max_usage, args.min_freespace, args.min_max_difference):
            break

        shadow_vms_on_most_used_ds = []
        for vm in vm_info.get_shadow_vms(most_used_ds.vm_handles):
            vm_disksize = vm.get_total_disksize() / 1024**3
            # move smaller volumes once the most and least used get closer to avoid oscillation
            vm_maxdisksize = min((least_used_ds.freespace - most_used_ds.freespace) / (2 * 1024**3), args.volume_max_size)
            if args.volume_min_size <= vm_disksize <= vm_maxdisksize:
               shadow_vms_on_most_used_ds.append(vm)
        if not shadow_vms_on_most_used_ds:
            log.warning(
                "- WARN - no more shadow vms to move on most used ds {}".format(most_used_ds.name))
            break
        largest_shadow_vm_on_most_used_ds=sort_vms_by_total_disksize(
            shadow_vms_on_most_used_ds)[0]
        move_shadow_vm_from_ds_to_ds(most_used_ds, least_used_ds,
                              largest_shadow_vm_on_most_used_ds)
        moves_done += 1
        ds_info.sort_by_usage()

def check_loop(args):
    """
    endless loop of generating move suggestions and wait for the next run
    """
    while True:
        log.info("INFO: starting new loop run")
        if args.dry_run:
            log.info("- INFO - dry-run mode: not doing anything harmful")

        # open a connection to the vcenter
        vc=VCenterHelper(host=args.vcenter_host,
                       user=args.vcenter_user, password=args.vcenter_password)

        # get the vm and ds info from the vcenter
        vm_info=VMs(vc)
        ds_info=DataStores(vc)
        na_info=NAs(vc, args.netapp_user, args.netapp_password, args.region)

        # do the actual balancing
        vmfs_balancing(ds_info, vm_info, args)

        # wait the interval time
        log.info("INFO: waiting %s minutes before starting the next loop run", str(
            args.interval))
        time.sleep(60 * int(args.interval))


def main():

    args=parse_commandline()

    check_loop(args)


if __name__ == '__main__':
    main()
