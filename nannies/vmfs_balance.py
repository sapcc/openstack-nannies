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

from helper.vcenter import *
from helper.prometheus_exporter import *
# prometheus export functionality
from prometheus_client import start_http_server, Gauge

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')


def parse_commandline():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help='dry run option not doing anything harmful')
    parser.add_argument("--vcenter-host", required=True,
                        help="Vcenter hostname")
    parser.add_argument("--vcenter-user", required=True,
                        help="Vcenter username")
    parser.add_argument("--vcenter-password", required=True,
                        help="Vcenter user password")
    parser.add_argument("--interval", type=int, default=1,
                        help="Interval in minutes between check runs")
    parser.add_argument("--min-usage", type=int, default=60,
                        help="Target ds usage must be below this value in % to do a move")
    parser.add_argument("--max-usage", type=int, default=0,
                        help="Source ds usage must be above this value in % to do a move")
    parser.add_argument("--min-freespace", type=int, default=2500,
                        help="Target ds free sapce should remain at least this value in gb to do a move")
    parser.add_argument("--max-move-vms", type=int, default=5,
                        help="Maximum number of VMs to (propose to) move")
    parser.add_argument("--print-max", type=int, default=10,
                        help="Maximum number largest volumes to print per ds")
    parser.add_argument("--ds-denylist", nargs='*',
                        required=False, help="ignore those ds")
    parser.add_argument("--volume-min-size", type=int, required=False, default=0,
                        help="Minimum size (>=) in gb for a volume to move for balancing")
    parser.add_argument("--volume-max-size", type=int, required=False, default=2500,
                        help="Maximum size (<=) in gb for a volume to move for balancing")
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
    elements=[]

    def __init__(self, vc):
        self.elements=[VM(vm_element) for vm_element in self.get_vms_dict(vc)]

    def get_vms_dict(self, vc):
        """
        get info about the vms from the vcenter\n
        return a dict of vms with the vm handles as keys
        """
        log.info("- INFO -  getting vm information from the vcenter")
        vm_view=vc.find_all_of_type(vc.vim.VirtualMachine)
        vms_dict=vc.collect_properties(vm_view, vc.vim.VirtualMachine,
                                         ['name', 'config.hardware', 'runtime'], include_mors=True)
        return vms_dict

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

    def get_shadow_vms(self, vm_handles=elements):
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
        self.usage=ds_element['summary.freeSpace'] / \
            ds_element['summary.capacity']
        self.vm_handles=ds_element['vm']
        self.ds_handle=ds_element['obj']

    def is_below_max_usage(self, args):
        """
        check if the ds usage is below the max usage given in the args
        returns true or false
        """
        if self.usage < args.max_usage:
            log.debug("- INFO - ds {} with usage {:.1f}% is below the max usage limit of {:.1f}%".format(
                self.name, self.usage, args.max_usage))
            return True
        else:
            log.info("- INFO - ds {} with usage {:.1f}% is above the max usage limit of {:.1f}%".format(
                self.name, self.usage, args.max_usage))
            return False

    def is_above_min_usage(self, args):
        """
        check if the ds usage is above the min usage given in the args
        returns true or false
        """
        if self.usage > args.min_usage:
            log.debug("- INFO - ds {} with usage {:.1f}% is above the min usage limit of {:.1f}%".format(
                self.name, self.usage, args.min_usage))
            return True
        else:
            log.info("- INFO - ds {} with usage {:.1f}% is below the min usage limit of {:.1f}%".format(
                self.name, self.usage, args.min_usage))
            return False

    def is_above_min_freespace(self, args):
        """
        check if the ds free space is above the min freespace given in the args
        returns true or false
        """
        if self.freespace > args.min_freespace * 1024**3:
            log.debug("- INFO - ds {} with free space {:0.f}G is above the freespace limit of {:.0f}G".format(
                self.name, self.freespace, args.min_freespace))
            return True
        else:
            log.info("- INFO - ds {} with free space {:0.f}G is below the freespace limit of {:.0f}G".format(
                self.name, self.freespace, args.min_freespace))
            return False

    def passes_all_checks(self, args):
        """
        check if the ds is fullfilling al the test criteria checked
        returns true or false
        """
        if self.is_below_max_usage(args) and self.is_above_min_usage(args) and self.is_above_min_freespace(args):
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
        self.usage=self.freespace / self.capacity

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
        self.usage=self.freespace / self.capacity


class DataStores:
    """
    this is for all datastores we get from the vcentera
    """
    elements=[]

    def __init__(self, vc):
        self.elements=[DS(ds_element)
                          for ds_element in self.get_datastores_dict(vc)]

    def get_datastores_dict(self, vc):
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
        # ds_name_regex_pattern='^(?:vmfs_vc.*_hdd_bb)(?P<bb>\d+)(_\d+)'
        ds_name_regex_pattern = '^(?:vmfs_vc.*_hdd_).*'
        self.elements = [ds for ds in self.elements if re.match(
            ds_name_regex_pattern, ds.name) and not (ds_denylist and ds.name in ds_denylist)]
        # self.elements=[ds for ds in self.elements if re.match(ds_name_regex_pattern, ds.name)]

    def sort_by_usage(self):
        self.elements.sort(key=lambda element: element.usage, reverse=True)


def sort_vms_by_total_disksize(vms):
    return sorted(vms, key=lambda vm: vm.get_total_disksize(), reverse=True)

def move_shadow_vm_from_ds_to_ds(ds1, ds2, vm):
    # remove vm from source ds
    ds1.remove_shadow_vm(vm)
    # for now just print out the move . later: do the actual move
    log.info(
        "- INFO - move vm {} from ds {} to ds {}".format(vm.name, ds1.name, ds2.name))
    # add the vm to the target ds
    ds2.add_shadow_vm(vm)

def vmfs_balancing(ds_info, vm_info, args):
    # only vmfs ds are balanced here
    ds_info.vmfs_ds(args.ds_denylist)

    if len(ds_info.elements) == 0:
        log.warning("- WARN - no vmfs ds in this vcenter")
        return

    ds_info.sort_by_usage()

    # first print out some ds and shadow vm info
    for i in ds_info.elements:
        if args.ds_denylist and i.name in args.ds_denylist:
          log.info("- INFO -   ds: {} - {:.1f}% - {:.0f}G free".format(i.name,
                   i.usage, i.freespace/1024**3))
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

    # balancing loop
    moves_done=0
    while True:
        if moves_done > args.max_move_vms:
            break
        most_used_ds=ds_info.elements[0]
        least_used_ds=ds_info.elements[-1]
        if most_used_ds.passes_all_checks(args) and least_used_ds.passes_all_checks(args):
            break
        if (most_used_ds.usage - least_used_ds.usage) < 1:
            log.warning("- WARN - usages of most used ds {} and least used ds {} are less than 1% apart".format(
                most_used_ds.name, least_used_ds.name))
            break
        shadow_vms_on_most_used_ds = []
        for vm in vm_info.get_shadow_vms(most_used_ds.vm_handles):
            vm_disksize = vm.get_totoal_disksize() / 1024**3
            if args.volume_min_size <= vm_disksize <= args.volume_max_size:
               shadow_vms_on_most_used_dsppend(vm)
        if not shadow_vms_on_most_used_ds:
            log.warning(
                "- WARN - no more shadow vms to move on most used ds {}".format(most_used_ds.name))
            break
        largest_shadow_vm_on_most_used_ds=sort_vms_by_total_disksize(
            shadow_vms_on_most_used_ds)[0]
        # if vm_info.get(largest_shadow_vm_on_largest_ds).get_totoal_disksize() > \
        #         0.5 * (ds_info.get(least_used_ds).freespace - ds.info.get(most_used_ds).freespace):
        #     break
        move_shadow_vm_from_ds_to_ds(most_used_ds, least_used_ds,
                              largest_shadow_vm_on_largest_ds)
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
