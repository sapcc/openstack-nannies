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
from helper.vmfs_balance_helper import *
# prometheus export functionality
from prometheus_client import start_http_server, Gauge

log = logging.getLogger(__name__)


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
    parser.add_argument("--netapp-password", required=True,
                        help="Netapp user password")
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
    # TODO: maybe add aggr-denylist as well
    parser.add_argument("--ds-denylist", nargs='*',
                        required=False, help="ignore those ds")
    parser.add_argument("--aggr-volume-min-size", type=int, required=False, default=0,
                        help="Minimum size (>=) in gb for a volume to move for aggr balancing")
    parser.add_argument("--aggr-volume-max-size", type=int, required=False, default=2500,
                        help="Maximum size (<=) in gb for a volume to move for aggr balancing")
    parser.add_argument("--aggr-max-usage", type=int, required=False, default=75,
                        help="Maximum usage of ithe max used aggr to still balance to it")
    parser.add_argument("--ds-volume-min-size", type=int, required=False, default=0,
                        help="Minimum size (>=) in gb for a volume to move for ds balancing")
    parser.add_argument("--ds-volume-max-size", type=int, required=False, default=2500,
                        help="Maximum size (<=) in gb for a volume to move for ds balancing")
    parser.add_argument("--hdd", action="store_true",
                        help="balance hdd storage instead of ssd storage")
    parser.add_argument("--debug", action="store_true",
                        help="add additional debug output")
    args = parser.parse_args()
    return args


def prometheus_exporter_setup(args):
    nanny_metrics_data = PromDataClass()
    nanny_metrics = PromMetricsClass()
    nanny_metrics.set_metrics('netapp_balancing_nanny_aggregate_usage',
                              'space usage per netapp aggregate in percent', ['aggregate'])
    REGISTRY.register(CustomCollector(nanny_metrics, nanny_metrics_data))
    prometheus_http_start(int(args.prometheus_port))
    return nanny_metrics_data


def vmfs_aggr_balancing(na_info, ds_info, vm_info, args, ds_type):
    """
    balance the usage of the underlaying aggregates of vmfs ds
    """

    # balance sdd or hdd storage based on cmdline switch
    if ds_type == 'hdd':
        lun_name_re = re.compile(r"^.*_hdd_.*$")
    else:
        lun_name_re = re.compile(r"^.*_ssd_.*$")

    # get a weight factor per datastore about underlaying aggr usage - see function above
    ds_weight = get_aggr_and_ds_stats(na_info, ds_info)

    # get the aggr usage info
    all_aggr_list_sorted, avg_aggr_usage = get_aggr_usage(na_info, 'vmfs')

    if len(all_aggr_list_sorted) == 0:
        log.info("- INFO - no aggegates found ...")
        return False

    min_usage_aggr = all_aggr_list_sorted.pop(0)
    max_usage_aggr = all_aggr_list_sorted[-1]

    # this is the difference from the current max used size to the avg used size - this much we might balance stuff away
    # allow to go autopilot_range below the avrg as the physical size will usually be lower than what we move
    max_size_to_free_on_max_used_aggr = (
        max_usage_aggr.usage - avg_aggr_usage + args.autopilot_range) * max_usage_aggr.capacity / 100

    # only do aggr balancing if max aggr usage is more than --autopilot-range % above the avg
    if max_usage_aggr.usage < avg_aggr_usage + args.autopilot_range:
        log.info("- INFO -  max usage aggr is still within the autopilot range above avg aggr usage - no aggr balancing required")
        return False
    else:
        log.info(
            "- INFO -  max usage aggr is more than the autopilot range above avg aggr usage - aggr balancing required")

    # find potential source ds for balancing: from max used aggr, vmfs and ssd or hdd
    balancing_source_ds = []
    log.info("- INFO -  ds on the max usage aggr:")
    for lun in max_usage_aggr.luns:
        # we only care for vmfs here
        if lun.type != 'vmfs':
            continue
        # we only care for ssd or hdd depending on -hdd cmdline switch
        if not lun_name_re.match(lun.name):
            continue
        ds_for_lun = ds_info.get_by_name(lun.name)
        if not ds_for_lun:
            log.warning("- WARN - lun {} seems to not be connected as ds to the vc or its ds name is maybe wrong".format(lun.name))
            continue
        log.info("- INFO -   {}".format(lun.name))
        balancing_source_ds.append(ds_for_lun)

    if balancing_source_ds == []:
        log.warning("- WARN - no vmfs {} ds in this vcenter - giving up".format(ds_type))
        return False

    balancing_source_ds.sort(key=lambda ds: ds.usage)

    # balancing the most used ds on the most used aggr makes most sense
    most_used_ds_on_most_used_aggr = balancing_source_ds[-1]

    # limit the ds info from the vc to vmfs ds only
    ds_info.vmfs_ds(ds_type = ds_type)
    ds_info.sort_by_usage()

    if len(ds_info.elements) == 0:
        log.warning("- WARN - no vmfs {} ds in this vcenter - giving up".format(ds_type))
        return

    # useful for debugging
    ds_overall_average_usage = ds_info.get_overall_average_usage()
    log.info("- INFO -  average usage across all vmfs ds is {:.1f}% ({:.0f}G free - {:.0f}G total)"
             .format(ds_overall_average_usage,
                     ds_info.get_overall_freespace() / 1024**3,
                     ds_info.get_overall_capacity() / 1024**3))

    # useful debugging info for ds and largest shadow vms
    for i in ds_info.elements:
        if args.ds_denylist and i.name in args.ds_denylist:
            log.info("- INFO -   ds: {} - {:.1f}% - {:.0f}G free - ignored as it is on the deny list".format(i.name,
                                                                                                             i.usage, i.freespace/1024**3))
            break
        log.info("- INFO -   ds: {} - {:.1f}% - {:.0f}G free".format(i.name,
                                                                     i.usage, i.freespace/1024**3))
        shadow_vms = vm_info.get_shadow_vms(i.vm_handles)
        shadow_vms_sorted_by_disksize = sort_vms_by_total_disksize(shadow_vms)
        printed = 0
        for j in shadow_vms_sorted_by_disksize:
            if printed < args.print_max:
                log.info(
                    "- INFO -    {} - {:.0f}G".format(j.name, j.get_total_disksize() / 1024**3))
                printed += 1

    # we do not want to balance to ds on the most used aggr: put those ds onto the deny list
    if args.ds_denylist:
        extended_ds_denylist = args.ds_denylist
    else:
        extended_ds_denylist = []
    extended_ds_denylist.extend([lun.name for lun in max_usage_aggr.luns])

    # exclude the ds from the above gernerated extended deny list
    ds_info.vmfs_ds(extended_ds_denylist, ds_type = ds_type)

    # balancing loop
    moves_done = 0
    moved_size = 0
    while True:

        if moves_done > args.max_move_vms:
            log.info(
                "- INFO -  max number of vms to move ({}) reached - stopping aggr balancing now".format(args.max_move_vms))
            break

        # balance at max as much space we would need to bring max aggr usage to avg plus autopilot_range
        if moved_size > max_size_to_free_on_max_used_aggr:
            log.info(
                "- INFO -  enough space freed from max usage aggr {} - stopping aggr balancing now".format(max_usage_aggr.name))
            break

        # balance at max slightly below the average as the most used ds on the
        # most used aggr might simply be below the avg due to dedup and compression
        if most_used_ds_on_most_used_aggr.usage < (ds_overall_average_usage - 4 * args.autopilot_range):
            log.info(
                "- INFO -  enough space freed from largest ds {} on max usage aggr {} - stopping aggr balancing now".format(most_used_ds_on_most_used_aggr.name, max_usage_aggr.name))
            break

        # resort based on aggr usage weights - for the target ds we want to
        # count this in to avoid balancing to ds on already full aggr
        ds_info.sort_by_usage(ds_weight)

        least_used_ds = ds_info.elements[-1]
        least_used_ds_free_space = least_used_ds.freespace - args.min_freespace * 1024**3

        if not sanity_checks_liter(least_used_ds, most_used_ds_on_most_used_aggr, args.min_freespace, args.min_max_difference):
            break

        shadow_vms_on_most_used_ds_on_most_used_aggr = []
        for vm in vm_info.get_shadow_vms(most_used_ds_on_most_used_aggr.vm_handles):
            vm_disksize = vm.get_total_disksize() / 1024**3
            if args.aggr_volume_min_size <= vm_disksize <= min(least_used_ds_free_space / 1024**3, args.aggr_volume_max_size):
                shadow_vms_on_most_used_ds_on_most_used_aggr.append(vm)
        if not shadow_vms_on_most_used_ds_on_most_used_aggr:
            log.warning(
                "- WARN -  no more shadow vms to move on most used ds {} on most used aggr {}".format(most_used_ds_on_most_used_aggr.name, max_usage_aggr.name))
            break
        # TODO: decide whether to balance from largest first or smallest first
        largest_shadow_vm_on_most_used_ds_on_most_used_aggr = sort_vms_by_total_disksize(
            shadow_vms_on_most_used_ds_on_most_used_aggr)[0]
        move_vmfs_shadow_vm_from_ds_to_ds(most_used_ds_on_most_used_aggr, least_used_ds,
                                     largest_shadow_vm_on_most_used_ds_on_most_used_aggr)
        moves_done += 1
        moved_size += largest_shadow_vm_on_most_used_ds_on_most_used_aggr.get_total_disksize()
        # smallest_shadow_vm_on_most_used_ds_on_most_used_aggr=sort_vms_by_total_disksize(
        #     shadow_vms_on_most_used_ds_on_most_used_aggr)[-1]
        # move_vmfs_shadow_vm_from_ds_to_ds(most_used_ds_on_most_used_aggr, least_used_ds,
        #                       smallest_shadow_vm_on_most_used_ds_on_most_used_aggr)
        # moves_done += 1
        # moved_size += smallest_shadow_vm_on_most_used_ds_on_most_used_aggr.get_total_disksize()

        # resort the ds by usage in preparation for the next loop iteration
        ds_info.sort_by_usage()


def vmfs_ds_balancing(na_info, ds_info, vm_info, args, ds_type):
    """
    balance the usage of the vmfs datastores
    """
    # get a weight factor per datastore about underlaying aggr usage - see function above
    ds_weight = get_aggr_and_ds_stats(na_info, ds_info)

    # get the aggr with the highest usage from the netapp to avoid its luns=vc ds as balancing target
    # get the aggr usage info
    all_aggr_list_sorted, avg_aggr_usage = get_aggr_usage(na_info, 'vmfs')

    if len(all_aggr_list_sorted) == 0:
        log.info("- INFO - no aggegates found ...")
        return False

    min_usage_aggr = all_aggr_list_sorted.pop(0)
    max_usage_aggr = all_aggr_list_sorted[-1]

    # limit the ds info from the vc to vmfs ds only
    ds_info.vmfs_ds(ds_type = ds_type)
    ds_info.sort_by_usage()

    if len(ds_info.elements) == 0:
        log.warning("- WARN - no vmfs {} ds in this vcenter - giving up".format(ds_type))
        return

    ds_overall_average_usage = ds_info.get_overall_average_usage()
    log.info("- INFO -  average usage across all vmfs ds is {:.1f}% ({:.0f}G free - {:.0f}G total)"
             .format(ds_overall_average_usage,
                     ds_info.get_overall_freespace() / 1024**3,
                     ds_info.get_overall_capacity() / 1024**3))

    # useful debugging info for ds and largest shadow vms
    for i in ds_info.elements:
        if args.ds_denylist and i.name in args.ds_denylist:
            log.info("- INFO -   ds: {} - {:.1f}% - {:.0f}G free - ignored as it is on the deny list".format(i.name,
                                                                                                             i.usage, i.freespace/1024**3))
            break
        log.info("- INFO -   ds: {} - {:.1f}% - {:.0f}G free".format(i.name,
                                                                     i.usage, i.freespace/1024**3))
        shadow_vms = vm_info.get_shadow_vms(i.vm_handles)
        shadow_vms_sorted_by_disksize = sort_vms_by_total_disksize(shadow_vms)
        printed = 0
        for j in shadow_vms_sorted_by_disksize:
            if printed < args.print_max:
                log.info(
                    "- INFO -    {} - {:.0f}G".format(j.name, j.get_total_disksize() / 1024**3))
                printed += 1

    # we do not want to balance to ds on the most used aggr: put those ds onto the deny list
    if args.ds_denylist:
        extended_ds_denylist = args.ds_denylist
    else:
        extended_ds_denylist = []

    # if the most used aggr is realatively full, remove its luns=ds from the target ds list
    if max_usage_aggr.usage > args.aggr_max_usage:
        extended_ds_denylist.extend([lun.name for lun in max_usage_aggr.luns])

    # exclude the ds from the above gernerated extended deny list
    ds_info.vmfs_ds(extended_ds_denylist, ds_type = ds_type)

    # if in auto pilot mode define the min/max values as a range around the avg
    if args.autopilot:
        min_usage = ds_overall_average_usage - args.autopilot_range
        max_usage = ds_overall_average_usage + args.autopilot_range
    else:
        min_usage = args.min_usage
        max_usage = args.max_usage

    # balancing loop
    moves_done = 0
    while True:

        if moves_done > args.max_move_vms:
            log.info(
                "- INFO -  max number of vms to move ({}) reached - stopping aggr balancing now".format(args.max_move_vms))
            break

        most_used_ds = ds_info.elements[0]

        # resort based on aggr usage weights - for the target ds we want to
        # count this in to avoid balancing to ds on already full aggr
        ds_info.sort_by_usage(ds_weight)

        least_used_ds = ds_info.elements[-1]

        # TODO: this has to be redefined as it does not longer work with the weighted values - lets try a lite version of the check
        # if not sanity_checks(least_used_ds, most_used_ds, min_usage, max_usage, args.min_freespace, args.min_max_difference):
        if not sanity_checks_lite(least_used_ds, most_used_ds, args.min_freespace, args.min_max_difference):
            break

        shadow_vms_on_most_used_ds = []
        for vm in vm_info.get_shadow_vms(most_used_ds.vm_handles):
            vm_disksize = vm.get_total_disksize() / 1024**3
            # move smaller volumes once the most and least used get closer to avoid oscillation
            vm_maxdisksize = min((least_used_ds.freespace - most_used_ds.freespace) /
                                 (2 * 1024**3), args.ds_volume_max_size)
            if args.ds_volume_min_size <= vm_disksize <= vm_maxdisksize:
                shadow_vms_on_most_used_ds.append(vm)
        if not shadow_vms_on_most_used_ds:
            log.warning(
                "- WARN -  no more shadow vms to move on most used ds {}".format(most_used_ds.name))
            break
        largest_shadow_vm_on_most_used_ds = sort_vms_by_total_disksize(
            shadow_vms_on_most_used_ds)[0]
        move_vmfs_shadow_vm_from_ds_to_ds(most_used_ds, least_used_ds,
                                     largest_shadow_vm_on_most_used_ds)
        moves_done += 1

        # resort the ds by usage in preparation for the next loop iteration
        ds_info.sort_by_usage()


def check_loop(args):
    """
    endless loop of generating move suggestions and wait for the next run
    """
    while True:

        log.info("INFO: starting new loop run")
        if args.dry_run:
            log.info("- INFO - dry-run mode: not doing anything harmful")

        # balance sdd or hdd storage based on cmdline switch
        if args.hdd:
            log.info("- INFO - doing balancing for ssd and hdd storage")
        else:
            log.info("- INFO - doing balancing for ssd storage")

        log.info("- CMND -  # aggregate balancing ssd")

        # open a connection to the vcenter
        vc = VCenterHelper(host=args.vcenter_host,
                           user=args.vcenter_user, password=args.vcenter_password)

        # get the vm and ds info from the vcenter
        vm_info = VMs(vc)
        ds_info = DataStores(vc)
        # get the info from the netapp
        na_info = NAs(vc, args.netapp_user, args.netapp_password, args.region)

        # do the aggregate balancing first
        vmfs_aggr_balancing(na_info, ds_info, vm_info, args, 'ssd')

        log.info("- CMND -  # ds balancing ssd")

        # get the vm and ds info from the vcenter again before doing the ds balancing
        vm_info = VMs(vc)
        ds_info = DataStores(vc)
        # get the info from the netapp again
        na_info = NAs(vc, args.netapp_user, args.netapp_password, args.region)

        vmfs_ds_balancing(na_info, ds_info, vm_info, args, 'ssd')

        if args.hdd:
            log.info("- CMND -  # ds balancing hdd")

            # get the vm and ds info from the vcenter again before doing the ds balancing
            vm_info = VMs(vc)
            ds_info = DataStores(vc)
            # get the info from the netapp again
            na_info = NAs(vc, args.netapp_user, args.netapp_password, args.region)

            vmfs_ds_balancing(na_info, ds_info, vm_info, args, 'hdd')

        # wait the interval time
        log.info("INFO: waiting %s minutes before starting the next loop run", str(
            args.interval))
        time.sleep(60 * int(args.interval))


def main():

    args = parse_commandline()

    log_level = logging.INFO
    if args.debug:
        log_level = logging.DEBUG

    logging.basicConfig(level=log_level, format='%(asctime)-15s %(message)s')

    check_loop(args)


if __name__ == '__main__':
    main()
