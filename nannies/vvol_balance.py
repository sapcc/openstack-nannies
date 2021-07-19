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
    parser.add_argument("--ds-volume-min-size", type=int, required=False, default=0,
                        help="Minimum size (>=) in gb for a volume to move for ds balancing")
    parser.add_argument("--ds-volume-max-size", type=int, required=False, default=2500,
                        help="Maximum size (<=) in gb for a volume to move for ds balancing")
    # 4600 is about 90% of 5tb
    parser.add_argument("--flexvol-max-size", type=int, required=False, default=4600,
                        help="Maximum size (<=) in gb a flexvol should have")
    # 6200 means 75% (see below) of it still gives the about 90% for the old 5tb volumes
    parser.add_argument("--flexvol-min-size", type=int, required=False, default=6200,
                        help="Minimum size a flexvol should have to be considered for balancing")
    parser.add_argument("--flexvol-max-usage", type=int, required=False, default=70,
                        help="Maximum usage in percent a flexvol should have")
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


def vvol_aggr_balancing(na_info, ds_info, vm_info, args):
    """
    balance the usage of the underlaying aggregates of vvol ds
    """

    # balancing loop
    moves_done = 0
    moved_size = 0
    while True:

        if moves_done > args.max_move_vms:
            log.info(
                "- INFO - max number of vms to move ({}) reached - stopping aggr balancing now".format(args.max_move_vms))
            break

        # get the aggr usage info
        all_aggr_list_sorted, avg_aggr_usage = get_aggr_usage(na_info, 'vvol')

        # try to map the aggr to a vvol ds in the vc to rule out aggr we do not have connected as vvol
        all_vvol_aggr_list_sorted = []
        for aggr in all_aggr_list_sorted:
            if ds_info.get_by_name(aggr_name_to_ds_name(aggr.host, aggr.name)):
                all_vvol_aggr_list_sorted.append(aggr)
            else:
                log.info("- INFO - the aggregate {} on the netapp does not seem to have a vvol ds in the vc".format(aggr.name))

        if len(all_vvol_aggr_list_sorted) == 0:
            log.info("- INFO - no vvol aggegates found ...")
            return False

        min_usage_aggr = all_vvol_aggr_list_sorted.pop(0)
        max_usage_aggr = all_vvol_aggr_list_sorted[-1]

        # TODO: does this one really make sense?
        if len(min_usage_aggr.luns) == 0:
            log.warning("- WARN - min usage vvol aggr {} does not seem to have any luns/ds on it".format(min_usage_aggr.name))
            break

        if len(max_usage_aggr.luns) == 0:
            log.warning("- WARN - max usage vvol aggr {} does not seem to have any luns/ds on it".format(min_usage_aggr.name))
            break

        # only do aggr balancing if max aggr usage is more than --autopilot-range % above the avg
        if max_usage_aggr.usage < avg_aggr_usage + args.autopilot_range:
            log.info("- INFO -  max usage vvol aggr {} is still within the autopilot range above avg aggr usage ({:.0f}+{:.0f}%) - no aggr balancing required".format(max_usage_aggr.name, avg_aggr_usage, args.autopilot_range))
            return False
        else:
            log.info(
                "- INFO -  max usage vvol aggr {} is more than the autopilot range above avg aggr usage ({:.0f}+{:.0f}%) - aggr balancing required".format(max_usage_aggr.name, avg_aggr_usage, args.autopilot_range))

        # find potential source vols for balancing: from max used aggr and vvol
        balancing_source_luns = []
        log.debug("- DEBG -  volumes on the max usage aggr {}:".format(max_usage_aggr.name))
        for lun in max_usage_aggr.luns:
            # we only care for vvol here
            if lun.type != 'vvol':
                continue
            log.debug("- DEBG -   {} - {:.0f}G".format(lun.name, lun.used/1024**3))
            balancing_source_luns.append(lun)
        balancing_source_luns.sort(key=lambda lun: lun.used)

        balancing_target_ds = ds_info.get_by_name(aggr_name_to_ds_name(min_usage_aggr.host, min_usage_aggr.name))
        if not balancing_target_ds:
            log.warning("- WARN - the min usage aggregate {} on the netapp does not seem to have a ds in the vc - this should not happen ...".format(min_usage_aggr.name))
            return False

        log.info("- INFO -  vc ds corresponding to the min usage aggr {}: {}".format(min_usage_aggr.name, balancing_target_ds.name))
        log.debug("- DEBG -  flexvols on the min usage aggr {}:".format(min_usage_aggr.name))
        # for debugging
        for fvol in min_usage_aggr.fvols:
            # we only care for vvol here
            if fvol.type != 'vvol':
                continue
            log.debug("- wDEBG -   {} - {:.0f}G".format(fvol.name, fvol.used/1024**3))

        if len(balancing_source_luns) == 0:
            log.warning("- WARN - no volumes on the most used aggregate {} - this should not happen ...".format(max_usage_aggr.name))
            return False

        shadow_luns_and_vms_on_most_used_ds_on_most_used_aggr = []
        for lun in balancing_source_luns:
            if lun.name in vm_info.vvol_shadow_vms_for_naaids.keys():
                shadow_luns_and_vms_on_most_used_ds_on_most_used_aggr.append((lun, vm_info.vvol_shadow_vms_for_naaids[lun.name]))

        shadow_luns_and_vms_on_most_used_ds_on_most_used_aggr_ok= []
        for lun_and_vm in shadow_luns_and_vms_on_most_used_ds_on_most_used_aggr:
            vm = lun_and_vm[1]
            # TODO: maybe it would be better to use the lun used size here instead?
            vm_disksize = vm.get_total_disksize() / 1024**3
            # if args.aggr_volume_min_size <= vm_disksize <= min(least_used_ds_free_space / 1024**3, args.aggr_volume_max_size):
            if args.aggr_volume_min_size <= vm_disksize <= args.aggr_volume_max_size:
                shadow_luns_and_vms_on_most_used_ds_on_most_used_aggr_ok.append(lun_and_vm)
        if not shadow_luns_and_vms_on_most_used_ds_on_most_used_aggr_ok:
            log.warning(
                "- WARN - no more shadow vms to move on most used ds {} on most used aggr {}".format(aggr_name_to_ds_name(max_usage_aggr.host, max_usage_aggr.name), max_usage_aggr.name))
            break

        # sort them by lun used size
        shadow_luns_and_vms_on_most_used_ds_on_most_used_aggr_ok = sorted(shadow_luns_and_vms_on_most_used_ds_on_most_used_aggr_ok, key=lambda lun_and_vm: lun_and_vm[0].used, reverse=True)

        largest_shadow_lun_and_vm_on_most_used_ds_on_most_used_aggr = shadow_luns_and_vms_on_most_used_ds_on_most_used_aggr_ok[0]
        move_vvol_shadow_vm_from_aggr_to_aggr(ds_info, max_usage_aggr, min_usage_aggr,
                                                largest_shadow_lun_and_vm_on_most_used_ds_on_most_used_aggr[0],
                                                largest_shadow_lun_and_vm_on_most_used_ds_on_most_used_aggr[1])
        moves_done += 1

def vvol_flexvol_balancing(na_info, ds_info, vm_info, args):
    """
    balance the usage of the flexvols on the netapp
    """

    # get all flexvols on all na
    all_fvols = []
    for na in na_info.elements:
        if na.na_fvol_elements != []:
            for fvol in na.na_fvol_elements:
                if fvol.type == 'vvol':
                    all_fvols.append(fvol)

    # we only care for fvols above the limit
    # too_large_fvols = [ fvol for fvol in all_fvols if fvol.used / 1024**3 > args.flexvol_max_size ]
    too_large_fvols = [ fvol for fvol in all_fvols if fvol.used / 1024**3 > args.flexvol_min_size and fvol.usage > args.flexvol_max_usage ]

    # sort them by used size
    too_large_fvols = sorted(too_large_fvols, key=lambda fvol: fvol.used, reverse=True)

    # balancing loop
    moves_done = 0
    moved_size = 0
    while True:

        if moves_done > args.max_move_vms:
            log.info(
                "- INFO - max number of vms to move ({}) reached - stopping flexvol balancing now".format(args.max_move_vms))
            break

        if len(too_large_fvols) == 0:
            log.info(
                "- INFO - there are no too large flexvols to balance - stopping flexvol balancing now")
            break

        # pick and take away the largest too large flexvol
        most_used_too_large_fvol = too_large_fvols.pop(0)

        # get the aggr usage info
        all_aggr_list_sorted, avg_aggr_usage = get_aggr_usage(na_info, 'vvol')

        # try to map the aggr to a vvol ds in the vc to rule out aggr we do not have connected as vvol
        all_vvol_aggr_list_sorted = []
        for aggr in all_aggr_list_sorted:
            if ds_info.get_by_name(aggr_name_to_ds_name(aggr.host, aggr.name)):
                all_vvol_aggr_list_sorted.append(aggr)
            else:
                log.info("- INFO - the aggregate {} on the netapp does not seem to have a vvol ds in the vc".format(aggr.name))

        if len(all_vvol_aggr_list_sorted) == 0:
            log.info("- INFO - no vvol aggegates found ...")
            return False

        min_usage_aggr = all_vvol_aggr_list_sorted.pop(0)
        max_usage_aggr = all_vvol_aggr_list_sorted[-1]

        # TODO: does this one really make sense?
        if len(min_usage_aggr.luns) == 0:
            log.warning("- WARN - min usage vvol aggr {} does not seem to have any luns/ds on it".format(min_usage_aggr.name))
            return False

        if len(max_usage_aggr.luns) == 0:
            log.warning("- WARN - max usage vvol aggr {} does not seem to have any luns/ds on it".format(min_usage_aggr.name))
            return False

        # make sure we are not on the least used aggr where we want to move to
        # in this case move to the next least used aggr as target and so on ...
        while aggr_name_to_ds_name(most_used_too_large_fvol.host, most_used_too_large_fvol.aggr) == aggr_name_to_ds_name(min_usage_aggr.host, min_usage_aggr.name):
            log.info("- INFO - most used too large flexvol {} is on ds {} of least used vvol aggr {} - trying the next aggr".format(most_used_too_large_fvol.name, aggr_name_to_ds_name(min_usage_aggr.host, min_usage_aggr.name), min_usage_aggr.name))
            # take the next more used one as new min used aggr
            min_usage_aggr = all_vvol_aggr_list_sorted.pop(0)
            # if nothing is left , then min used aggr = max used aggr and we do not want to move things there
            if len(all_vvol_aggr_list_sorted) <= 1:
                log.warning("- WARN - only the max used vvol aggr is left as move target and we do not want to move anything there - giving up")
                break

        if len(most_used_too_large_fvol.luns) == 0:
            log.warning("- WARN - no volumes on the most used flexvol {} - this should not happen ...".format(most_used_too_large_fvol.name))
            return False

        shadow_luns_and_vms_on_most_used_fvol_on_most_used_aggr = []
        for lun in most_used_too_large_fvol.luns:
            if lun.name in vm_info.vvol_shadow_vms_for_naaids.keys():
                shadow_luns_and_vms_on_most_used_fvol_on_most_used_aggr.append((lun, vm_info.vvol_shadow_vms_for_naaids[lun.name]))

        shadow_luns_and_vms_on_most_used_fvol_on_most_used_aggr_ok= []
        for lun_and_vm in shadow_luns_and_vms_on_most_used_fvol_on_most_used_aggr:
            vm = lun_and_vm[1]
            # TODO: maybe it would be better to use the lun used size here instead?
            vm_disksize = vm.get_total_disksize() / 1024**3
            # if args.aggr_volume_min_size <= vm_disksize <= min(least_used_ds_free_space / 1024**3, args.aggr_volume_max_size):
            # TODO: args param to be renamed from _ds_ to _fvol_
            if args.ds_volume_min_size <= vm_disksize <= args.ds_volume_max_size:
                shadow_luns_and_vms_on_most_used_fvol_on_most_used_aggr_ok.append(lun_and_vm)
        if not shadow_luns_and_vms_on_most_used_fvol_on_most_used_aggr_ok:
            log.warning(
                "- WARN -  no more shadow vms to move on most used fvol {} on most used vvol aggr {}".format(most_used_too_large_fvol.name, max_usage_aggr.name))
            break

        # sort them by lun used size
        shadow_luns_and_vms_on_most_used_fvol_on_most_used_aggr_ok = sorted(shadow_luns_and_vms_on_most_used_fvol_on_most_used_aggr_ok, key=lambda lun_and_vm: lun_and_vm[0].used, reverse=True)

        largest_shadow_lun_and_vm_on_most_used_fvol_on_most_used_aggr = shadow_luns_and_vms_on_most_used_fvol_on_most_used_aggr_ok[0]
        log.info("- INFO - current usage of too large fvol {} is {:.0f} gb".format(most_used_too_large_fvol.name, most_used_too_large_fvol.used / 1024**3))
        move_vvol_shadow_vm_from_aggr_to_aggr(ds_info, na_info.get_aggr_by_name(most_used_too_large_fvol.host, most_used_too_large_fvol.aggr), min_usage_aggr,
                                                largest_shadow_lun_and_vm_on_most_used_fvol_on_most_used_aggr[0],
                                                largest_shadow_lun_and_vm_on_most_used_fvol_on_most_used_aggr[1])
        moves_done += 1


def check_loop(args):
    """
    endless loop of generating move suggestions and wait for the next run
    """
    while True:

        log.info("INFO: starting new loop run")
        if args.dry_run:
            log.info("- INFO - dry-run mode: not doing anything harmful")

        # open a connection to the vcenter
        vc = VCenterHelper(host=args.vcenter_host,
                           user=args.vcenter_user, password=args.vcenter_password)

        log.info("- CMND -  # flexvol balancing")

        # get the vm and ds info from the vcenter again before doing the ds balancing
        vm_info = VMs(vc)
        ds_info = DataStores(vc)
        # get the info from the netapp again
        na_info = NAs(vc, args.netapp_user, args.netapp_password, args.region)

        vvol_flexvol_balancing(na_info, ds_info, vm_info, args)

        log.info("- CMND -  # aggregate balancing")

        # get the vm and ds info from the vcenter
        vm_info = VMs(vc)
        ds_info = DataStores(vc)
        # get the info from the netapp
        na_info = NAs(vc, args.netapp_user, args.netapp_password, args.region)

        # do the aggregate balancing first
        vvol_aggr_balancing(na_info, ds_info, vm_info, args)

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
