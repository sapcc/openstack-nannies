#!/usr/bin/env python3
#
# Copyright (c) 2020 SAP SE
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
import os

from helper.netapp import NetAppHelper
from helper.vcenter import *
from helper.openstack import OpenstackHelper
from helper.prometheus_exporter import *

from pyVim.task import WaitForTask

# prometheus export functionality
from prometheus_client import start_http_server, Gauge

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')

def parse_commandline():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help='dry run option not doing anything harmful')
    parser.add_argument("--vcenter-host", required=True, help="Vcenter hostname")
    parser.add_argument("--vcenter-user", required=True, help="Vcenter username")
    parser.add_argument("--vcenter-password", required=True, help="Vcenter user password")
    parser.add_argument("--netapp-user", required=True, help="Netapp username")
    parser.add_argument("--netapp-password", required=True, help="Netapp user password")
    parser.add_argument("--region", required=True, help="(Openstack) region")
    # 4600 is about 90% of 5tb
    parser.add_argument("--flexvol-size-limit", type=int, required=False, default=4600,
                        help="Maximum size in gb for a healthy flexvol")
    parser.add_argument("--flexvol-lun-min-size", type=int, required=False, default=20,
                        help="Minimum size (>=) in gb for a volume to move for flexvol balancing")
    parser.add_argument("--flexvol-lun-max-size", type=int, required=False, default=1200,
                        help="Maximum size (<) in gb for a volume to move for flexvol balancing")
    parser.add_argument("--flexvol-denylist", nargs='*', required=False, help="ignore those flexvols")
    parser.add_argument("--aggr-lun-min-size", type=int, required=False, default=1200,
                        help="Minimum size (>=) in gb for a volume to move for aggregate balancing")
    # 2050 is about 2tb
    parser.add_argument("--aggr-lun-max-size", type=int, required=False, default=2050,
                        help="Maximum size (<=) in gb for a volume to move for aggregate balancing")
    parser.add_argument("--aggr-denylist", nargs='*', required=False, help="ignore those aggregates")
    parser.add_argument("--max-move-vms", type=int, default=10,
                        help="Maximum number of VMs to (propose to) move")
    parser.add_argument("--min-threshold", type=int, default=60,
                        help="Target aggregate usage must be below this value to do a move")
    parser.add_argument("--max-threshold", type=int, default=70,
                        help="Source aggregate usage must be above this value to do a move")
    parser.add_argument("--max-threshold-hysteresis", type=int, default=5,
                        help="How much to lower the usage below max-threshold")
    parser.add_argument("--interval", type=int, default=360,
                        help="Interval in minutes between check runs")
    parser.add_argument("--prometheus-port", type=int, default=9456,
                        help="Port to run the prometheus exporter for metrics on")
    args = parser.parse_args()
    return args

def prometheus_exporter_setup(args):
    nanny_metrics_data = PromDataClass()
    nanny_metrics = PromMetricsClass()
    nanny_metrics.set_metrics('netapp_balancing_nanny_flexvol_usage',
            'space usage per netapp flexvol in bytes', ['flexvol'])
    nanny_metrics.set_metrics('netapp_balancing_nanny_flexvol_usage_threshold',
            'usage per netapp flexvol above which balancing should be done', ['dummy'])
    nanny_metrics.set_metrics('netapp_balancing_nanny_aggregate_usage',
            'space usage per netapp aggregate in percent', ['aggregate'])
    nanny_metrics.set_metrics('netapp_balancing_nanny_aggregate_usage_threshold',
            'usage per netapp aggregate above which balancing should be done', ['dummy'])
    nanny_metrics.set_metrics('netapp_balancing_nanny_move_suggestions',
            'number of suggested volume moves', ['type', 'attach_state'])
    nanny_metrics.set_metrics('netapp_balancing_nanny_move_suggestions_max',
            'maximum number of suggested volume moves', ['dummy'])
    nanny_metrics.set_metrics('netapp_balancing_nanny_error_count',
            'number of errors we run into during a nanny run', ['error-type'])
    REGISTRY.register(CustomCollector(nanny_metrics, nanny_metrics_data))
    prometheus_http_start(int(args.prometheus_port))
    return nanny_metrics_data

# get all vvol netapps via the vcenter
# TODO: correlate this info with openstack info
def get_netapp_hosts(vc, region):
    # iterate over datastores to find vvols --> netapps
    netapp_hosts = []
    dstores = vc.find_all_of_type(vc.vim.Datastore).view
    for dstore in dstores:
        name = dstore.name.lower()
        if name.startswith("vvol_"):
            # example for the pattern: vvol_bb123
            m = re.match("^(?:vvol)_bb(?P<bb>\d+)$", name)
            bbnum = int(m.group('bb'))
            # one of our netapps is inconsistent in its naming - handle this here
            if bbnum == 56:
                stnpa_num = 0
            else:
                stnpa_num = 1
            # e.g. stnpca1-bb123.cc.<region>.cloud.sap - those are the netapp cluster addresses (..np_c_a1..)
            netapp_name = "stnpca{}-bb{:03d}.cc.{}.cloud.sap".format(stnpa_num, bbnum, region)
            # build a list of netapps
            netapp_hosts.append(netapp_name)

    return netapp_hosts

# return a list of (netapp_host, aggregate-name, percent-used-capacity, size-totoal) per aggregates
def get_aggr_usage_list(nh, netapp_host, aggr_denylist, nanny_metrics_data):
    aggr_usage = []
    # get aggregates
    for aggr in nh.get_aggregate_usage():

        # print info for aggr_denylisted aggregates
        if aggr['aggregate-name'] in aggr_denylist:
            log.info("- INFO -   aggregate {} is aggr_denylist'ed via cmdline".format(aggr['aggregate-name']))

        if aggr['aggr-raid-attributes']['is-root-aggregate'] == 'false' and aggr['aggregate-name'] not in aggr_denylist:
            log.info("- INFO -   aggregate {} of size {:.0f} gb is at {}% utilization"
                .format(aggr['aggregate-name'], int(aggr['aggr-space-attributes']['size-total']) / 1024**3, aggr['aggr-space-attributes']['percent-used-capacity']))
            aggr_usage.append((netapp_host, aggr['aggregate-name'],
                                int(aggr['aggr-space-attributes']['percent-used-capacity']),int(aggr['aggr-space-attributes']['size-total'])))
            nanny_metrics_data.set_data('netapp_balancing_nanny_aggregate_usage', int(aggr['aggr-space-attributes']['percent-used-capacity']),[aggr['aggregate-name']])
    return aggr_usage
    # examples:
    # (netapphost1, aggregate01, 50, 10000000)

# return a list of (netapp_host, flexvol_name, containing-aggregate-name, size-used, size-total) per flexvol
def get_flexvol_usage_list(nh, netapp_host, flexvol_denylist, nanny_metrics_data):
    flexvol_usage = []
    # get flexvols
    for flexvol in nh.get_volume_usage():

        # print info for flexvol_denylisted aggregates
        if flexvol['volume-id-attributes']['name'] in flexvol_denylist:
            log.info("- INFO -   flexvol {} is flexvol_denylist'ed via cmdline".format(flexvol['volume-id-attributes']['name']))

        if flexvol['volume-id-attributes']['name'].lower().startswith('vv0_') and flexvol['volume-id-attributes']['name'] not in flexvol_denylist:
            log.info("- INFO -   flexvol {} of size {:.0f} gb of a total size {:.0f} gb"
                .format(flexvol['volume-id-attributes']['name'], int(flexvol['volume-space-attributes']['size-used']) / 1024**3, int(flexvol['volume-space-attributes']['size-total']) / 1024**3))
            flexvol_usage.append((netapp_host, flexvol['volume-id-attributes']['name'], flexvol['volume-id-attributes']['containing-aggregate-name'],
                                int(flexvol['volume-space-attributes']['size-used']),int(flexvol['volume-space-attributes']['size-total'])))
            nanny_metrics_data.set_data('netapp_balancing_nanny_flexvol_usage', int(flexvol['volume-space-attributes']['size-used']),[flexvol['volume-id-attributes']['name']])
    return flexvol_usage
    # examples:
    # (netapphost1, flexvol01, aggr1, 50, 100)
    # (netapphost1, flexvol02, aggr2, 60, 100)
    # (netapphost1, flexvol03, aggr1, 60, 100)

# generate the vvol datastore name from the name of an aggregate
def bb_name_from_aggregate_name(aggregate_name):
    # example for the pattern: aggr_ssd_bb123_1
    m = re.match("^(?:aggr_ssd_bb)(?P<bb>\d+)_\d$", aggregate_name)
    # example ds_name: BB123
    ds_name = 'BB' + m.group('bb')

    return ds_name

    valid_netapp_ids_flexvol = [vmdk_flexvol[1] for vmdk_flexvol in vmdks_flexvol]

def get_vcenter_info(vc):
    # get all vms from vcenter
    log.info("- INFO -  getting information from the vcenter")
    vm_view = vc.find_all_of_type(vc.vim.VirtualMachine)
    vms = vc.collect_properties(vm_view, vc.vim.VirtualMachine,
                                ['name', 'config.annotation', 'config.hardware.memoryMB', 'config.hardware.numCPU', 'runtime.powerState',
                                 'config.hardware.device'],
                                include_mors=True)
    attached_volumes = []
    for vm in vms:
        # check if this is a shadow vm
        # build list of vvol attachments on non shadow openstack vms
        # the double condition is just for safety - they usually should never both match
        if vc.is_openstack_vm(vm) and not vc.is_shadow_vm(vm) and not vc.is_snapshot_shadow_vm(vm):
            # iterate over all devices
            for device in vm['config.hardware.device']:
                # TODO maybe use the "continue" method to skip non matching?
                # and filter out the virtual disks
                if isinstance(device, vc.vim.vm.device.VirtualDisk):
                    # we are only interested in vvols here
                    if device.backing.fileName.lower().startswith('[vvol_'):
                        # add backingObjectId to our list of attached volumes
                        attached_volumes.append(device.backing.backingObjectId)

    return vms, attached_volumes

# we move the lun/volume between the netapps by telling the vcenter to move the attached
# storage of the corresponding shadow vm to another datastore (i.e. anorther netapp)
def move_shadow_vm(vc, volume_uuid, target_ds, dry_run):
    # vm = vc.get_object_by_name(vim.VirtualMachine, volume_uuid)
    vm = vc.find_server(volume_uuid)
    ds = vc.get_object_by_name(vim.Datastore, target_ds)
    spec = vim.VirtualMachineRelocateSpec()
    spec.datastore = ds
    if not dry_run:
        task = vm.RelocateVM_Task(spec)
        try:
            status = WaitForTask(task,si=vc.api)
        except Exception as e:
            logging.error("- ERROR - failed to move volume %s to data store %s with error message: %s",
                    str(volume_uuid), str(target_ds), str(e.msg))
            return False
        else:
            log.info("- INFO - move of volume %s to data store %s successful with status %s",
                    str(volume_uuid), str(target_ds), str(status))
    return True

# endless loop of generating move suggestions and wait for the next run
def check_loop(args, nanny_metrics_data):
        while True:
            log.info("INFO: starting new loop run")
            if args.dry_run:
                log.info("- INFO - dry-run mode: not doing anything harmful")
            # first blanace out flexvols
            move_suggestions_flexvol(args, nanny_metrics_data)
            # then balance out aggregates
            move_suggestions_aggr(args, nanny_metrics_data)
            # sync metrics to prometheus exporter
            nanny_metrics_data.sync_data()
            # wait the interval time
            log.info("INFO: waiting %s minutes before starting the next loop run", str(args.interval))
            time.sleep(60 * int(args.interval))

# print out suggestions which luns should be moved
# for now this is: suggest to move the largest attached volumes from the fullest netapp aggregate to the emptiest
def move_suggestions_flexvol(args, nanny_metrics_data):

    # TODO this might go maybe as we will not need the metrics in some return cases
    # a counter for move suggestions
    gauge_value_move_suggestions_detached = 0
    gauge_value_move_suggestions_attached = 0
    error_count_not_enough = 0
    # send the empty metric now already in case we are returning before setting a new value
    nanny_metrics_data.set_data('netapp_balancing_nanny_move_suggestions', gauge_value_move_suggestions_detached, ['flexvol', 'detached'])
    nanny_metrics_data.set_data('netapp_balancing_nanny_move_suggestions', gauge_value_move_suggestions_attached, ['flexvol', 'attached'])
    nanny_metrics_data.set_data('netapp_balancing_nanny_error_count', error_count_not_enough,['flexvol_not_enough'])

    # used for log output
    if args.dry_run:
        action_string = "dry-run:"
    else:
        action_string = "action:"

    log.info("- INFO - === flexvol balancing ===")
    # create a connection to openstack
    log.info("- INFO - connecting to openstack in region %s", args.region)
    oh = OpenstackHelper(args.region, os.getenv('OS_USER_DOMAIN_NAME'), os.getenv('OS_PROJECT_DOMAIN_NAME'),
                                                   os.getenv('OS_PROJECT_NAME'), os.getenv('OS_USERNAME'), os.getenv('OS_PASSWORD'))

    vc = VCenterHelper(host=args.vcenter_host, user=args.vcenter_user, password=args.vcenter_password)

    netapp_hosts = get_netapp_hosts(vc, args.region)

    # there are some bbs with only vmfs and no vvols
    if not netapp_hosts:
        log.info("- INFO - netapp flexvol balancing - no vvol datastores found for this vc - giving up")
        return

    # connect to netapp and get the netapp version
    flexvol_usage = []
    aggr_usage = []
    netapps = {}
    for netapp_host in netapp_hosts:
        log.info("- INFO - connecting to netapp %s", netapp_host)
        netapps[netapp_host] = NetAppHelper(host=netapp_host, user=args.netapp_user, password=args.netapp_password)
        nh = netapps[netapp_host]
        vi = nh.get_single("system-get-version")
        log.info("- INFO -  {} is on version {}".format(netapp_host, vi['version']))

        # TODO this can go maybe by changing the function to use an empty list by default
        # make flexvol_denylist an empty list if not set via cmdline
        if args.flexvol_denylist:
            flexvol_denylist = args.flexvol_denylist
        else:
            flexvol_denylist = []

        # TODO this can go maybe by changing the function to use an empty list by default
        # make aggr_denylist an empty list if not set via cmdline
        if args.aggr_denylist:
            aggr_denylist = args.aggr_denylist
        else:
            aggr_denylist = []

        # collect flexvol usage across all netapp hosts
        flexvol_usage += get_flexvol_usage_list(nh, netapp_host, flexvol_denylist, nanny_metrics_data)

        # collect aggregate usage across all netapp hosts
        aggr_usage += get_aggr_usage_list(nh, netapp_host, aggr_denylist, nanny_metrics_data)

    # sort the flexvols top down to start with the biggest ones
    flexvol_usage.sort(key=lambda x: x[3], reverse=True)
    # to keep things simple we only work on the largest flexvol on each nanny run
    flexvol_most_used = flexvol_usage[0]
    # we only have to balance flexvols in case we are over the limit with the largest one
    if flexvol_most_used[3] <= args.flexvol_size_limit * 1024**3:
        log.info("- INFO - all flexvols are fine - nothing to be done")
        return

    # TODO check if containing aggr is not least used aggr
    nh_flexvol_most_used = netapps[flexvol_most_used[0]]
    luns_on_flexvol_most_used = nh_flexvol_most_used.get_luns_for_flexvol(flexvol_most_used[1])
    # sort the luns by used-size
    luns_on_flexvol_most_used.sort(key=lambda x: int(x['size-used']), reverse=True)

    # filter luns for desired size range
    luns_on_flexvol_most_used = [lun for lun in luns_on_flexvol_most_used
        if args.flexvol_lun_min_size * 1024**3 <= int(lun['size-used']) < args.flexvol_lun_max_size * 1024**3]

    # we can only balance if there are any luns to balance on the flexvol within the given min and max regions
    if len(luns_on_flexvol_most_used) == 0:
        log.info("- PLEASE IGNORE - there are no movable volumes within the current min/max limits on flexvol {} - maybe limits should be adjusted?".format(flexvol_most_used[1]))
        return

    # sort the aggregates top down to start with the highest usage percentage
    aggr_usage.sort(key=lambda x: x[2], reverse=True)

    # to keep things simple we only work on the largest aggr on each nanny run
    # find aggr with highest usage, aggr with lowest usage (that is not on highest usage host)
    # TODO see todo above - we need to check that the least used aggr is not the containing aggr of the most used flexvol
    #      in that case we want to use the second least used aggregate
    aggr_most_used = aggr_usage[0]
    aggr_least_used = None
    for aggr in reversed(aggr_usage):
        # make sure we are not on the same netapp and not on the netapp of the source flexvol
        if aggr[0] != aggr_most_used[0] and aggr[0] != flexvol_most_used[0]:
            aggr_least_used = aggr
            break
    # TODO this should be double checked and combined with the todo from above
    else:
        log.error("- ERROR - no aggregate found that is not on the same netapp")
        return 1

    log.info("- INFO - least utilized aggregate is {1} on {0} with a usage of {2}%".format(*aggr_least_used))
    log.info("- INFO - using it as target for balancing volume movements")
    log.info("- INFO - calculating volume move suggestions for automatic flexvol balancing ... this may take a moment")

    # TODO we do not use the comments in the end - we map via vcenter backing
    # <uuid>_brick.vmdk - DATA
    # <uuid>.vmdk - DATA
    # <uuid>_1.vmdk - DATA
    comment_re = re.compile(r"^(?P<vmdk>.*\.vmdk) - DATA$")

    # /vol/vv0_BB123_01/naa.<netapp uuid>.vmdk
    naa_path_re = re.compile(r"^/vol/.*/(?P<naa>naa\..*)\.vmdk$")

    vmdks_flexvol = []
    for lun in luns_on_flexvol_most_used:
        # TODO we do not use the comments in the end - we map via vcenter backing
        # looks like not all luns have a comment
        if lun.get('comment'):
            # does this map to an instance?
            comment_match = comment_re.match(lun['comment'])
            if not comment_match:
                continue
        else:
            continue

        # get the netapp id (naa.xyz...) name
        path_match = naa_path_re.match(lun['path'])
        if not path_match:
            continue

        # shadow vm uuid = volume uuid, netapp id, size-used
        # TODO maybe also add the aggr this is on to keep track of that too ... requires adding that above too
        vmdk_flexvol = (comment_match.group('vmdk'), path_match.group('naa'), int(lun['size-used']), lun['volume'])
        vmdks_flexvol.append(vmdk_flexvol)
        log.debug("==> flexvol vmdk file: {} - netapp id: {} - size {:.0f} gb"
            .format(vmdk_flexvol[0], vmdk_flexvol[1], vmdk_flexvol[2] / 1024**3))

    # limit to the largest --max-move-vms
    # off for debug
    #vmdks_flexvol = vmdks_flexvol[:args.max_move_vms]
    valid_netapp_ids_flexvol = [vmdk_flexvol[1] for vmdk_flexvol in vmdks_flexvol]

    attached_volumes = []
    vms, attached_volumes = get_vcenter_info(vc)

    # index = netapp-id, value = ( vm-name, attach-state )
    vmvmdks_flexvol = dict()
    for vm in vms:
        # the double condition is just for safety - they usually should never both match
        # TODO i think we already check for them to be a shadow vm in get_vcenter_info -> double check
        if vc.is_shadow_vm(vm) and not vc.is_openstack_vm(vm):
            # find disk backing
            for device in vm['config.hardware.device']:
                if isinstance(device, vc.vim.vm.device.VirtualDisk):
                    if device.backing.backingObjectId:
                        # check if this disk references one of our netapp luns (via naa path thingy)
                        if device.backing.backingObjectId in valid_netapp_ids_flexvol:
                            if device.backing.backingObjectId in attached_volumes:
                                vmvmdks_flexvol[device.backing.backingObjectId] = (vm['name'], 'attached')
                            else:
                                vmvmdks_flexvol[device.backing.backingObjectId] = (vm['name'], 'detached')
                        break
        elif vc.is_snapshot_shadow_vm(vm) and not vc.is_openstack_vm(vm):
            # find disk backing
            for device in vm['config.hardware.device']:
                if isinstance(device, vc.vim.vm.device.VirtualDisk):
                    if device.backing.backingObjectId:
                        # check if this disk references one of our netapp luns (via naa path thingy)
                        if device.backing.backingObjectId in valid_netapp_ids_flexvol:
                            if device.backing.backingObjectId in attached_volumes:
                                # not sure if this case is actually possible
                                vmvmdks_flexvol[device.backing.backingObjectId] = (vm['name'], 'snapshot attached')
                            else:
                                vmvmdks_flexvol[device.backing.backingObjectId] = (vm['name'], 'snapshot detached')
                        break
            log.debug("==> snapshot shadow vm - netapp id: {} - volume uuid: {}".format(device.backing.backingObjectId, vm['name']))

    # calculate to which percentage we want to bring down the most used aggregate
    aggr_most_used_target_percentage = args.max_threshold - args.max_threshold_hysteresis
    # like the last one but as absolute size instead of percentage and for the least used aggregate
    aggr_least_used_target_size = int(aggr_least_used[3]) * (aggr_most_used_target_percentage / 100)
    # this is for tracking how much the most used aggregate is used after each lun movement
    aggr_least_used_current_size = int(aggr_least_used[3]) * (int(aggr_least_used[2]) / 100)
    # this is for tracking the size of the flexvol we are moving stuff away from after each lun movement
    flexvol_most_used_current_size = int(flexvol_most_used[3])

    log.info("- PLEASE IGNORE - below might be some debugging output for the planned automatic move of detached volumes")
    for vmdk in vmdks_flexvol:
        # if aggr_least_used_current_size >= aggr_least_used_target_size:
        #     log.info("- INFO - no automatic lun movements as we would fill up {} too much".format(aggr_least_used[1]))
        #     break
        if vmvmdks_flexvol.get(vmdk[1]):
            if vmvmdks_flexvol.get(vmdk[1])[1] == 'detached':
                if oh.api.block_store.get_volume(vmvmdks_flexvol.get(vmdk[1])[0]).attachments:
                    log.info("- PLEASE IGNORE - the volume {} seems to be attached to instance {} meanwhile - doing nothing # size {:.0f} gb".format(vmvmdks_flexvol.get(vmdk[1])[0], oh.api.block_store.get_volume(vmvmdks_flexvol.get(vmdk[1])[0]).attachments[0]['server_id'], vmdk[2] / 1024**3))
                else:
                    # this should be DEBUG later
                    log.debug("==> before - lun size: {:.0f} gb - flexvol usage: {:.0f} gb - target aggr usage: {:.0f} gb".format(vmdk[2] / 1024**3, flexvol_most_used_current_size / 1024**3, aggr_least_used_current_size / 1024**3))
                    log.info("- PLEASE IGNORE - plan: move volume {} from flexvol {} to {} # size {:.0f} gb".format(vmvmdks_flexvol.get(vmdk[1])[0], flexvol_most_used[1], bb_name_from_aggregate_name(aggr_least_used[1]), vmdk[2] / 1024**3))
                    # debug
                    log.info("- PLEASE IGNORE -  {} locking volume {} before moving it".format(action_string, vmvmdks_flexvol.get(vmdk[1])[0]))
                    if not args.dry_run:
                        oh.lock_volume(vmvmdks_flexvol.get(vmdk[1])[0])
                    log.info("- PLEASE IGNORE -   {} moving shadow vm of volume {} to {}".format(action_string, vmvmdks_flexvol.get(vmdk[1])[0], bb_name_from_aggregate_name(aggr_least_used[1])))
                    if not args.dry_run:
                        move_shadow_vm(vc, vmvmdks_flexvol.get(vmdk[1])[0], "vVOL_" + str(bb_name_from_aggregate_name(aggr_least_used[1])))
                    log.info("- PLEASE IGNORE -  {} unlocking volume {} after moving it".format(action_string, vmvmdks_flexvol.get(vmdk[1])[0]))
                    if not args.dry_run:
                        oh.unlock_volume(vmvmdks_flexvol.get(vmdk[1])[0])

                    # trying to keep track of the actual usage of the participating flexvols and aggregates
                    flexvol_most_used_current_size -= int(vmdk[2])
                    #aggr_least_used_current_size += vmdk[2]
                    aggr_least_used_current_size += int(vmdk[2])
                    # this should be DEBUG later
                    log.debug("==> after - lun size: {:.0f} gb - flexvol usage: {:.0f} gb - target aggr usage: {:.0f} gb".format(vmdk[2] / 1024**3, flexvol_most_used_current_size / 1024**3, aggr_least_used_current_size / 1024**3))
                    gauge_value_move_suggestions_detached += 1
                    if gauge_value_move_suggestions_detached >= args.max_move_vms:
                        log.info("- PLEASE IGNORE - max-move-vms of {} reached - stopping here".format(args.max_move_vms))
                        break
                    if aggr_least_used_current_size >= aggr_least_used_target_size:
                        log.info("- PLEASE IGNORE - further movements would fill up {} too much - stopping here".format(bb_name_from_aggregate_name(aggr_least_used[1])))
                        break
                    if flexvol_most_used_current_size < (args.flexvol_size_limit * 1024**3):
                        log.info("- PLEASE IGNORE - the size of the flexvol {} is below the limit of {:.0f} gb now - stopping here".format(flexvol_most_used[1], args.flexvol_size_limit))
                        break
    if flexvol_most_used_current_size > (args.flexvol_size_limit * 1024**3):
        log.info("- PLEASE IGNORE - there are not enough (or no) detached volumes within the current min/max limits to bring the flexvol {} below the limit of {:.0f} gb - stopping here".format(flexvol_most_used[1], args.flexvol_size_limit))
        error_count_not_enough += 1

    # when the balancing goal is reached an "optional" string is appended to the recommendations
    optional_string = ''
    overload_warning_printed = 0
    log.info("- INFO - volume move target size for flexvol {} is to get below {:.0f} gb".format(flexvol_most_used[1], args.flexvol_size_limit))
    log.info("- INFO - volume move suggestions for manual flexvol balancing (from largest in range to smallest):")
    # go through all the shadow vms found on the netapp
    for vmdk in vmdks_flexvol:
        # check if they actually exist in the vcenter
        if vmvmdks_flexvol.get(vmdk[1]):
            # for attached volumes a move suggestion is printed out
            if vmvmdks_flexvol.get(vmdk[1])[1] == 'attached':
                if (aggr_least_used_current_size >= aggr_least_used_target_size) and (overload_warning_printed == 0):
                    # stop if the aggregate we move to gets too full
                    log.info("- IMPORTANT - please stop with movements here as we would fill up {} too much".format(aggr_least_used[1]))
                    optional_string = ' (no move)'
                    overload_warning_printed = 1
                if (flexvol_most_used_current_size < (args.flexvol_size_limit * 1024**3)) and (optional_string == ''):
                    optional_string = ' (optional)'
                # this should be DEBUG later
                log.debug("==> before - lun size: {:.0f} gb - flexvol usage: {:.0f} gb - target aggr usage: {:.0f} gb".format(vmdk[2] / 1024**3, flexvol_most_used_current_size / 1024**3, aggr_least_used_current_size / 1024**3))
                # print out info for the manual volume move
                log.info("- INFO - netapp flexvol balancing - ./svmotion_cinder_v2.py {} vVOL_{} # from flexvol {} on {} - size {:.0f} gb{}".format(vmvmdks_flexvol.get(vmdk[1])[0], bb_name_from_aggregate_name(aggr_least_used[1]), flexvol_most_used[1], flexvol_most_used[2], vmdk[2] / 1024**3, optional_string))
                # trying to keep track of the actual usage of the participating flexvols and aggregates
                flexvol_most_used_current_size -= int(vmdk[2])
                aggr_least_used_current_size += int(vmdk[2])
                # this should be DEBUG later
                log.debug("==> after - lun size: {:.0f} gb - flexvol usage: {:.0f} gb - target aggr usage: {:.0f} gb".format(vmdk[2] / 1024**3, flexvol_most_used_current_size / 1024**3, aggr_least_used_current_size / 1024**3))
                gauge_value_move_suggestions_attached += 1
                if gauge_value_move_suggestions_attached >= args.max_move_vms:
                    log.info("- IMPORTANT - please stop with movements - max-move-vms of {} reached".format(args.max_move_vms))
                    optional_string = ' (no move)'

    if flexvol_most_used_current_size > (args.flexvol_size_limit * 1024**3):
        log.info("- INFO - there are not enough (or no) attached volumes within the current min/max limits to bring the flexvol {} below the limit of {:.0f} gb - maybe limits should be adjusted?".format(flexvol_most_used[1], args.flexvol_size_limit))
        error_count_not_enough += 1

    nanny_metrics_data.set_data('netapp_balancing_nanny_move_suggestions', gauge_value_move_suggestions_detached,['flexvol', 'detached'])
    nanny_metrics_data.set_data('netapp_balancing_nanny_move_suggestions', gauge_value_move_suggestions_attached,['flexvol', 'attached'])
    nanny_metrics_data.set_data('netapp_balancing_nanny_error_count', error_count_not_enough,['flexvol_not_enough'])

# print out suggestions which volumes should be moved
# for now this is: suggest to move the largest attached volumes from the fullest netapp aggregate to the emptiest
def move_suggestions_aggr(args, nanny_metrics_data):

    # a counter for move suggestions
    gauge_value_move_suggestions_detached = 0
    gauge_value_move_suggestions_attached = 0
    error_count_not_enough = 0
    # send the empty metric now already in case we are returning before setting a new value
    nanny_metrics_data.set_data('netapp_balancing_nanny_move_suggestions', gauge_value_move_suggestions_detached, ['aggregate', 'detached'])
    nanny_metrics_data.set_data('netapp_balancing_nanny_move_suggestions', gauge_value_move_suggestions_attached, ['aggregate', 'attached'])
    nanny_metrics_data.set_data('netapp_balancing_nanny_error_count', error_count_not_enough,['aggregate_not_enough'])

    # used for log output
    if args.dry_run:
        action_string = "dry-run:"
    else:
        action_string = "action:"

    log.info("- INFO - === aggregate balancing ===")
    # create a connection to openstack
    log.info("- INFO - connecting to openstack in region %s", args.region)
    oh = OpenstackHelper(args.region, os.getenv('OS_USER_DOMAIN_NAME'), os.getenv('OS_PROJECT_DOMAIN_NAME'),
                                                   os.getenv('OS_PROJECT_NAME'), os.getenv('OS_USERNAME'), os.getenv('OS_PASSWORD'))

    vc = VCenterHelper(host=args.vcenter_host, user=args.vcenter_user, password=args.vcenter_password)

    netapp_hosts = get_netapp_hosts(vc, args.region)

    # there are some bbs with only vmfs and no vvols
    if not netapp_hosts:
        log.info("- INFO - netapp aggregate balancing - no vvol datastores found for this vc - giving up")
        return

    # connect to netapp and get the netapp version
    aggr_usage = []
    netapps = {}
    for netapp_host in netapp_hosts:
        log.info("- INFO - connecting to netapp %s", netapp_host)
        netapps[netapp_host] = NetAppHelper(host=netapp_host, user=args.netapp_user, password=args.netapp_password)
        nh = netapps[netapp_host]
        vi = nh.get_single("system-get-version")
        log.info("- INFO -  {} is on version {}".format(netapp_host, vi['version']))

        # make aggr_denylist an empty list if not set via cmdline
        if args.aggr_denylist:
            aggr_denylist = args.aggr_denylist
        else:
            aggr_denylist = []

        # collect aggregate usage across all netapp hosts
        aggr_usage += get_aggr_usage_list(nh, netapp_host, aggr_denylist, nanny_metrics_data)

    # sort the aggregates top down to start with the highest usage percentage
    aggr_usage.sort(key=lambda x: x[2], reverse=True)
    # find aggr with highest usage, aggr with lowest usage (that is not on highest usage host)
    aggr_most_used = aggr_usage[0]
    aggr_least_used = None
    for aggr in reversed(aggr_usage):
        if aggr[0] != aggr_most_used[0]:
            aggr_least_used = aggr
            break
    else:
        log.error("- ERROR - no aggregate found that is not on the same netapp")
        return 1

    log.info("- INFO - most utilized aggregate is {1} on {0} with a usage of {2}%".format(*aggr_most_used))
    log.info("- INFO - least utilized aggregate is {1} on {0} with a usage of {2}%".format(*aggr_least_used))

    if int(aggr_most_used[2]) < args.max_threshold:
        log.info("- INFO - netapp aggregate balancing - usage of most used source aggregate {} of {}% is below threshold of {}% - doing nothing".format(aggr_most_used[1], aggr_most_used[2], args.max_threshold))
        return

    if int(aggr_least_used[2]) > args.min_threshold:
        log.info("- INFO - netapp aggregate balancing - usage of least used target aggregate {} of {}% is above threshold of {}% - doing nothing".format(aggr_least_used[1], aggr_least_used[2], args.min_threshold))
        return

    log.info("- INFO - calculating volume move suggestions for automatic aggregate balancing ... this may take a moment")

    log.info("- INFO -  getting luns from the netapp")
    nh_most_used = netapps[aggr_most_used[0]]
    luns = nh_most_used.get_luns_for_aggr(aggr_most_used[1], "vv0_BB")

    # filter luns for desired size range
    luns = [lun for lun in luns
            if args.aggr_lun_min_size * 1024**3 <= int(lun['size-used']) <= args.aggr_lun_max_size * 1024**3]
    luns.sort(key=lambda lun: int(lun['size-used']), reverse=True)

    # we can only balance if there are any luns to balance on the aggregate within the given min and max regions
    if len(luns) == 0:
        log.info("- IMPORTANT - there are no movable volumes within the current min/max limits on aggregate {} - maybe limits should be adjusted?".format(aggr_most_used[1]))
        return

    # NOTE we do not use the comments in the end - we map via vcenter backing
    # <uuid>_brick.vmdk - DATA
    # <uuid>.vmdk - DATA
    # <uuid>_1.vmdk - DATA
    comment_re = re.compile(r"^(?P<vmdk>.*\.vmdk) - DATA$")

    # /vol/vv0_BB123_01/naa.<netapp uuid>.vmdk
    naa_path_re = re.compile(r"^/vol/.*/(?P<naa>naa\..*)\.vmdk$")

    vmdks = []
    for lun in luns:
        # TODO i think we no longer use the comments and map via vcenter
        # looks like not all luns have a comment
        if lun.get('comment'):
            # does this map to an instance?
            comment_match = comment_re.match(lun['comment'])
            if not comment_match:
                continue
        else:
            continue

        # get the netapp id (naa.xyz...) name
        path_match = naa_path_re.match(lun['path'])
        if not path_match:
            continue

        vmdk = (comment_match.group('vmdk'), path_match.group('naa'), int(lun['size-used']))
        vmdks.append(vmdk)
        log.debug("==> vmdk file: {} - netapp id: {} - size {:.0f} gb"
              .format(vmdk[0], vmdk[1], vmdk[2] / 1024**3))

    # limit to the largest --max-move-vms
    #vmdks = vmdks[:args.max_move_vms]
    valid_netapp_ids = [vmdk[1] for vmdk in vmdks]

    attached_volumes = []
    vms, attached_volumes = get_vcenter_info(vc)

    # index = netapp-id, value = ( vm-name, attach-state )
    vmvmdks = dict()
    vmvmdks_flexvol = dict()
    for vm in vms:
        # the double condition is just for safety - they usually should never both match
        if vc.is_shadow_vm(vm) and not vc.is_openstack_vm(vm):
            # find disk backing
            for device in vm['config.hardware.device']:
                if isinstance(device, vc.vim.vm.device.VirtualDisk):
                    if device.backing.backingObjectId:
                        # check if this disk references one of our netapp luns (via naa path thingy)
                        if device.backing.backingObjectId in valid_netapp_ids:
                            if device.backing.backingObjectId in attached_volumes:
                                vmvmdks[device.backing.backingObjectId] = (vm['name'], 'attached')
                            else:
                                vmvmdks[device.backing.backingObjectId] = (vm['name'], 'detached')
                        break
        elif vc.is_snapshot_shadow_vm(vm) and not vc.is_openstack_vm(vm):
            # find disk backing
            for device in vm['config.hardware.device']:
                if isinstance(device, vc.vim.vm.device.VirtualDisk):
                    if device.backing.backingObjectId:
                        # check if this disk references one of our netapp luns (via naa path thingy)
                        if device.backing.backingObjectId in valid_netapp_ids:
                            if device.backing.backingObjectId in attached_volumes:
                                # not sure if this case is actually possible
                                vmvmdks[device.backing.backingObjectId] = (vm['name'], 'snapshot attached')
                            else:
                                vmvmdks[device.backing.backingObjectId] = (vm['name'], 'snapshot detached')
                        break
            log.debug("==> snapshot shadow vm - netapp id: {} - volume uuid: {}".format(device.backing.backingObjectId, vm['name']))

    # calculate to which percentage we want to bring down the most used aggregate
    aggr_most_used_target_percentage = args.max_threshold - args.max_threshold_hysteresis
    # like the last one but as absolute size instead of percentage
    aggr_most_used_target_size = int(aggr_most_used[3]) * (aggr_most_used_target_percentage / 100)
    # like the last one but for the least used aggregate
    aggr_least_used_target_size = int(aggr_least_used[3]) * (aggr_most_used_target_percentage / 100)
    # this is for tracking how much the most used aggregate is used after each lun movement
    aggr_most_used_current_size = int(aggr_most_used[3]) * (int(aggr_most_used[2]) / 100)
    # this is for tracking how much the most used aggregate is used after each lun movement
    aggr_least_used_current_size = int(aggr_least_used[3]) * (int(aggr_least_used[2]) / 100)

    log.info("- PLEASE IGNORE - below might be some debugging output for the planned automatic move of detached volumes")
    for vmdk in vmdks:
        # if aggr_least_used_current_size >= aggr_least_used_target_size:
        #     log.info("- INFO - no automatic lun movements as we would fill up {} too much".format(aggr_least_used[1]))
        #     break
        if vmvmdks.get(vmdk[1]):
            if vmvmdks.get(vmdk[1])[1] == 'detached':
                if oh.api.block_store.get_volume(vmvmdks.get(vmdk[1])[0]).attachments:
                    log.info("- PLEASE IGNORE - the volume {} seems to be attached to instance {} meanwhile - doing nothing # size {:.0f} gb".format(vmvmdks.get(vmdk[1])[0], oh.api.block_store.get_volume(vmvmdks.get(vmdk[1])[0]).attachments[0]['server_id'], vmdk[2] / 1024**3))
                else:
                    log.info("- PLEASE IGNORE - plan: move volume {} from {} to {} # size {:.0f} gb".format(vmvmdks.get(vmdk[1])[0], bb_name_from_aggregate_name(aggr_most_used[1]), bb_name_from_aggregate_name(aggr_least_used[1]), vmdk[2] / 1024**3))
                    # debug
                    log.info("- PLEASE IGNORE -  {} locking volume {} before moving it".format(action_string, vmvmdks.get(vmdk[1])[0]))
                    if not args.dry_run:
                        oh.lock_volume(vmvmdks.get(vmdk[1])[0])
                    log.info("- PLEASE IGNORE -   {} moving shadow vm of volume {} to {}".format(action_string, vmvmdks.get(vmdk[1])[0], bb_name_from_aggregate_name(aggr_least_used[1])))
                    if not args.dry_run:
                        move_shadow_vm(vc, vmvmdks.get(vmdk[1])[0], "vVOL_" + str(bb_name_from_aggregate_name(aggr_least_used[1])))
                    log.info("- PLEASE IGNORE -  {} unlocking volume {} after moving it".format(action_string, vmvmdks.get(vmdk[1])[0]))
                    if not args.dry_run:
                        oh.unlock_volume(vmvmdks.get(vmdk[1])[0])

                    # trying to keep track of the actual usage of the participating aggregates
                    aggr_most_used_current_size -= vmdk[2]
                    aggr_least_used_current_size += vmdk[2]
                    gauge_value_move_suggestions_detached += 1
                    if gauge_value_move_suggestions_detached == args.max_move_vms:
                        log.info("- PLEASE IGNORE - max-move-vms of {} reached - stopping here".format(args.max_move_vms))
                        break
                    if aggr_least_used_current_size >= aggr_least_used_target_size:
                        log.info("- PLEASE IGNORE - further movements would fill up {} too much - stopping here".format(bb_name_from_aggregate_name(aggr_least_used[1])))
                    break
    if aggr_most_used_current_size > aggr_most_used_target_size:
        log.info("- PLEASE IGNORE - there are not enough (or no) detached volumes within the current min/max limits to bring the aggregate {} below the limit of {:.0f} gb - stopping here".format(aggr_most_used[1], aggr_most_used_target_size))
        error_count_not_enough += 1

    # when the balancing goal is reached an "optional" string is appended to the recommendations
    optional_string = ''
    overload_warning_printed = 0
    log.info("- INFO - volume move target usage for aggregate {} is {:.0f}% corresponding to a size of {:.0f} gb".format(aggr_most_used[1], aggr_most_used_target_percentage, aggr_most_used_target_size / 1024**3))
    log.info("- INFO - volume move suggestions for manual aggregate balancing (from largest in range to smallest):")
    # go through all the shadow vms found on the netapp
    for vmdk in vmdks:
        # check if they actually exist in the vcenter
        if vmvmdks.get(vmdk[1]):
            # for attached volumes a move suggestion is printed out
            if vmvmdks.get(vmdk[1])[1] == 'attached':
                if (aggr_least_used_current_size >= aggr_least_used_target_size) and (overload_warning_printed == 0):
                    # stop if the aggregate we move to gets too full
                    log.info("- IMPORTANT - please stop with movements here as we would fill up {} too much".format(aggr_least_used[1]))
                    optional_string = ' (no move)'
                    overload_warning_printed = 1
                if (aggr_most_used_current_size < aggr_most_used_target_size) and (optional_string == ''):
                    optional_string = ' (optional)'
                # this should be DEBUG later
                log.debug("==> before - lun size: {:.0f} gb - source aggr usage: {:.0f} gb - target aggr usage: {:.0f} gb".format(vmdk[2] / 1024**3, aggr_most_used_current_size / 1024**3, aggr_least_used_current_size / 1024**3))
                # trying to keep track of the actual usage of the participating aggregates
                aggr_most_used_current_size -= vmdk[2]
                aggr_least_used_current_size += vmdk[2]
                gauge_value_move_suggestions_attached += 1
                # print out info for the manual volume move
                log.info("- INFO - netapp aggregate balancing - ./svmotion_cinder_v2.py {} vVOL_{} # from {} - size {:.0f} gb{}".format(vmvmdks.get(vmdk[1])[0], bb_name_from_aggregate_name(aggr_least_used[1]), aggr_most_used[1], vmdk[2] / 1024**3, optional_string))
                # this should be DEBUG later
                log.debug("==> after - lun size: {:.0f} gb - source aggr usage: {:.0f} gb - target aggr usage: {:.0f} gb".format(vmdk[2] / 1024**3, aggr_most_used_current_size / 1024**3, aggr_least_used_current_size / 1024**3))
                if gauge_value_move_suggestions_attached >= args.max_move_vms:
                    log.info("- IMPORTANT - please stop with movements - max-move-vms of {} reached".format(args.max_move_vms))
                    optional_string = ' (no move)'

    if aggr_most_used_current_size > aggr_most_used_target_size:
        log.info("- INFO - there are not enough (or no) attached volumes within the current min/max limits to bring the aggregate {} below the limit of {:.0f} gb - maybe limits should be adjusted?".format(aggr_most_used[1], aggr_most_used_target_size / 1024**3))
        error_count_not_enough += 1

    nanny_metrics_data.set_data('netapp_balancing_nanny_move_suggestions', gauge_value_move_suggestions_detached,['aggregate', 'detached'])
    nanny_metrics_data.set_data('netapp_balancing_nanny_move_suggestions', gauge_value_move_suggestions_attached,['aggregate', 'attached'])
    nanny_metrics_data.set_data('netapp_balancing_nanny_error_count', error_count_not_enough,['aggregate_not_enough'])


def main():

    args = parse_commandline()

    nanny_metrics_data = prometheus_exporter_setup(args)

    # set some fixed threshold value metrics based on the cmdline args
    nanny_metrics_data.set_data('netapp_balancing_nanny_flexvol_usage_threshold', args.flexvol_size_limit,['dummy'])
    nanny_metrics_data.set_data('netapp_balancing_nanny_aggregate_usage_threshold', args.max_threshold,['dummy'])
    nanny_metrics_data.set_data('netapp_balancing_nanny_move_suggestions_max', args.max_move_vms,['dummy'])

    check_loop(args, nanny_metrics_data)


if __name__ == '__main__':
    main()
