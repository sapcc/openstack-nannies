
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
import os, argparse, re
from helper.vcenter import *
from helper import openstack
from pyVim.connect import SmartConnect, Disconnect
from openstack import connection
from pyVmomi import vim, vmodl
import argparse
import logging
import time
from helper.prometheus_exporter import *
from collections import namedtuple
from helper.prometheus_connect import *

# prometheus export functionality
from prometheus_client import start_http_server, Gauge

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')
vm_uuid_re = re.compile('^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)


def run_check(args, vcenter_data):
    while True:
        log.info("- INFO - starting new loop run with automation %s ", args.automated)
        status = vm_move_suggestions(args, vcenter_data)
        if status == "success":
            vcenter_data.sync_data()
        else:
            log.info("- INFO vrops connection issue\n")
        # wait the interval time
        log.info("- INFO - waiting %s minutes before starting the next loop run \n \n", str(args.interval))
        time.sleep(60 * int(args.interval))


def vm_move_suggestions(args, vcenter_data):

    big_vm_template = namedtuple("big_vm_details", ['host', 'big_vm', 'big_vm_size'])
    target_host_template = namedtuple("target_host_details", ['host', 'free_host_size'])
    source_host_template = namedtuple("source_host_details", ['host', 'consumed_host_size'])

    #  openstack info
    log.info("- INFO - Nanny Handle Big VM size between %s and %s", args.min_vm_size,args.max_vm_size)
    log.info("- INFO - connecting to openstack to region %s", args.region)
    openstack_obj = openstack.OpenstackHelper(args.region, args.user_domain_name,
                                              args.project_domain_name, args.project_name,args.username, args.password)

    prom_connect = PrometheusInfraConnect(region=args.region)

    # cleaning-up last nanny job orphaned cleanup
    nanny_metadata_handle = "nanny_big_vm_handle"
    shard_vcenter_all = openstack_obj.get_shard_vcenter_all(args.vc_host)
    log.info("- INFO - all building block number %s which is enabled/disable from openstack of region %s",shard_vcenter_all, args.region)
    avail_zone = args.region.lower() + re.split("-",args.vc_host)[1].lower()
    openstack_obj.delete_nanny_metadata(nanny_metadata_handle,avail_zone,shard_vcenter_all)

    log.info("- INFO - all denial_list_hosts %s ", args.denial_list)
    log.info("- INFO - all allowed_list_hosts %s ", args.allowed_list)
    if args.denial_list:
        denial_bb_name = [int(re.search(r"[0-9]+", i).group(0)) for i in args.denial_list]
    else:
        denial_bb_name = []

    if args.allowed_list:
        allowed_bb_name = [int(re.search(r"[0-9]+", i).group(0)) for i in args.allowed_list]
    else:
        allowed_bb_name = []

    percentage = args.percentage
    bb_consume  = {}
    bb_overall = {}
    bb_bigvm_consume = {}
    shard_vcenter = openstack_obj.get_shard_vcenter(args.vc_host)
    log.info("- INFO - getting building block info from openstack of region %s", args.region)
    bb_name = [int(re.search(r"[0-9]+", i).group(0)) for i in shard_vcenter]
    for bb in bb_name:
        bb_consume[bb] = 0
        bb_overall[bb] = 0
        bb_bigvm_consume[bb] = 0
    log.info("- INFO - all building block number %s which is enabled from openstack of region %s",shard_vcenter,args.region)

    # vcenter info
    log.info("- INFO - connecting to vcenter to host %s", args.vc_host)
    try:
        vc = VCenterHelper(args.vc_host,args.vc_user,args.vc_password)
    except Exception as err:
        log.info("- INFO - connecting to vcenter to host %s have an issue with %s", args.vc_host,err)
        return
    log.info("- INFO - getting cluster view info from vcenter host %s", args.vc_host)
    cluster_view = vc.find_all_of_type(vc.vim.ClusterComputeResource)
    big_vm_host = vc.get_big_vm_host(cluster_view)
    log.info("- INFO - all Big_VM_backup_hosts %s ",[ big_vm.name for big_vm in big_vm_host])
    fail_over_hosts = vc.get_failover_host(cluster_view,failover_host=0)
    fail_over_hosts.extend(vc.get_failover_host(cluster_view,failover_host=1))
    log.info("- INFO - all fail over hosts %s ", [ fail_over.name for fail_over in fail_over_hosts])
    cluster_view.Destroy()
    log.info("- INFO - getting hostview view info from vcenter host %s\n", args.vc_host)
    host_view = vc.find_all_of_type(vc.vim.HostSystem)
    hosts = vc.collect_properties(host_view, vim.HostSystem, ['name','config.host','hardware.memorySize',
                                                              'parent','runtime','vm'],include_mors=True)
    host_view.Destroy()
    big_vm_to_move_list = []
    target_host = []
    source_host = []

    # looping over each esxi node(which is random order)
    for host in hosts:
        if host['runtime'].inMaintenanceMode == True or host['runtime'].inQuarantineMode == True or \
                        host['runtime'].connectionState == "notResponding":
            continue
        if host['config.host'] in big_vm_host or host['config.host'] in fail_over_hosts:
            continue
        if int(re.findall(r"[0-9]+",host['name'])[1]) not in bb_name:
            continue
        if allowed_bb_name and int(re.findall(r"[0-9]+",host['name'])[1]) not in allowed_bb_name:
            continue
        if not host['parent'].name.startswith("production"):
            continue
        log.info("- INFO - node started here %s and its status %s",host['name'],host['runtime'].connectionState)
        host_contention = prom_connect.find_host_contention(args.vc_host,host['name'])
        if host_contention == "no_host_contention":
            log.info("- INFO - node started %s but its no_host_contention so will not consider as target/source host\n",
                     host['name'])
            continue
        elif host_contention == "host_contention":
            log.info("- INFO - node started %s but its host_contention so will be consider as target/source host",
                     host['name'])
        else:
            log.info("prom connection issue")
            return "no_success"
        host_size = host['hardware.memorySize']/1048576      # get host memory size in MB
        bb_overall[int(re.findall(r"[0-9]+",host['name'])[1])] = bb_overall[int(re.findall(r"[0-9]+",host['name'])[1])] + host_size
        log.info("- INFO - host name %s and size %.2f GB ",host['name'],host_size/1024)
        vcenter_data.set_data('vm_balance_nanny_host_size_bytes', int(host['hardware.memorySize']),[host['name']])
        host_consumed_size = 0
        big_vm_total_size = 0
        max_big_vm_size_handle = args.max_vm_size
        big_vm_to_move = ""
        for vm in host['vm']:
            try:
                if vm.config.hardware.memoryMB == 128 and vm.config.hardware.numCPU == 1 and \
                                vm.runtime.powerState == 'poweredOff':
                    continue
                if not vm.config.annotation:
                    continue
                host_consumed_size = host_consumed_size + vm.config.hardware.memoryMB
                if vm.config.hardware.memoryMB > args.min_vm_size:
                    big_vm_name_detail = str(vm.name)
                    try:
                        if vm_uuid_re.match(re.split("\(|\)", big_vm_name_detail)[-2]):
                            big_vm_uuid_detail = re.split("\(|\)", big_vm_name_detail)[-2]
                            vm_detail = openstack_obj.get_server_detail(big_vm_uuid_detail)
                            log.info("- INFO - vm name %s is big vm and size %.2f GB and created at: %s",vm.name, vm.config.hardware.memoryMB/1024,vm_detail.created_at)
                        else:
                            log.info("- INFO - vm name %s is big vm and size %.2f GB", vm.name,vm.config.hardware.memoryMB / 1024)
                    except IndexError :
                        log.info("- ERROR - vm name %s having some issue",vm.name)
                    big_vm_total_size = big_vm_total_size + vm.config.hardware.memoryMB
                    bb_bigvm_consume[int(re.findall(r"[0-9]+", host['name'])[1])] = bb_bigvm_consume[int(
                        re.findall(r"[0-9]+", host['name'])[1])] + vm.config.hardware.memoryMB
                    ##VM readiness
                    vm_readiness = prom_connect.find_vm_readiness(args.vc_host,vm.name)
                    if vm_readiness == "no_vm_readiness":
                        log.info(
                            "- INFO - vm started %s but its no_vm_readiness so will not consider vm for vmotion",vm.name)
                        continue
                    elif vm_readiness == "vm_readiness":
                        log.info("- INFO - vm started %s but its vm_readiness so will consider as vm for vmotion if its large vm",vm.name)
                    else:
                        log.info("prom connection issue")
                        return "no_success"
                    if vm.config.hardware.memoryMB < max_big_vm_size_handle:
                        big_vm_to_move = str(vm.name)
                        max_big_vm_size_handle = vm.config.hardware.memoryMB
            except vmodl.fault.ManagedObjectNotFound:  # VM went away, nothing we can really do about that.
                pass
            except AttributeError as error:
                log.info("- INFO - No attribute is defined with error %s", error)
        log.info("- INFO - host name %s and consumed size %.2f GB ",host['name'],host_consumed_size/1024)
        log.info("- INFO - host name %s and BIG VM consumed size %.2f GB ",host['name'], big_vm_total_size/1024)
        bb_consume[int(re.findall(r"[0-9]+", host['name'])[1])] = bb_consume[int(
            re.findall(r"[0-9]+", host['name'])[1])] + host_consumed_size
        vcenter_data.set_data('vm_balance_nanny_host_size_consume_all_vm_bytes', int(host_consumed_size*1024*1024), [host['name']])
        vcenter_data.set_data('vm_balance_nanny_host_size_consume_big_vm_bytes', int(big_vm_total_size*1024*1024), [host['name']])

        if host_consumed_size >= host_size:
            log.info("- INFO - host name {} over utilised ".format(host['name']))
        """
        else:
            if (host_size - big_vm_total_size) > args.min_vm_size :
                #big_vm_template = namedtuple("big_vm_details", ['host', 'big_vm', 'big_vm_size'])
                #target_host_template = namedtuple("host_details", ['host', 'free_host_size'])
                target_host_details = target_host_template(host=host['name'],free_host_size=(host_size - big_vm_total_size))
                target_host.append(target_host_details)
        """
        if big_vm_total_size >= host_size*(1+percentage/100):
            if max_big_vm_size_handle != 1050000:
                log.info("- INFO - Alert host name {} over utilised with BIG_VM Alert".format(host['name']))
                #big_vm_template = namedtuple("big_vm_details", ['host', 'big_vm', 'big_vm_size'])
                #target_host_template = namedtuple("target_host_details", ['host', 'free_host_size'])
                big_vm_details = big_vm_template(host=host['name'],big_vm=big_vm_to_move,big_vm_size=max_big_vm_size_handle)
                big_vm_to_move_list.append(big_vm_details)
            else:
                log.info("- INFO - Alert host name {} over utilised with BIG_VM Alert (No suitable_vm for move)".format(host['name']))
                #source_host_template = namedtuple("source_host_details", ['host', 'consumed_host_size'])
                source_host_details = source_host_template(host=host['name'],consumed_host_size=big_vm_total_size)
                source_host.append(source_host_details)
        else:
            if (host_size - big_vm_total_size) > args.min_vm_size :
                #big_vm_template = namedtuple("big_vm_details", ['host', 'big_vm', 'big_vm_size'])
                #target_host_template = namedtuple("host_details", ['host', 'free_host_size'])
                target_host_details = target_host_template(host=host['name'],free_host_size=(host_size - big_vm_total_size))
                target_host.append(target_host_details)
        log.info("- INFO - node end here %s \n ",host['name'])

    for bb in bb_name:
        vcenter_data.set_data('vm_balance_building_block_consume_all_vm_bytes', int(bb_consume[bb]*1024*1024), [str(bb)])
        vcenter_data.set_data('vm_balance_building_block_consume_big_vm_bytes', int(bb_bigvm_consume[bb]*1024*1024), [str(bb)])
        vcenter_data.set_data('vm_balance_building_block_total_size_bytes', int(bb_overall[bb]*1024*1024), [str(bb)])

    if len(big_vm_to_move_list) > 0:
        target_host = sorted(target_host, key=lambda x: x[1], reverse=True)
        big_vm_to_move_list = sorted(big_vm_to_move_list, key=lambda x: x[2], reverse=True)
        log.info("- INFO - target host here %s ", target_host)
        log.info("- INFO - big_vm list here %s ", big_vm_to_move_list)
        log.info("- Alert - found here %s",len(big_vm_to_move_list))
        log.info("- Printing - suggestion for vmotion below\n")
        big_vm_movement_suggestion(args,vc,openstack_obj,big_vm_to_move_list,target_host,vcenter_data,nanny_metadata_handle,denial_bb_name)

    for source_h in source_host:
        log.info("- Alert - found alert with no suitable for move after checking vm_readiness")
        log.info("- INFO - overbooked host %s and overbooked by %s", source_h.host,source_h.consumed_host_size)
        vcenter_data.set_data('vm_balance_no_suitable_vm', int(source_h.consumed_host_size * 1024),[source_h.host])

    return "success"

def big_vm_movement_suggestion(args,vc,openstack_obj,big_vm_to_move_list,target_host,vcenter_data,nanny_metadata_handle,denial_bb_name):
    vcenter_error_count = 0
    vmotion_count = 0
    # big_vm_template = namedtuple("big_vm_details", ['host', 'big_vm', 'big_vm_size'])
    # target_host_template = namedtuple("target_host_details", ['host', 'free_host_size'])
    target_host_template = namedtuple("target_host_details", ['host', 'free_host_size'])

    for big_vm in big_vm_to_move_list:
        # to handle certain bb like bb56
        node = re.findall(r"[0-9]+", big_vm.host)[1]
        for target_h in target_host[:]:
            if node == re.findall(r"[0-9]+", target_h.host)[1]:
                if target_h.free_host_size - big_vm.big_vm_size > 0:
                    log.info(f"- INFO - big Vm {big_vm.big_vm} of size {round(big_vm.big_vm_size,2)} is move from source {big_vm.host} to target {target_h.host}  having {round(target_h.free_host_size,2)} memory and left with memory {round((target_h.free_host_size - big_vm.big_vm_size),2)} after vmotion")
                    if int(re.findall(r"[0-9]+", target_h.host)[1]) in denial_bb_name:
                        vcenter_data.set_data('vm_balance_nanny_manual_suggestion_bytes', int(big_vm.big_vm_size * 1024),[big_vm.host, target_h.host, big_vm.big_vm])
                    if vm_uuid_re.match(re.split("\(|\)", big_vm.big_vm)[-2]):
                        big_vm_uuid = re.split("\(|\)", big_vm.big_vm)[-2]
                    else:
                        log.info("- INFO - VM UUID cant grab VM detail %s",big_vm.big_vm)
                        break
                    try:
                        openstack_obj.api.compute.get_server(big_vm_uuid)
                    except openstack.exceptions.ResourceNotFound:
                        log.info("- INFO - BIG_VM %s Not exist anymore", big_vm_uuid)
                        break
                    # automation script for vmotion called here
                    if args.automated:
                        if int(re.findall(r"[0-9]+", target_h.host)[1]) in denial_bb_name:
                            log.info(f"- INFO - Manual movement needed for big Vm  {big_vm.big_vm} of size {round(big_vm.big_vm_size,2)} is move from source {big_vm.host} to target {target_h.host} as BB in denial_list")
                        else:
                            status = vc.vmotion_inside_bb(openstack_obj, big_vm_uuid, target_h.host, nanny_metadata_handle)
                            log.info(f"- INFO - big Vm {big_vm.big_vm} of size {round(big_vm.big_vm_size,2)} is move from source {big_vm.host} to target {target_h.host} with {status}")
                            if status != "success":
                                log.info(
                                    f"- ERROR - Failed Vmotion of big Vm {big_vm.big_vm} of size {round(big_vm.big_vm_size, 2)} is not move from source {big_vm.host} to target {target_h.host} with {status}")
                                vcenter_error_count += 1
                            if status == "success":
                                log.info(
                                    f"- INFO - big Vm {big_vm.big_vm} of size {round(big_vm.big_vm_size, 2)} is move from source {big_vm.host} to target {target_h.host} with {status}")
                                vcenter_data.set_data('vm_balance_vmotion_status',
                                                      int(big_vm.big_vm_size * 1024),
                                                      [big_vm.host, target_h.host, big_vm.big_vm])
                                vmotion_count += 1

                    else:
                        vcenter_data.set_data('vm_balance_nanny_suggestion_bytes', int(big_vm.big_vm_size * 1024),[big_vm.host, target_h.host, big_vm.big_vm])
                    if (target_h.free_host_size - big_vm.big_vm_size) >= args.min_vm_size:
                        target_host_details = target_host_template(host=target_h.host,
                                                                   free_host_size=(target_h.free_host_size - big_vm.big_vm_size))
                        target_host.append(target_host_details)
                        target_host.remove(target_h)
                        target_host = sorted(target_host, key=lambda x: x[1], reverse=True)
                    else:
                        target_host.remove(target_h)
                    break
        else:
            log.info("- INFO - big Vm %s is move from source %s to big_vm_node as no node left ", big_vm.big_vm, big_vm.host)
            vcenter_data.set_data('vm_balance_too_full_building_block', int(big_vm.big_vm_size * 1024), [str(big_vm.host)])
            #consolidation needed for building block

    vcenter_data.set_data('vm_balance_error_count', vcenter_error_count,["vmotion_error"])
    vcenter_data.set_data('vm_balance_vmotion_count', vmotion_count, ["vmotion_success"])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--region",required=True)
    parser.add_argument("--username",required=True)
    parser.add_argument("--password",required=True)
    parser.add_argument("--user_domain_name",required=True)
    parser.add_argument("--project_domain_name",required=True)
    parser.add_argument("--project_name",required=True)
    parser.add_argument("--vc_user",required=True)
    parser.add_argument("--vc_password",required=True)
    parser.add_argument("--vc_host",required=True, help="vcenter hostname")
    parser.add_argument("--interval",type=int, default=360,help="Interval in minutes between check runs")
    parser.add_argument("--prometheus-port", type=int, default=9456,help="Port to run the prometheus exporter for metrics on")
    parser.add_argument("--min_vm_size", type=int, default=231056, help="Min Big_Vm size to handle 500000 ")
    parser.add_argument("--max_vm_size", type=int, default=550000, help="Max Big_Vm size to handle")
    parser.add_argument("--percentage", type=int, default=3, help="percentage of overbooked")
    parser.add_argument("--automated",action="store_true", help='false as automation of big_vm not doing vmotion only suggestion')
    parser.add_argument("--denial_list",nargs='*',required=False,default=None,help='all building block ignored by nanny')
    parser.add_argument("--allowed_list", nargs='*', required=False, default=None,help='only building block allowed by nanny')
    vcenter_data = PromDataClass()
    mymetrics = PromMetricsClass()
    mymetrics.set_metrics('vm_balance_nanny_host_size_bytes', 'des:vm_balance_nanny_host_size_bytes', ['nodename'])
    mymetrics.set_metrics('vm_balance_nanny_host_size_consume_all_vm_bytes',
                          'des:vm_balance_nanny_host_size_consume_all_vm_bytes', ['nodename'])
    mymetrics.set_metrics('vm_balance_nanny_host_size_consume_big_vm_bytes',
                          'des:vm_balance_nanny_host_size_consume_big_vm_bytes', ['nodename'])
    mymetrics.set_metrics('vm_balance_nanny_suggestion_bytes', 'des:vm_balance_nanny_suggestion_bytes',
                          ['source_node', 'target_node', 'big_vm_name', 'big_vm_size'])
    mymetrics.set_metrics('vm_balance_nanny_manual_suggestion_bytes', 'des:vm_balance_nanny_manual_suggestion_bytes',
                          ['source_node', 'target_node', 'big_vm_name', 'big_vm_size'])
    mymetrics.set_metrics('vm_balance_building_block_consume_all_vm_bytes',
                          'des:vm_balance_building_block_consume_all_vm_bytes',
                          ['Building_block'])
    mymetrics.set_metrics('vm_balance_building_block_consume_big_vm_bytes',
                          'des:vm_balance_building_block_consume_big_vm_bytes',
                          ['Building_block'])
    mymetrics.set_metrics('vm_balance_building_block_total_size_bytes',
                          'des:vm_balance_building_block_total_siz_bytes',
                          ['Building_block'])
    mymetrics.set_metrics('vm_balance_error_count','des:vm_balance_error_count',['error_type'])
    mymetrics.set_metrics('vm_balance_too_full_building_block','des:vm_balance_too_full_building_block', ['consolidated_needed'])
    mymetrics.set_metrics('vm_balance_no_suitable_vm', 'des:vm_balance_no_suitable_vm',
                          ['source_node','node_overbooked_by'])
    mymetrics.set_metrics('vm_balance_vmotion_status', 'des:vm_balance_vmotion_status',
                          ['source_node', 'target_node', 'big_vm_name', 'big_vm_size'])
    mymetrics.set_metrics('vm_balance_vmotion_count', 'des:vm_balance_vmotion_count', ['success'])
    args = parser.parse_args()
    REGISTRY.register(CustomCollector(mymetrics, vcenter_data))
    prometheus_http_start(int(args.prometheus_port))

    run_check(args, vcenter_data)

if __name__ == "__main__":
    main()
