
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

# prometheus export functionality
from prometheus_client import start_http_server, Gauge

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')


def run_check(args, vcenter_data):
    while True:
        log.info("- INFO - starting new loop run with automation %s ", args.automated)
        # convert iterations from string to integer and avoid off by one error
        # below  object creation for prometheus collector metric
        vm_move_suggestions(args,vcenter_data)
        # wait the interval time
        log.info("- INFO - waiting %s minutes before starting the next loop run \n \n", str(args.interval))
        vcenter_data.sync_data()
        time.sleep(60 * int(args.interval))


def vm_move_suggestions(args, vcenter_data):

    #  openstack info
    log.info("- INFO - Nanny Handle Big VM size between %s and %s", args.min_vm_size,args.max_vm_size)
    log.info("- INFO - connecting to openstack to region %s", args.region)
    openstack_obj = openstack.OpenstackHelper(args.region, args.user_domain_name,
                                              args.project_domain_name,args.project_name,args.username, args.password)

    # cleaning-up last nanny job orphne cleanup
    nanny_metadata_handle = "nanny_big_vm_handle"
    avail_zone = args.region.lower() + re.split("-",args.vc_host)[1].lower()
    openstack_obj.delete_nanny_metadata(nanny_metadata_handle,avail_zone)

    percentage = args.percentage
    bb_consume  = {}
    bb_overall = {}
    bb_bigvm_consume = {}
    shard_vcenter = openstack_obj.get_shard_vcenter(args.vc_host)
    #openstack_re = re.compile("^name")
    log.info("- INFO - getting building block info from openstack of region %s", args.region)
    bb_name = [int(re.search(r"[0-9]+", i).group(0)) for i in shard_vcenter]
    for bb in bb_name:
        bb_consume[bb] = 0
        bb_overall[bb] = 0
        bb_bigvm_consume[bb] = 0
    log.info("- INFO - all building block number %s which is enabled from openstack of region %s",bb_name,args.region)

    # vcenter info
    log.info("- INFO - connecting to vcenter to host %s", args.vc_host)
    vc = VCenterHelper(args.vc_host,args.vc_user,args.vc_password)
    log.info("- INFO - getting cluster view info from vcenter host %s", args.vc_host)
    cluster_view = vc.find_all_of_type(vc.vim.ClusterComputeResource)
    big_vm_host = vc.get_big_vm_host(cluster_view)
    log.info("- INFO - all Big_VM_backup_hosts %s ",[ big_vm.name for big_vm in big_vm_host])
    fail_over_hosts = vc.get_failover_host(cluster_view)
    log.info("- INFO - all fail over hosts %s ", [ fail_over.name for fail_over in fail_over_hosts])
    cluster_view.Destroy()
    log.info("- INFO - getting hostview view info from vcenter host %s", args.vc_host)
    host_view = vc.find_all_of_type(vc.vim.HostSystem)
    hosts = vc.collect_properties(host_view, vim.HostSystem, ['name','config.host','hardware.memorySize',
                                                              'parent','runtime','vm'],include_mors=True)
    host_view.Destroy()
    big_vm_to_move_list = []
    target_host = []

    # added runtime details please
    for host in hosts:
        if host['runtime'].inMaintenanceMode == True or host['runtime'].inQuarantineMode == True or \
                        host['runtime'].connectionState == "notResponding":
            continue
        if host['config.host'] in big_vm_host or host['config.host'] in fail_over_hosts:
            continue
        if int(re.findall(r"[0-9]+",host['name'])[1]) not in bb_name:
            continue
        if not host['parent'].name.startswith("production"):
            continue
        log.info("- INFO - node started here %s and its status %s",host['name'],host['runtime'].connectionState)
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
                #if not openstack_re.match(vm.config.annotation):
                #    continue
                #log.info("- INFO - vm name {} and size {}".format(vm.name,vm.config.hardware.memoryMB))
                host_consumed_size = host_consumed_size + vm.config.hardware.memoryMB
                if vm.config.hardware.memoryMB > args.min_vm_size:
                    log.info("- INFO - vm name %s is big vm and size %.2f GB",vm.name, vm.config.hardware.memoryMB/1024)
                    big_vm_total_size = big_vm_total_size + vm.config.hardware.memoryMB
                    bb_bigvm_consume[int(re.findall(r"[0-9]+", host['name'])[1])] = bb_bigvm_consume[int(
                        re.findall(r"[0-9]+", host['name'])[1])] + vm.config.hardware.memoryMB
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
        else:
            if (host_size - host_consumed_size) > args.min_vm_size :
                target_host.append((host['name'],host_size - host_consumed_size))

        if big_vm_total_size >= host_size*(1+percentage/100):
            log.info("- INFO - Alert Alert host name {} over utilised with BIG_VM Alert Alert".format(host['name']))
            big_vm_to_move_list.append((host['name'],big_vm_to_move, max_big_vm_size_handle))
        log.info("- INFO - node end here %s ",host['name'])

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
        log.info("- Printing - suggestion for vmotion below")
        big_vm_movement_suggestion(args,vc,openstack_obj,big_vm_to_move_list,target_host,vcenter_data,nanny_metadata_handle)

def big_vm_movement_suggestion(args,vc,openstack_obj,big_vm_to_move_list,target_host,vcenter_data,nanny_metadata_handle):
    vcenter_error_count = 0
    vm_uuid_re = re.compile('^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
    for big_vm in big_vm_to_move_list:
        node = re.findall(r"[0-9]+", big_vm[0])[1]
        for target_h in target_host[:]:
            if node == re.findall(r"[0-9]+", target_h[0])[1]:
                if target_h[1] - big_vm[2] > 0:
                    log.info(f"- INFO - big Vm {big_vm[1]} of size {round(big_vm[2],2)} is move from source {big_vm[0]} to target {target_h[0]}  having {round(target_h[1],2)} memory and left with memory {round((target_h[1] - big_vm[2]),2)} after vmotion")
                    vcenter_data.set_data('vm_balance_nanny_suggestion_bytes', int(big_vm[2] * 1024),[big_vm[0], target_h[0], big_vm[1]])
                    # automation script for vmotion called here
                    if vm_uuid_re.match(re.split("\(|\)", big_vm[1])[-2]):
                        big_vm_uuid = re.split("\(|\)", big_vm[1])[-2]
                    else:
                        log.info("- INFO - VM UUID cant grab VM detail %s",big_vm[1])
                        break
                    if args.automated:
                        status = vc.vmotion_inside_bb(openstack_obj, big_vm_uuid, target_h[0], nanny_metadata_handle)
                        log.info(f"- INFO - big Vm {big_vm[1]} of size {round(big_vm[2],2)} is move from source {big_vm[0]} to target {target_h[0]} with {status}")
                        if status != "success":
                            vcenter_error_count += 1
                    if (target_h[1] - big_vm[2]) >= args.min_vm_size:
                        target_host.append((target_h[0], target_h[1] - big_vm[2]))
                        target_host.remove(target_h)
                        target_host = sorted(target_host, key=lambda x: x[1], reverse=True)
                    else:
                        target_host.remove(target_h)
                    break
        else:
            log.info("- INFO - big Vm %s is move from source %s to big_vm_node as no node left ", big_vm[1], big_vm[0])

    vcenter_data.set_data('vm_balance_error_count', vcenter_error_count,["vmotion_error"])

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
    parser.add_argument("--max_vm_size", type=int, default=1050000, help="Max Big_Vm size to handle")
    parser.add_argument("--percentage", type=int, default=3, help="percentage of overbooked")
    parser.add_argument("--automated",action="store_true", help='false as automation of big_vm not doing vmotion only suggestion')
    vcenter_data = PromDataClass()
    mymetrics = PromMetricsClass()
    mymetrics.set_metrics('vm_balance_nanny_host_size_bytes', 'des:vm_balance_nanny_host_size_bytes', ['nodename'])
    mymetrics.set_metrics('vm_balance_nanny_host_size_consume_all_vm_bytes',
                          'des:vm_balance_nanny_host_size_consume_all_vm_bytes', ['nodename'])
    mymetrics.set_metrics('vm_balance_nanny_host_size_consume_big_vm_bytes',
                          'des:vm_balance_nanny_host_size_consume_big_vm_bytes', ['nodename'])
    mymetrics.set_metrics('vm_balance_nanny_suggestion_bytes', 'des:vm_balance_nanny_suggestion_bytes',
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
    args = parser.parse_args()
    REGISTRY.register(CustomCollector(mymetrics, vcenter_data))
    prometheus_http_start(int(args.prometheus_port))

    run_check(args, vcenter_data)

if __name__ == "__main__":
    main()
