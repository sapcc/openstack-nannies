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

#import atexit
import time
import argparse
import os
import logging

from helper.vcenter import *
from helper.openstack import OpenstackHelper

from pyVmomi import vim
from pyVim.task import WaitForTask

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')

def main():

    # connection for vm
    openstack_obj = OpenstackHelper(os.getenv('REGION'), os.getenv('OS_USER_DOMAIN_NAME'),
                                    os.getenv('OS_PROJECT_DOMAIN_NAME'),os.getenv('OS_PROJECT_NAME'),
                                    os.getenv('OS_USERNAME'), os.getenv('OS_PASSWORD'))

    vc = VCenterHelper(host=os.getenv('VM_BALANCE_VCHOST'),user=os.getenv('VM_BALANCE_VCUSER'),password=os.getenv('VM_BALANCE_VCPASSWORD'))

    big_vm_name_uuid = input("Enter BIG VM Instance UUID: ")

    """
    servers = openstack_obj.api.compute.servers(details=True, all_projects=True)

    for i in servers:
        print(i)
        Enter BIG VM Instance UUID: ...
        Enter Free Node name: ...
    """

    # details about vm and  free node
    vm = vc.find_server(big_vm_name_uuid)
    free_node_name = input("Enter Free Node name like(node*-bb*.cc.*-*-*.cloud.sap):")
    vhost = vc.get_object_by_name(vim.HostSystem,free_node_name)
    log.info("INFO:  vmotion of instance uuid %s started to target node %s",big_vm_name_uuid,free_node_name)

    # capture the status of server
    # check metadata and lock if exist
    loc_check = openstack_obj.api.compute.get_server(big_vm_name_uuid)
    log.info("INFO: instance uuid %s lock status %s", big_vm_name_uuid, loc_check['is_locked'])

    # setting metadata and lock for nanny
    metadata_check = openstack_obj.api.compute.set_server_metadata(big_vm_name_uuid, metadata="nanny_big_vm_handle")
    loc = openstack_obj.api.compute.lock_server(big_vm_name_uuid)
    loc_check = openstack_obj.api.compute.get_server(big_vm_name_uuid)
    log.info("INFO: instance uuid %s lock status set by nanny %s", big_vm_name_uuid, loc_check['is_locked'])

    # actual vmotion step
    spec = vim.VirtualMachineRelocateSpec()
    spec.host = vhost
    task = vm.RelocateVM_Task(spec)
    try:
        state = WaitForTask(task,si=vc.api)
    except Exception as e:
        logging.error("ERROR: failed to relocate big vm %s to target node %s with error message =>%s",
                      str(big_vm_name_uuid),str(free_node_name),str(e.msg))
    else:
        log.info("INFO: vmotion done big vm %s to target node %s and state %s",str(big_vm_name_uuid),str(free_node_name),str(state))

    # if result failed through alert
    # unlock the server and unset nanny metadata
    unloc = openstack_obj.api.compute.unlock_server(big_vm_name_uuid)
    metadata_check = openstack_obj.api.compute.delete_server_metadata(big_vm_name_uuid,['metadata'])

    # check unlock succesfully done
    unloc_check = openstack_obj.api.compute.get_server(big_vm_name_uuid)
    log.info("INFO: instance uuid %s unlock status %s done", big_vm_name_uuid, unloc_check['is_locked'])

    # check status of server

if __name__ == '__main__':
    main()
