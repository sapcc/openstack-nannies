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
import os

from helper.netapp import NetAppHelper
from helper.vcenter import *
from helper.vmfs_balance_helper import *
from helper.openstack import OpenstackHelper

log = logging.getLogger(__name__)


def parse_commandline():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="dry-run mode - do nothing critical")
    parser.add_argument("--debug", action="store_true",
                        help="add additional debug output")
    parser.add_argument("--vcenter-host", required=True,
                        help="vcenter hostname")
    parser.add_argument("--vcenter-user", required=True,
                        help="vcenter username")
    parser.add_argument("--vcenter-password", required=True,
                        help="vcenter user password")
    parser.add_argument("--volume-uuid", required=True,
                        help="openstack uuid of the volume to move")
    parser.add_argument("--instance-uuid", required=True,
                        help="openstack uuid of the instance the volume is attached to")
    parser.add_argument("--target-ds", required=True,
                        help="target ds to move the volume to")
    args = parser.parse_args()
    return args


# we move the lun/volume between the netapps by telling the vcenter to move the attached
# storage of the corresponding shadow vm to another datastore (i.e. anorther netapp)
def move_detached_volume_shadow_vm(args, vc, vm_info, ds_info, volume_uuid, target_ds):
    vm = vm_info.get_by_instanceuuid(volume_uuid)
    targetds = ds_info.get_by_name(target_ds)
    if not args.dry_run:
        spec = vim.VirtualMachineRelocateSpec()
        spec.datastore = targetds.handle
        task = vm.handle.RelocateVM_Task(spec)
        try:
            status = WaitForTask(task,si=vc.api)
        except Exception as e:
            logging.error("- ERROR - failed to move detached volume {} to data store {} with error message: {}".format(volume_uuid, target_ds, str(e))) 
            return False
        else:
            log.info("- INFO -    move of detached volume {} to data store {} successful with status {}".format(volume_uuid, target_ds, str(status))) 
    else:
        log.info("- INFO -    dry-run: simulating move of detached volume {} to ds {}".format(volume_uuid, target_ds))
        time.sleep(5)
    return True


# if the volume is attached to an instance more extra steps are required to make
# sure the file naming on the datastore stays consistent (i.e. volume uuid as
# file name)
def move_attached_volume_shadow_vm(args, vc, vm_info, ds_info, volume_uuid, instance_uuid, target_ds):
    shadowvm = vm_info.get_by_name(volume_uuid)
    for device in shadowvm.hardware.device:
        if isinstance(device, vim.vm.device.VirtualDisk):
            vmdk_origin_path = device.backing.fileName
            vmdk_vmware_uuid = device.backing.uuid
    log.info(vmdk_origin_path)
    
    instance = vm_info.get_by_instanceuuid(instance_uuid)
    instance_vm_name = instance.name
    targetds = ds_info.get_by_name(target_ds)

    thinProvisioned = False
    eagerlyScrub = False

    if not args.dry_run:

        vm_relocate_spec = vim.vm.RelocateSpec()
        list_locators = []
        for device in instance.hardware.device:
            if isinstance(device, vim.vm.device.VirtualDisk):
                locator = vim.vm.RelocateSpec.DiskLocator()
                if targetds.handle.summary.type == "VMFS" and device.backing.uuid == vmdk_vmware_uuid:
                    locator.diskBackingInfo = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
                    locator.diskBackingInfo.eagerlyScrub = eagerlyScrub
                    locator.diskBackingInfo.thinProvisioned = thinProvisioned
                else:
                    if device.backing.uuid == vmdk_vmware_uuid:
                        locator.diskBackingInfo = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
                        locator.diskBackingInfo.eagerlyScrub = False
                        locator.diskBackingInfo.thinProvisioned = True
                locator.diskId = device.key
                if device.backing.uuid == vmdk_vmware_uuid:
                    locator.datastore = targetds.handle
                    locator.diskBackingInfo.fileName = "[%s]/%s/%s" % (targetds.name, volume_uuid, volume_uuid) 
                else:
                    locator.datastore = device.backing.datastore
                list_locators.append(locator)

        vm_relocate_spec.disk = list_locators
        instance.handle.Rename(volume_uuid)
        my_task = instance.handle.Relocate(vm_relocate_spec)
        log.info("Moving ....")
        state = None
        renamed = False
        while state not in (vim.TaskInfo.State.success, vim.TaskInfo.State.error):
            try:
                task_info = my_task.info
                if task_info.progress is not None:
                    log.info("progress: {}".format(task_info.progress))
                state = task_info.state
                if state == 'running' and not renamed and task_info.progress is not None and task_info.progress > 30:
                    renamed = True
                    instance.handle.Rename(instance_vm_name)
            except vmodl.fault.ManagedObjectNotFound as e:
                log.info("Task object has been deleted: {}".format(e.obj))
                break
        if not renamed:
                instance.handle.Rename(instance_vm_name)
        # we have to re-read the device info here from the vcenter directly and update
        # the corresponding cached info in our instance class as some things changed
        instance.hardware.device = instance.handle.config.hardware.device
        for device in instance.hardware.device:
            if isinstance(device, vim.vm.device.VirtualDisk) and device.backing.uuid == vmdk_vmware_uuid:
                new_vmdk_path = device.backing.fileName
                log.info(new_vmdk_path)
        
        disk_spec = vim.vm.device.VirtualDeviceSpec()
        disk_spec.operation =  vim.vm.device.VirtualDeviceSpec.Operation.edit
        for device in shadowvm.hardware.device:
            if isinstance(device, vim.vm.device.VirtualDisk) and device.backing.uuid == vmdk_vmware_uuid:
                disk_spec.device = device
        disk_spec.device.backing.fileName = new_vmdk_path
        disk_spec.device.backing.uuid = vmdk_vmware_uuid

        reconfig_spec = vim.vm.ConfigSpec()
        reconfig_spec.deviceChange = [disk_spec]
        my_task = shadowvm.handle.ReconfigVM_Task(spec=reconfig_spec)
        WaitForTask(my_task)

        shadowvm_relocate_spec = vim.vm.RelocateSpec()
        shadowvm_relocate_spec.datastore = targetds.handle
        shadowvm.handle.Relocate(shadowvm_relocate_spec)

        # we have to re-read the device info here from the vcenter directly and update
        # the corresponding cached info in our instance class as some things changed
        shadowvm.hardware.device = shadowvm.handle.config.hardware.device

    else:
        log.info("- INFO -    dry-run: simulating move of attached volume {} to ds {}".format(volume_uuid, target_ds))
        time.sleep(5)
    return True


def detached_volume_move_test(args, vc, vm_info, ds_info, oh):
    # TODO: error handling
    log.info("- INFO - detached volume move testing")
    log.info("- INFO -  locking volume {} in openstack".format(args.volume_uuid))
    oh.lock_volume_vc(args.volume_uuid, args.vcenter_host)
    log.info("- INFO -  marking volume {} in vc".format(args.volume_uuid))
    vc_mark_instance(vc, vm_info, args.volume_uuid, None)
    log.info("- INFO -   move of detached volume {} to data store {}".format(args.volume_uuid, args.target_ds)) 
    move_detached_volume_shadow_vm(args, vc, vm_info, ds_info, args.volume_uuid, args.target_ds)
    log.info("- INFO -  unmarking volume {} in vc".format(args.volume_uuid))
    vc_unmark_instance(vc, vm_info, args.volume_uuid)
    log.info("- INFO -  unlocking volume {} in openstack".format(args.volume_uuid))
    oh.unlock_volume_vc(args.volume_uuid, args.vcenter_host)


def attached_volume_move_test(args, vc, vm_info, ds_info, oh):
    # TODO: error handling
    log.info("- INFO - attached volume move testing")
    log.info("- INFO -  locking volume {} in openstack".format(args.volume_uuid))
    oh.lock_volume_vc(args.volume_uuid, args.vcenter_host)
    log.info("- INFO -  marking instance {} and volume {} in vc".format(args.instance_uuid, args.volume_uuid))
    vc_mark_instance(vc, vm_info, args.volume_uuid, args.instance_uuid)
    log.info("- INFO -   move of attached volume {} to data store {}".format(args.volume_uuid, args.target_ds)) 
    move_attached_volume_shadow_vm(args, vc, vm_info, ds_info, args.volume_uuid, args.instance_uuid, args.target_ds)
    log.info("- INFO -  unmarking instance {} in vc".format(args.instance_uuid))
    vc_unmark_instance(vc, vm_info, args.instance_uuid)
    log.info("- INFO -  unmarking volume {} in vc".format(args.volume_uuid))
    vc_unmark_instance(vc, vm_info, args.volume_uuid)
    log.info("- INFO -  unlocking volume {} in openstack".format(args.volume_uuid))
    oh.unlock_volume_vc(args.volume_uuid, args.vcenter_host)


def os_check_for_leftovers(args, oh):
    log.info("- INFO -  checking volumes for leftover storage_balancing metadata in openstack")
    result = True
    temporary_volume_list = list(oh.api.block_store.volumes(details=False, all_projects=1))
    if temporary_volume_list:
        for volume in temporary_volume_list:
            log.debug("- INFO -   volume uuid: {} - name: {}".format(volume.id, volume.name))
            # check if volumes have the storage_balancing property set for the current vc
            # if oh.check_volume_metadata(volume.id, 'storage_balancing', args.vcenter_host):
            #     log.info("- INFO -  volume {} has the storage_balancing property set for this vc {}".format(volume.id, args.vcenter_host))
            volume_sb_key = None
            if volume.metadata:
                volume_sb_key = volume.metadata.get('storage_balancing', None)
            if volume_sb_key:
                log.info("- INFO -   volume {} has storage_balancing metadata key set to {}".format(volume.id, volume_sb_key))
                result = False

    return result

def vc_check_for_leftovers(args, vm_info):
    log.info("- INFO -  checking volumes for leftover storage_balancing metadata in vc")
    result = True
    # this functionsis run at the beginning so we can trust the fresh cashed values in vm_info
    for vm in vm_info.elements:
        if not vm.annotation:
            continue
        annotation_list = vm.annotation.splitlines()
        for line in annotation_list:
            if line.startswith('storage_balancing-'):
                log.info("- INFO -   instance {} has storage_balancing annotation set: {}".format(vm.instanceuuid, line))
                result = False

    return result

def vc_check_ds_connected_hosts(vc, ds_info, target_ds):
    targetds = ds_info.get_by_name(target_ds)
    targetds_mounted_hosts = targetds.handle.host
    connected_hosts = []
    for host in targetds_mounted_hosts:
        if host.mountInfo.accessible and host.mountInfo.mounted:
            connected_hosts.append(host.key)

    return connected_hosts


def os_check_attach_status(args, oh, volume_uuid):
    log.info("- INFO -  checking the attach status for volume {} in openstack".format(volume_uuid))
    log.info("- INFO -   checking in cinder")
    myvol = oh.api.block_storage.get_volume(volume_uuid).to_dict()
    status = myvol['status']
    attachments = myvol['attachments']
    if status == "available":
        instance_uuid = None
        log.info("- INFO -    cinder: atatus = ailable - the volume does not seem to be attached to an instance")
    elif status == 'in-use':
        log.info("- INFO -    cinder: volume status = in-use - the volume seems to be attached to an instance")
        if len(attachments) != 1:
            instance_uuid = None
            log.info("- WARN - more than one attachment entry in cinder for this volume")
        else:
            cinder_instance_uuid = attachments[0]['server_id']
            log.info("- INFO -    cinder: the volume seems to be attached to instance {}".format(cinder_instance_uuid))
            log.info("- INFO -   checking in nova")
            nova_instance_attachments = oh.api.compute.volume_attachments(cinder_instance_uuid)
            for attachment in nova_instance_attachments:
                if attachment.volume_id == volume_uuid:
                    nova_instance_uuid = cinder_instance_uuid
                    log.info("- INFO -    nova: the volume seems to be attached to instance {}".format(nova_instance_uuid))
            if not nova_instance_uuid:
                    log.info("- WARN - the volume does not seem to be attached in nova at all")
            if nova_instance_uuid == cinder_instance_uuid:
                instance_uuid = cinder_instance_uuid
            else:
                log.info("- WARN - nova: the volume does not seem to be attached to instance {} (to which it is attached in cinder)".format(cinder_instance_uuid))
                log.info("- WARN - nova: instead it seems to be attached to instance {} in nova".format(nova_instance_uuid))
                instance_uuid = None
    else:
        instance_uuid = None
        log.warning("- WARN - volume {} has a strange state in openstack: {}".format(volume_uuid, status))

    return instance_uuid


def vc_check_attach_status(args, vc, vm_info, volume_uuid, instance_uuid):
    log.info("- INFO -  checking the attach status for volume {} in the vcenter".format(volume_uuid))
    result = True
    instance = vm_info.get_by_instanceuuid(instance_uuid)
    shadowvm = vm_info.get_by_instanceuuid(volume_uuid)
    # get this information fresh from the vcenter and update the class cached values too
    shadowvm.hardware.device = shadowvm.handle.config.hardware.device
    for device in shadowvm.hardware.device:
        # shadow vms only have exactly one disk
        if isinstance(device, vim.vm.device.VirtualDisk):
            shadowvm_backing_uuid = device.backing.uuid
    # get this information fresh from the vcenter and update the class cached values too
    instance.hardware.device = instance.handle.config.hardware.device
    instance_backing_uuid = None
    for device in instance.hardware.device:
        if isinstance(device, vim.vm.device.VirtualDisk) and device.backing.uuid == shadowvm_backing_uuid:
                instance_backing_uuid = device.backing.uuid
    if not instance_backing_uuid:
        log.warning("- WARN - volume {} seems to not be attached to instance {} in the vc".format(volume_uuid, instance_uuid))
        result = False
    else:
        # if the volume is attached to the instance in the vc make sure the backing uuid matches the extraconfig value
        # get the volume attachment which nova has written into the extraConfig
        extraconfig = instance.handle.config.extraConfig
        extraconfig_volume_uuid = None
        if extraconfig:
            # this should always point to the proper shadow vm even if thigs are not ok anymore
            # here the key is relevant for comparing to the backing uuid or the openstack volume uuid
            for entry in extraconfig:
                match = re.search(r"^volume-(.*)", entry.key)
                if not match:
                    continue
                if entry.value == instance_backing_uuid:
                    extraconfig_volume_uuid = entry.value
    if not extraconfig_volume_uuid:
        log.warning("- WARN - volume {} attached to instance {} does not seem to have a proper extraconfig volume entry".format(volume_uuid, instance_uuid))
        result = False
    log.info("- INFO -   shadow vm backing uuid: {}".format(shadowvm_backing_uuid))
    log.info("- INFO -   instance backing uuid: {}".format(instance_backing_uuid))
    log.info("- INFO -   extraconfig volume uuid: {}".format(extraconfig_volume_uuid))

    if shadowvm_backing_uuid != instance_backing_uuid:
        log.warning("- WARN - the backing uuid of the shadow vm {} does not match that of the instance {}",format(shadowvm_backing_uuid, instance_backing_uuid))
        result = False

    if instance_backing_uuid != extraconfig_volume_uuid:
        log.warning("- WARN - the backing uuid of the instance {} does not match the uuid in the volume extraConfig {}",format(instance_backing_uuid, extraconfig_volume_uuid))
        result = False

    return result


def main():

    args = parse_commandline()

    print(args.dry_run)

    log_level = logging.INFO
    if args.debug:
        log_level = logging.DEBUG

    logging.basicConfig(level=log_level, format='%(asctime)-15s %(message)s')
    
    # create a connection to openstack
    log.info("- INFO - region: {} - vcenter: {}".format(os.getenv('OS_REGION'), args.vcenter_host))
    log.info("- INFO -  connecting to openstack")
    oh = OpenstackHelper(os.getenv('OS_REGION'), os.getenv('OS_USER_DOMAIN_NAME'), os.getenv('OS_PROJECT_DOMAIN_NAME'),
                                    os.getenv('OS_PROJECT_NAME'), os.getenv('OS_USERNAME'), os.getenv('OS_PASSWORD'))
    # connect to the vcenter
    vc = VCenterHelper(host=args.vcenter_host, user=args.vcenter_user, password=args.vcenter_password)

    # get the vm and ds info from the vcenter
    vm_info = VMs(vc)
    ds_info = DataStores(vc)

    # this is just for debugging to get rid of accidental maintenance states
    #oh.unlock_volume_vc(args.volume_uuid, args.vcenter_host)

    if not os_check_for_leftovers(args, oh):
        log.warning("- WARN - storage_balancing metadata found in os - giving up")
        return False
    
    if not vc_check_for_leftovers(args, vm_info):
        log.warning("- WARN - storage_balancing metadata found in vc - giving up")
        return False

    instance_uuid = os_check_attach_status(args, oh, args.volume_uuid)

    connected_hosts = vc_check_ds_connected_hosts(vc, ds_info, args.target_ds)

    if instance_uuid:
        instance = vm_info.get_by_instanceuuid(instance_uuid)
        instance_node = instance.handle.runtime.host
        if instance_node in connected_hosts:
            log.info("- INFO -  the target ds {} is available on the node {} the instance {} with the volume {} attached is running on".format(args.target_ds, instance_node.name, instance_uuid, args.volume_uuid))
        else:
            log.warning("- WARN - the target ds {} is not available on the node {} the instance {} with the volume {} attached is running on - giving up".format(args.target_ds, instance_node.name, instance_uuid, args.volume_uuid))
            return False
        if instance_uuid == args.instance_uuid:
            result = vc_check_attach_status(args, vc, vm_info, args.volume_uuid, args.instance_uuid)
            if result:
                log.info("- INFO - moving volume {} attached to instance {} to ds {}".format(args.volume_uuid, args.instance_uuid, args.target_ds))
                attached_volume_move_test(args, vc, vm_info, ds_info, oh)
        else:
            log.warning("- WARN - the instance {} the volume {} is attached to does not match the one given on the cmdline {} - not doing anything",format(instance_uuid, args.volume_uuid, args.instance_uuid))
    else:
        log.info("- INFO -   moving detached volume {} to ds {}".format(args.volume_uuid, args.instance_uuid, args.target_ds))
        detached_volume_move_test(args, vc, vm_info, ds_info, oh)


if __name__ == '__main__':
    main()
