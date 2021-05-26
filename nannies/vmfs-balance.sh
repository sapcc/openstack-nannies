#!/bin/bash
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

echo -n "INFO: vmfs ds balancing (dry-run only for now) - "
date
if [ "$VMFS_BALANCE_DRY_RUN" = "False" ] || [ "$VMFS_BALANCE_DRY_RUN" = "false" ]; then
    DRY_RUN=""
else
    DRY_RUN="--dry-run"
fi
if [ "$VMFS_BALANCE_HDD" = "True" ] || [ "$VMFS_BALANCE_HDD" = "true" ]; then
    BALANCE_HDD="--hdd"
else
    BALANCE_HDD=""
fi
if [ "$VMFS_BALANCE_AUTOPILOT" = "False" ] || [ "$VMFS_BALANCE_AUTOPILOT" = "false" ]; then
    AUTOPILOT=""
else
    if [ "$VMFS_BALANCE_AUTOPILOT_RANGE" != "" ]; then
        AUTOPILOT="--autopilot --autopilot-range $VMFS_BALANCE_AUTOPILOT_RANGE"
    else
        AUTOPILOT="--autopilot"
    fi
fi

python3 /scripts/vmfs_balance.py $DRY_RUN --vcenter-host $VMFS_BALANCE_VCHOST --vcenter-user $VMFS_BALANCE_VCUSER --vcenter-password $VMFS_BALANCE_VCPASSWORD --netapp-user $VMFS_BALANCE_NAUSER --netapp-password $VMFS_BALANCE_NAPASSWORD --region $VMFS_BALANCE_REGION --interval $VMFS_BALANCE_INTERVAL --min-usage $VMFS_BALANCE_MIN_USAGE --max-usage $VMFS_BALANCE_MAX_USAGE --min-max-difference $VMFS_BALANCE_MIN_MAX_DIFFERENCE --min-freespace $VMFS_BALANCE_MIN_FREESPACE --max-move-vms $VMFS_BALANCE_MAX_MOVE_VMS --aggr-volume-min-size $VMFS_BALANCE_AGGR_VOLUME_MIN_SIZE --aggr-volume-max-size $VMFS_BALANCE_AGGR_VOLUME_MAX_SIZE --ds-volume-min-size $VMFS_BALANCE_DS_VOLUME_MIN_SIZE --ds-volume-max-size $VMFS_BALANCE_DS_VOLUME_MAX_SIZE --print-max $VMFS_BALANCE_PRINT_MAX $AUTOPILOT $BALANCE_HDD
