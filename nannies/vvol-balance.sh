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
if [ "$VVOL_BALANCE_DRY_RUN" = "False" ] || [ "$VVOL_BALANCE_DRY_RUN" = "false" ]; then
    DRY_RUN=""
else
    DRY_RUN="--dry-run"
fi
if [ "$VVOL_BALANCE_AUTOPILOT" = "False" ] || [ "$VVOL_BALANCE_AUTOPILOT" = "false" ]; then
    AUTOPILOT=""
else
    if [ "$VVOL_BALANCE_AUTOPILOT_RANGE" != "" ]; then
        AUTOPILOT="--autopilot --autopilot-range $VVOL_BALANCE_AUTOPILOT_RANGE"
    else
        AUTOPILOT="--autopilot"
    fi
fi
if [ "$VVOL_BALANCE_PROJECT_DENYLIST" != "" ]; then
    PROJECT_DENYLIST="--project-denylist $VVOL_BALANCE_PROJECT_DENYLIST"
else
    PROJECT_DENYLIST=""
fi

python3 /scripts/vvol_balance.py $DRY_RUN --vcenter-host $VVOL_BALANCE_VCHOST --vcenter-user $VVOL_BALANCE_VCUSER \
    --vcenter-password $VVOL_BALANCE_VCPASSWORD --netapp-user $VVOL_BALANCE_NAUSER \
    --netapp-password $VVOL_BALANCE_NAPASSWORD --region $VVOL_BALANCE_REGION --interval $VVOL_BALANCE_INTERVAL \
    --min-usage $VVOL_BALANCE_MIN_USAGE --max-usage $VVOL_BALANCE_MAX_USAGE \
    --min-max-difference $VVOL_BALANCE_MIN_MAX_DIFFERENCE --min-freespace $VVOL_BALANCE_MIN_FREESPACE \
    --max-move-vms $VVOL_BALANCE_MAX_MOVE_VMS --aggr-volume-min-size $VVOL_BALANCE_AGGR_VOLUME_MIN_SIZE \
    --aggr-volume-max-size $VVOL_BALANCE_AGGR_VOLUME_MAX_SIZE --ds-volume-min-size $VVOL_BALANCE_DS_VOLUME_MIN_SIZE \
    --ds-volume-max-size $VVOL_BALANCE_DS_VOLUME_MAX_SIZE --flexvol-max-size $VVOL_BALANCE_FLEXVOL_MAX_SIZE \
    --print-max $VVOL_BALANCE_PRINT_MAX $AUTOPILOT $PROJECT_DENYLIST
