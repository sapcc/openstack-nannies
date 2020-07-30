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

echo -n "INFO: netapp aggregate balancing (dry-run only for now) - "
date
if [ "$NETAPP_BALANCE_DRY_RUN" = "False" ] || [ "$NETAPP_BALANCE_DRY_RUN" = "false" ]; then
    DRY_RUN=""
else
    DRY_RUN="--dry-run"
fi

python3 /scripts/netapp_balance.py $DRY_RUN --vcenter-host $NETAPP_BALANCE_VCHOST --vcenter-user $NETAPP_BALANCE_VCUSER --vcenter-password $NETAPP_BALANCE_VCPASSWORD --netapp-user $NETAPP_BALANCE_NETAPPUSER --netapp-password $NETAPP_BALANCE_NETAPPPASSWORD --region $NETAPP_BALANCE_REGION --flexvol-size-limit $NETAPP_BALANCE_FLEXVOLSIZELIMIT --flexvol-lun-min-size $NETAPP_BALANCE_FLEXVOLLUNMINSIZE --flexvol-lun-max-size $NETAPP_BALANCE_FLEXVOLLUNMAXSIZE --aggr-lun-min-size $NETAPP_BALANCE_AGGRLUNMINSIZE --aggr-lun-max-size $NETAPP_BALANCE_AGGRLUNMAXSIZE --max-move-vms $NETAPP_BALANCE_MAXMOVEVMS --min-threshold $NETAPP_BALANCE_MINTHRESHOLD --max-threshold $NETAPP_BALANCE_MAXTHRESHOLD --max-threshold-hysteresis $NETAPP_BALANCE_MAXTHRESHOLDHYSTERESIS --interval $NETAPP_BALANCE_INTERVAL
