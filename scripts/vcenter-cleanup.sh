#!/bin/bash
#
# Copyright (c) 2018 SAP SE
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

echo -n "INFO: cleaning up vcenter entities without valid openstack counterparts - "
date
if [ "$VCENTER_CLEANUP_DRY_RUN" = "False" ] || [ "$VCENTER_CLEANUP_DRY_RUN" = "false" ]; then
    DRY_RUN=""
else
    DRY_RUN="--dry-run"
fi

if [ "$VCENTER_CLEANUP_POWER_OFF" = "True" ] || [ "$VCENTER_CLEANUP_POWER_OFF" = "true" ]; then
    POWER_OFF="--power-off"
else
    POWER_OFF=""
fi

if [ "$VCENTER_CLEANUP_UNREGISTER" = "True" ] || [ "$VCENTER_CLEANUP_UNREGISTER" = "true" ]; then
    UNREGISTER="--unregister"
else
    UNREGISTER=""
fi

if [ "$VCENTER_CLEANUP_DELETE" = "True" ] || [ "$VCENTER_CLEANUP_DELETE" = "true" ]; then
    DELETE="--delete"
else
    DELETE=""
fi

if [ "$VCENTER_CLEANUP_DETACH_GHOST_PORTS" = "True" ] || [ "$VCENTER_CLEANUP_DETACH_GHOST_PORTS" = "true" ]; then
    DETACH_GHOST_PORTS="--detach-ghost-ports"
else
    DETACH_GHOST_PORTS=""
fi

if [ "$VCENTER_CLEANUP_DETACH_GHOST_VOLUMES" = "True" ] || [ "$VCENTER_CLEANUP_DETACH_GHOST_VOLUMES" = "true" ]; then
    DETACH_GHOST_VOLUMES="--detach-ghost-volumes"
else
    DETACH_GHOST_VOLUMES=""
fi

if [ "$VCENTER_CLEANUP_DETACH_GHOST_LIMIT" != "" ]; then
    DETACH_GHOST_LIMIT="--detach-ghost-limit $VCENTER_CLEANUP_DETACH_GHOST_LIMIT"
else
    DETACH_GHOST_LIMIT=""
fi

if [ "$VCENTER_CLEANUP_VOL_CHECK" = "True" ] || [ "$VCENTER_CLEANUP_VOL_CHECK" = "true" ]; then
    VOL_CHECK="--vol-check"
else
    VOL_CHECK=""
fi

if [ "$VCENTER_CLEANUP_BIGVM_SIZE" != "" ]; then
    BIGVM_SIZE="--bigvm-size $VCENTER_CLEANUP_BIGVM_SIZE"
else
    BIGVM_SIZE=""
fi

export OS_USER_DOMAIN_NAME
export OS_PROJECT_NAME
export OS_PASSWORD
export OS_AUTH_URL
export OS_USERNAME
export OS_PROJECT_DOMAIN_NAME

/var/lib/kolla/venv/bin/python /scripts/vcenter-cleanup.py $DRY_RUN $POWER_OFF $UNREGISTER $DELETE $DETACH_GHOST_PORTS $DETACH_GHOST_VOLUMES $DETACH_GHOST_LIMIT $VOL_CHECK $BIGVM_SIZE --host $VCENTER_CLEANUP_HOST --username $VCENTER_CLEANUP_USER --password $VCENTER_CLEANUP_PASSWORD --iterations $VCENTER_CLEANUP_ITERATIONS --interval $VCENTER_CLEANUP_INTERVAL
