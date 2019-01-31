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

echo -n "INFO: checking consistency between vcenter , nova and cinder - "
date
if [ "$VCENTER_CONSISTENCY_DRY_RUN" = "False" ] || [ "$VCENTER_CONSISTENCY_DRY_RUN" = "false" ]; then
    DRY_RUN=""
else
    DRY_RUN="--dry-run"
fi

export OS_USER_DOMAIN_NAME
export OS_PROJECT_NAME
export OS_PASSWORD
export OS_AUTH_URL
export OS_USERNAME
export OS_PROJECT_DOMAIN_NAME

/var/lib/kolla/venv/bin/python /scripts/vcenter-cleanup.py $DRY_RUN $POWER_OFF $UNREGISTER $DELETE $DETACH_GHOST_PORTS $DETACH_GHOST_VOLUMES $DETACH_GHOST_LIMIT $VOL_CHECK --vchost $VCENTER_CLEANUP_HOST --vcusername $VCENTER_CLEANUP_USER --vcpassword $VCENTER_CLEANUP_PASSWORD --iterations $VCENTER_CONSISTENCY_ITERATIONS --interval $VCENTER_CONSISTENCY_INTERVAL