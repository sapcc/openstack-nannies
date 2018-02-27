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
if [ "$VCENTER_CLEANUP_DRY_RUN" = "True" ] || [ "$VCENTER_CLEANUP_DRY_RUN" = "true" ]; then
  DRY_RUN="--dry-run"
fi
if [ "$VCENTER_CLEANUP_POWER_OFF" = "True" ] || [ "$VCENTER_CLEANUP_POWER_OFF" = "true" ]; then
  POWER_OFF="--power-off"
fi
if [ "$VCENTER_CLEANUP_UNREGISTER" = "True" ] || [ "$VCENTER_CLEANUP_UNREGISTER" = "true" ]; then
  UNREGISTER="--unregister"
fi
if [ "$VCENTER_CLEANUP_DELETE" = "True" ] || [ "$VCENTER_CLEANUP_DELETE" = "true" ]; then
  DELETE="--delete"
fi

export OS_USER_DOMAIN_NAME
export OS_PROJECT_NAME
export OS_PASSWORD
export OS_AUTH_URL
export OS_USERNAME
export OS_PROJECT_DOMAIN_NAME

/var/lib/kolla/venv/bin/python /scripts/vcenter-cleanup.py $DRY_RUN $POWER_OFF $UNREGISTER $DELETE --host $VCENTER_CLEANUP_HOST --username $VCENTER_CLEANUP_USER --password $VCENTER_CLEANUP_PASSWORD --iterations $VCENTER_CLEANUP_ITERATIONS --interval $VCENTER_CLEANUP_INTERVAL
