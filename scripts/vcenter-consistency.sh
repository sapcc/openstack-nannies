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

echo -n "INFO: checking consistency between vcenter, nova and cinder - "
date
if [ "$VCENTER_CONSISTENCY_DRY_RUN" = "False" ] || [ "$VCENTER_CONSISTENCY_DRY_RUN" = "false" ]; then
    DRY_RUN=""
else
    DRY_RUN="--dry-run"
fi

if [ "$VCENTER_CONSISTENCY_FIX_LIMIT" != "" ]; then
    FIX_LIMIT="--fix-limit $VCENTER_CONSISTENCY_FIX_LIMIT"
else
    FIX_LIMIT=""
fi

if { [ "$NOVA_CELL2_ENABLED" = "True" ] || [ "$NOVA_CELL2_ENABLED" = "true" ]; } && \
  [ "$NOVA_CELL2_VC" = "$VCENTER_CONSISTENCY_HOST" ]; then
    NOVACONFIG=/nova-etc/nova-cell2.conf
else
    NOVACONFIG=/nova-etc/nova.conf
fi

CINDERCONFIG=/cinder-etc/cinder.conf

export OS_USER_DOMAIN_NAME
export OS_PROJECT_NAME
export OS_PASSWORD
export OS_AUTH_URL
export OS_USERNAME
export OS_PROJECT_DOMAIN_NAME

/var/lib/kolla/venv/bin/python /scripts/vcenter_consistency_check.py $DRY_RUN --vchost $VCENTER_CONSISTENCY_HOST --vcusername $VCENTER_CONSISTENCY_USER --vcpassword $VCENTER_CONSISTENCY_PASSWORD --iterations $VCENTER_CONSISTENCY_ITERATIONS --interval $VCENTER_CONSISTENCY_INTERVAL --cinderconfig $CINDERCONFIG --novaconfig $NOVACONFIG $FIX_LIMIT