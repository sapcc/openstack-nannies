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

if [ "$CINDER_POSTGRESQL_PW" != "" ]; then
    CINDER_PW="--cinderpassword $CINDER_POSTGRESQL_PW"
else
    CINDER_PW=""
fi

if [ "$NOVA_POSTGRESQL_PW" != "" ]; then
    NOVA_PW="--novapassword $NOVA_POSTGRESQL_PW"
else
    NOVA_PW=""
fi

if [ "$REGION" != "" ]; then
    CURRENT_REGION="--region $REGION"
else
    CURRENT_REGION=""
fi

if [ "$VCENTER_CONSISTENCY_FIX_LIMIT" != "" ]; then
    FIX_LIMIT="--fix-limit $VCENTER_CONSISTENCY_FIX_LIMIT"
else
    FIX_LIMIT=""
fi

if { [ "$NOVA_CELL2_ENABLED" = "True" ] || [ "$NOVA_CELL2_ENABLED" = "true" ]; } && \
  [ "$NOVA_CELL2_DB_NAME" != "" ] && [ "$NOVA_CELL2_DB_USER" != "" ] && \
  [ "$NOVA_CELL2_AZ" = "$VCENTER_CONSISTENCY_HOST" ]; then
    CELL2_PARAMETERS="--novadbname $NOVA_CELL2_DB_NAME --novadbuser $NOVA_CELL2_DB_USER"
else
    CELL2_PARAMETERS=""
fi

export OS_USER_DOMAIN_NAME
export OS_PROJECT_NAME
export OS_PASSWORD
export OS_AUTH_URL
export OS_USERNAME
export OS_PROJECT_DOMAIN_NAME

/var/lib/kolla/venv/bin/python /scripts/vcenter_consistency_check.py $DRY_RUN --vchost $VCENTER_CONSISTENCY_HOST --vcusername $VCENTER_CONSISTENCY_USER --vcpassword $VCENTER_CONSISTENCY_PASSWORD --iterations $VCENTER_CONSISTENCY_ITERATIONS --interval $VCENTER_CONSISTENCY_INTERVAL $CINDER_PW $NOVA_PW $CURRENT_REGION $FIX_LIMIT $CELL2_PARAMETERS