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

echo -n "INFO: cleaning up manila entities without a valid project in the manila db - "
date

export OS_USER_DOMAIN_NAME
export OS_PROJECT_NAME
export OS_PASSWORD
export OS_AUTH_URL
export OS_USERNAME
export OS_PROJECT_DOMAIN_NAME

if [ "$MANILA_DB_CLEANUP_DRY_RUN" = "False" ] || [ "$MANILA_DB_CLEANUP_DRY_RUN" = "false" ]; then
    DRY_RUN=""
else
    DRY_RUN="--dry-run"
fi
# not yet implemented
#/var/lib/kolla/venv/bin/python /scripts/db-cleanup.py $DRY_RUN --iterations $MANILA_DB_CLEANUP_ITERATIONS --interval $MANILA_DB_CLEANUP_INTERVAL --manila
