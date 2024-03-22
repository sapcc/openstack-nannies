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

set -e

unset http_proxy https_proxy all_proxy no_proxy

# we run an endless loop to run the script periodically
if [ "$MANILA_QUOTA_SYNC_ENABLED" = "True" ] || [ "$MANILA_QUOTA_SYNC_ENABLED" = "true" ]; then
    if [ "$MANILA_QUOTA_SYNC_DRY_RUN" = "False" ] || [ "$MANILA_QUOTA_SYNC_DRY_RUN" = "false" ]; then
        echo "INFO: run nanny job for the manila quota sync"
        /var/lib/openstack/bin/python /scripts/manila-quota-sync.py
    else
        echo "INFO: run nanny job for the manila quota sync in dry-run mode only!"
        /var/lib/openstack/bin/python /scripts/manila-quota-sync.py --dry-run
    fi
fi
