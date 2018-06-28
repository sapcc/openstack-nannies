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

echo "INFO: copying nova config files to /etc/nova"
cp -v /nova-etc/* /etc/nova

# we run an endless loop to run the script periodically
echo "INFO: starting a loop to periodically run the nanny job for the nova instance info cache sync from neutron"
while true; do
    if [ "$NOVA_SYNC_NEUTRON_CACHE_ENABLED" = "True" ] || [ "$NOVA_SYNC_NEUTRON_CACHE_ENABLED" = "true" ]; then
        if [ "$NOVA_SYNC_NEUTRON_CACHE_DRY_RUN" = "False" ] || [ "$NOVA_SYNC_NEUTRON_CACHE_DRY_RUN" = "false" ]; then
            echo -n "INFO: comparing nova instance info cache with neutron values - "
            date
            /var/lib/kolla/venv/bin/python /scripts/nova-sync-neutron-cache.py --config /etc/nova/nova.conf
        else
            echo -n "INFO: syncing nova instance info cache sync from neutron (ONLY DRY RUN FOR NOW) - "
            date
            /var/lib/kolla/venv/bin/python /scripts/nova-sync-neutron-cache.py --dry-run --config /etc/nova/nova.conf
        fi
    fi
    echo -n "INFO: waiting $NOVA_SYNC_NEUTRON_CACHE_INTERVAL minutes before starting the next loop run - "
    date
    sleep $(( 60 * $NOVA_SYNC_NEUTRON_CACHE_INTERVAL ))
done
