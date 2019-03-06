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
# this is a temporary hack to avoid annoying raven warnings - we do not need sentry for this nanny for now
sed -i 's,raven\.handlers\.logging\.SentryHandler,logging.NullHandler,g' /etc/nova/logging.ini

# this is to handle the case of having a second cell db for nova
if [ "$NOVA_CELL2_ENABLED" = "True" ] || [ "$NOVA_CELL2_ENABLED" = "true" ]; then
    if [ -f /etc/nova/nova-cell2.conf ]; then
        # we append the cell2 config file to the regular one, so that the get_db_url function will find
        # the cell2 db string as it uses the last string provided if multiple connection sections are
        # given like in the resulting file, on the other side it still contains all the other config
        # options from the original nova.conf file, which are partially needed for the sync as well
        cat /etc/nova/nova-cell2.conf >> /etc/nova/nova.conf
    else
        echo "ERROR: PLEASE CHECK MANUALLY - nova cell2 is enabled, but there is no /etc/nova/nova-cell2.conf file - giving up!"
        exit 1
    fi
fi

# we run an endless loop to run the script periodically
echo "INFO: starting a loop to periodically run the nanny job for the nova instance info cache sync from neutron"
while true; do
    if [ "$NOVA_SYNC_NEUTRON_CACHE_ENABLED" = "True" ] || [ "$NOVA_SYNC_NEUTRON_CACHE_ENABLED" = "true" ]; then
        if [ "$NOVA_SYNC_NEUTRON_CACHE_DRY_RUN" = "False" ] || [ "$NOVA_SYNC_NEUTRON_CACHE_DRY_RUN" = "false" ]; then
            echo -n "INFO: syncing nova instance info cache from neutron - "
            date
            /var/lib/kolla/venv/bin/python /scripts/nova-sync-neutron-cache.py --config /etc/nova/nova.conf
        else
            echo -n "INFO: comparing nova instance info cache with neutron values - "
            date
            /var/lib/kolla/venv/bin/python /scripts/nova-sync-neutron-cache.py --dry-run --config /etc/nova/nova.conf
        fi
    fi
    echo -n "INFO: waiting $NOVA_SYNC_NEUTRON_CACHE_INTERVAL minutes before starting the next loop run - "
    date
    sleep $(( 60 * $NOVA_SYNC_NEUTRON_CACHE_INTERVAL ))
done
