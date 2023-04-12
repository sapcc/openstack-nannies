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
cp -vrL /nova-etc/* /etc/nova
# # this is a temporary hack to avoid annoying raven warnings - we do not need sentry for this nanny for now
# sed -i 's,raven\.handlers\.logging\.SentryHandler,logging.NullHandler,g' /etc/nova/logging.ini

# this is to handle the case of having a second cell db for nova
if [ "$NOVA_CELL2_ENABLED" = "True" ] || [ "$NOVA_CELL2_ENABLED" = "true" ]; then
    if [ -f /etc/nova/nova-cell2.conf ]; then
        # in the cell2 case we simply replace the db.conf file used explicitely by some of the
        # scripts or implicitely by the nova-manage db purge_deleted_instances tool by the cell2
        # config - it nearly only contains the db string and this is what we are interested in here
        # this copying of a nova conf to a db.conf is done here as the nova-cell2.conf has not been
        # converted to the new structure of having a separate config file for the db config
        cp -f /etc/nova/nova-cell2.conf /etc/nova/nova.conf.d/db.conf
    else
        echo "ERROR: PLEASE CHECK MANUALLY - nova cell2 is enabled, but there is no /etc/nova/nova-cell2.conf file - giving up!"
        exit 1
    fi
fi

# nova is now using proxysql by default in its config - change that back to a normal
# config for the nanny as we do not need it and do not have the proxy around by default
sed -i 's,@/nova?unix_socket=/run/proxysql/mysql.sock&,@nova-mariadb/nova?,g' /etc/nova/nova.conf.d/db.conf

# we run an endless loop to run the script periodically
echo "INFO: starting a loop to periodically run the nanny job for the nova instance info cache sync from neutron"
while true; do
    if [ "$NOVA_SYNC_NEUTRON_CACHE_ENABLED" = "True" ] || [ "$NOVA_SYNC_NEUTRON_CACHE_ENABLED" = "true" ]; then
        if [ "$NOVA_SYNC_NEUTRON_CACHE_DRY_RUN" = "False" ] || [ "$NOVA_SYNC_NEUTRON_CACHE_DRY_RUN" = "false" ]; then
            # the sync-baremetal case is only relevant for the non dry-run mode, as in dry-run we do not sync anything anyway
            if [ "$NOVA_SYNC_NEUTRON_CACHE_BAREMETAL" = "True" ] || [ "$NOVA_SYNC_NEUTRON_CACHE_BAREMETAL" = "true" ]; then
                SYNC_BAREMETAL="--sync-baremetal"
            else
                SYNC_BAREMETAL=""
            fi
            echo -n "INFO: syncing nova instance info cache from neutron - "
            date
            python3 /scripts/nova-sync-neutron-cache.py $SYNC_BAREMETAL --config /etc/nova/nova.conf.d/db.conf
        else
            echo -n "INFO: comparing nova instance info cache with neutron values - "
            date
            python3 /scripts/nova-sync-neutron-cache.py --dry-run --config /etc/nova/nova.conf.d/db.conf
        fi
    fi
    echo -n "INFO: waiting $NOVA_SYNC_NEUTRON_CACHE_INTERVAL minutes before starting the next loop run - "
    date
    sleep $(( 60 * $NOVA_SYNC_NEUTRON_CACHE_INTERVAL ))
done
