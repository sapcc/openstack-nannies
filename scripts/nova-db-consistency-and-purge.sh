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

# export the env we get from kubernetes - not really required, as we source the corresponding script
export NOVA_DB_PURGE_DRY_RUN
export NOVA_DB_PURGE_MAX_NUMBER
export NOVA_DB_PURGE_OLDER_THAN

# this is to handle the case of having a second cell db for nova
if [ "$NOVA_CELL2_ENABLED" = "True" ] || [ "$NOVA_CELL2_ENABLED" = "true" ]; then
    if [ -f /etc/nova/nova.conf.d/cell2.conf ]; then
        # in the cell2 case we simply replace the api-db.conf file used explicitely by some of the
        # scripts or implicitely by the nova-manage db purge_deleted_instances tool by the cell2
        # config - it nearly only contains the db string and this is what we are interested in here
        # this copying of a nova conf to a api-db.conf is done here as the nova cell2.conf has not been
        # converted to the new structure of having a separate config file for the api-db config
        cp -f /etc/nova/nova.conf.d/cell2.conf /etc/nova/nova.conf.d/api-db.conf
    else
        echo "ERROR: PLEASE CHECK MANUALLY - nova cell2 is enabled, but there is no /etc/nova/nova.conf.d/cell2.conf file - giving up!"
        exit 1
    fi
fi

# nova is now using proxysql by default in its config - change that back to a normal
# config for the nanny as we do not need it and do not have the proxy around by default
sed -i 's,@/nova?unix_socket=/run/proxysql/mysql.sock&,@nova-mariadb/nova?,g' /etc/nova/nova.conf.d/api-db.conf

# we run an endless loop to run the script periodically
echo "INFO: starting a loop to periodically run the nanny job for the nova db concistency check and purge"
while true; do
    if [ "$NOVA_CONSISTENCY_ENABLED" = "True" ] || [ "$NOVA_CONSISTENCY_ENABLED" = "true" ]; then
        if [ "$NOVA_CONSISTENCY_DRY_RUN" = "False" ] || [ "$NOVA_CONSISTENCY_DRY_RUN" = "false" ]; then
            if [ "$NOVA_CONSISTENCY_OLDER_THAN" != "" ]; then
                OLDER_THAN="--older-than $NOVA_CONSISTENCY_OLDER_THAN"
            else
                OLDER_THAN=""
            fi
            if [ "$NOVA_CONSISTENCY_MAX_INSTANCE_FAULTS" != "" ]; then
                MAX_INSTANCE_FAULTS="--max-instance-faults $NOVA_CONSISTENCY_MAX_INSTANCE_FAULTS"
            else
                MAX_INSTANCE_FAULTS=""
            fi
            if [ "$NOVA_CONSISTENCY_FIX_LIMIT" != "" ]; then
                FIX_LIMIT="--fix-limit $NOVA_CONSISTENCY_FIX_LIMIT"
            else
                FIX_LIMIT=""
            fi
            echo -n "INFO: checking and fixing nova db consistency - "
            date
            python3 /scripts/nova-consistency.py --config /etc/nova/nova.conf.d/api-db.conf $OLDER_THAN $MAX_INSTANCE_FAULTS $FIX_LIMIT
        else
            echo -n "INFO: checking nova db consistency - "
            date
            python3 /scripts/nova-consistency.py --config /etc/nova/nova.conf.d/api-db.conf --dry-run
        fi
    fi
    if [ "$NOVA_QUEENS_INSTANCE_MAPPING_ENABLED" = "True" ] || [ "$NOVA_QUEENS_INSTANCE_MAPPING_ENABLED" = "true" ]; then
        if [ "$NOVA_QUEENS_INSTANCE_MAPPING_DRY_RUN" = "False" ] ||  [ "$NOVA_QUEENS_INSTANCE_MAPPING_DRY_RUN" = "false" ]; then
            echo -n "INFO: "
            DRY_RUN=""
        else
            echo -n "INFO: dry run mode only - "
            DRY_RUN="--dry-run"
        fi
        echo -n "Searching for inconsistent instance mappings and deleting duplicates - "
        date
        python3 /scripts/nova-queens-instance-mapping.py --config /etc/nova/nova.conf.d/api-db.conf $DRY_RUN
    fi
    if [ "$NOVA_DB_PURGE_ENABLED" = "True" ] || [ "$NOVA_DB_PURGE_ENABLED" = "true" ]; then
        # the purge_deleted_instances command meanwhile handles all cells so only required for the non cell2 case
        if [ "$NOVA_CELL2_ENABLED" = "False" ] || [ "$NOVA_CELL2_ENABLED" = "false" ]; then
            echo -n "INFO: archive old deleted instances from the nova db to shadow tables and later purge them - "
            date
            if [ "$NOVA_DB_PURGE_DRY_RUN" = "True" ] ||  [ "$NOVA_DB_PURGE_DRY_RUN" = "true" ]; then
                echo "IMPORTANT: dry run mode no longer supported"
            fi
            echo -n "INFO: archiving deleted db entries older than $NOVA_DB_PURGE_OLDER_THAN days to shadow tables with a batch size of $NOVA_DB_PURGE_MAX_NUMBER - "
            echo `date`
            nova-manage db archive_deleted_rows --until-complete --max_rows $NOVA_DB_PURGE_MAX_NUMBER --all-cells --verbose --before $(date -Id -d "now - $NOVA_DB_PURGE_OLDER_THAN days")
            echo -n "INFO: purging db entries older than $((2 * $NOVA_DB_PURGE_OLDER_THAN)) days from shadow tables - "
            echo `date`
            # `nova-manage db purge` exits with return code 3 if nothing was deleted. Catch this specific value and only that one.
            nova-manage db purge --verbose --all-cells --before $(date -Id -d "now - $((2 * $NOVA_DB_PURGE_OLDER_THAN)) days") || [[ "$?" == "3" ]]
        fi
    fi
    echo -n "INFO: waiting $NOVA_NANNY_INTERVAL minutes before starting the next loop run - "
    date
    sleep $(( 60 * $NOVA_NANNY_INTERVAL ))
done
