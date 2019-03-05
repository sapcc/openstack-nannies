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

# export the env we get from kubernetes - not really required, as we source the corresponding script
export NOVA_DB_PURGE_DRY_RUN
export NOVA_DB_PURGE_MAX_NUMBER
export NOVA_DB_PURGE_OLDER_THAN

# this is to handle the case of having a second cell db for nova
if [ "$NOVA_CELL2_ENABLED" = "True" ] || [ "$NOVA_CELL2_ENABLED" = "true" ]; then
    if [ -f /etc/nova/nova-cell2.conf ]; then
        cp /etc/nova/nova-cell2.conf /etc/nova/nova.conf
    else
        echo "ERROR: PLEASE CHECK MANUALLY - nova cell2 is enabled, but there is no /etc/nova/nova-cell2.conf file - giving up!"
        exit 1
    fi
fi

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
            echo -n "INFO: checking and fixing nova db consistency - "
            date
            /var/lib/kolla/venv/bin/python /scripts/nova-consistency.py --config /etc/nova/nova.conf $OLDER_THAN $MAX_INSTANCE_FAULTS
        else
            echo -n "INFO: checking nova db consistency - "
            date
            /var/lib/kolla/venv/bin/python /scripts/nova-consistency.py --config /etc/nova/nova.conf --dry-run
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
        /var/lib/kolla/venv/bin/python /scripts/nova-queens-instance-mapping.py --config /etc/nova/nova.conf $DRY_RUN
    fi
    if [ "$NOVA_DB_PURGE_ENABLED" = "True" ] || [ "$NOVA_DB_PURGE_ENABLED" = "true" ]; then
        echo -n "INFO: purge old deleted instances from the nova db - "
        date
        if [ "$NOVA_DB_PURGE_DRY_RUN" = "False" ] ||  [ "$NOVA_DB_PURGE_DRY_RUN" = "false" ]; then
            echo -n "INFO: "
            DRY_RUN=""
        else
            echo -n "INFO: dry run mode only - "
            DRY_RUN="--dry-run"
        fi
        echo -n "purging at max $NOVA_DB_PURGE_MAX_NUMBER deleted instances older than $NOVA_DB_PURGE_OLDER_THAN days from the nova db - "
        echo -n `date`
        echo -n " - "
        /var/lib/kolla/venv/bin/nova-manage db purge_deleted_instances $DRY_RUN --older-than $NOVA_DB_PURGE_OLDER_THAN --max-number $NOVA_DB_PURGE_MAX_NUMBER
    fi
    echo -n "INFO: waiting $NOVA_NANNY_INTERVAL minutes before starting the next loop run - "
    date
    sleep $(( 60 * $NOVA_NANNY_INTERVAL ))
done
