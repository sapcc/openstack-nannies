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
echo "INFO: starting a loop to periodically run the nanny job for the manila db consistency check and purge"
while true; do
    if [ "$MANILA_CONSISTENCY_ENABLED" = "True" ] || [ "$MANILA_CONSISTENCY_ENABLED" = "true" ]; then
        if [ "$MANILA_CONSISTENCY_DRY_RUN" = "False" ] || [ "$MANILA_CONSISTENCY_DRY_RUN" = "false" ]; then
            echo -n "INFO: checking and fixing manila db consistency - "
            date
            /var/lib/openstack/bin/python /scripts/manila-consistency.py --older-than ${MANILA_CONSISTENCY_OLDER_THAN:-2}
        else
            echo -n "INFO: checking manila db consistency - "
            date
            /var/lib/openstack/bin/python /scripts/manila-consistency.py --older-than ${MANILA_CONSISTENCY_OLDER_THAN:-2} --dry-run
        fi
    fi
    if [ "$MANILA_DB_PURGE_ENABLED" = "True" ] || [ "$MANILA_DB_PURGE_ENABLED" = "true" ]; then
        echo -n "INFO: purging deleted manila entities older than $MANILA_DB_PURGE_OLDER_THAN days from the manila db - "
        date
        /var/lib/openstack/bin/manila-manage --config-dir ${MANILA_NANNY_CONFIG_DIR:-"/etc/manila/"} db purge $MANILA_DB_PURGE_OLDER_THAN
    fi
    echo -n "INFO: waiting $MANILA_NANNY_INTERVAL seconds before starting the next loop run - "
    date
    sleep $MANILA_NANNY_INTERVAL
done
