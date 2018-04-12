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

echo "INFO: copying manila config files to /etc/manila"
cp -v /manila-etc/* /etc/manila

# we run an endless loop to run the script periodically
echo "INFO: starting a loop to periodically run the nanny job for the manila db consistency check and purge"
while true; do
    if [ "$MANILA_CONSISTENCY_ENABLED" = "True" ] || [ "$MANILA_CONSISTENCY_ENABLED" = "true" ]; then
        if [ "$MANILA_CONSISTENCY_DRY_RUN" = "False" ] || [ "$MANILA_CONSISTENCY_DRY_RUN" = "false" ]; then
            echo -n "INFO: checking and fixing manila db consistency - "
            date
            /var/lib/kolla/venv/bin/python /scripts/manila-consistency.py --config /etc/manila/manila.conf
        else
            echo -n "INFO: checking manila db consistency - "
            date
            /var/lib/kolla/venv/bin/python /scripts/manila-consistency.py --config /etc/manila/manila.conf --dry-run
        fi
    fi
    if [ "$MANILA_DB_PURGE_ENABLED" = "True" ] || [ "$MANILA_DB_PURGE_ENABLED" = "true" ]; then
        echo -n "INFO: purging deleted manila entities older than $MANILA_DB_PURGE_OLDER_THAN days from the manila db - "
        date
        /var/lib/kolla/venv/bin/manila-manage db purge $MANILA_DB_PURGE_OLDER_THAN
    fi
    echo -n "INFO: waiting $MANILA_NANNY_INTERVAL minutes before starting the next loop run - "
    date
    sleep $(( 60 * $MANILA_NANNY_INTERVAL ))
done
