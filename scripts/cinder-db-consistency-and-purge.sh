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

echo "INFO: copying cinder config files to /etc/cinder"
cp -v /cinder-etc/* /etc/cinder

# we run an endless loop to run the script periodically
echo "INFO: starting a loop to periodically run the nanny job for the cinder db consistency check and purge"
while true; do
    if [ "$CINDER_CONSISTENCY_ENABLED" = "True" ] || [ "$CINDER_CONSISTENCY_ENABLED" = "true" ]; then
        if [ "$CINDER_CONSISTENCY_DRY_RUN" = "False" ] || [ "$CINDER_CONSISTENCY_DRY_RUN" = "false" ]; then
            echo -n "INFO: checking and fixing cinder db consistency"
            date
            /var/lib/kolla/venv/bin/python /scripts/cinder-consistency.py --config /etc/cinder/cinder.conf
        else
            echo -n "INFO: checking cinder db consistency"
            date
            /var/lib/kolla/venv/bin/python /scripts/cinder-consistency.py --config /etc/cinder/cinder.conf --dry-run
        fi
    fi
    if [ "$CINDER_DB_PURGE_ENABLED" = "True" ] || [ "$CINDER_DB_PURGE_ENABLED" = "true" ]; then
        echo -n "INFO: purging deleted cinder entities older than $CINDER_DB_PURGE_OLDER_THAN days from the cinder db"
        date
        /var/lib/kolla/venv/bin/cinder-manage db purge $CINDER_DB_PURGE_OLDER_THAN
    fi
    echo -n "INFO: waiting $CINDER_NANNY_INTERVAL minutes before starting the next loop run"
    date
    sleep $(( 60 * $CINDER_NANNY_INTERVAL ))
done
