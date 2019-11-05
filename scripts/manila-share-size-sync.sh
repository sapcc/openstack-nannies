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
echo "INFO: starting a loop to periodically run the nanny job for the manila share size sync"
while true; do
    if [ "$MANILA_SHARE_SIZE_SYNC_ENABLED" = "True" ] || [ "$MANILA_SHARE_SIZE_SYNC_ENABLED" = "true" ]; then
        if [ "$MANILA_SHARE_SIZE_SYNC_DRY_RUN" = "False" ] || [ "$MANILA_SHARE_SIZE_SYNC_DRY_RUN" = "false" ]; then
            echo -n "INFO: checking and fixing manila db share size - "
            date
            /var/lib/kolla/venv/bin/python /scripts/manila-share-size-sync.py \
                --config /etc/manila/manila.conf \
                --promhost $PROMETHEUS_HOST
        else
            echo -n "INFO: checking manila db share size - "
            date
            /var/lib/kolla/venv/bin/python /scripts/manila-share-size-sync.py \
                --config /etc/manila/manila.conf \
                --promhost $PROMETHEUS_HOST \
                --dry-run
        fi
    fi
    echo -n "INFO: waiting $MANILA_NANNY_INTERVAL seconds before starting the next loop run - "
    date
    sleep $(( $MANILA_NANNY_INTERVAL ))
done
