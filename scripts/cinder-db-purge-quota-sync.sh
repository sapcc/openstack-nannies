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
echo "INFO: starting a loop to periodically run the nanny jobs for the cinder db"
while true; do
  if [ "$CINDER_QUOTA_SYNC_ENABLED" = "True" ] || [ "$CINDER_QUOTA_SYNC_ENABLED" = "true" ]; then
    echo "INFO: sync cinder quotas"
    for i in `/var/lib/kolla/venv/bin/python /scripts/cinder-quota-sync.py --config /etc/cinder/cinder.conf --list_projects`; do
      echo project: $i
      /var/lib/kolla/venv/bin/python /scripts/cinder-quota-sync.py --config /etc/cinder/cinder.conf --sync --project_id $i
    done
  fi
  if [ "$CINDER_CONSISTENCY_ENABLED" = "True" ] || [ "$CINDER_CONSISTENCY_ENABLED" = "true" ]; then
    if [ "$CINDER_CONSISTENCY_DRY_RUN" = "True" ] || [ "$CINDER_CONSISTENCY_DRY_RUN" = "true" ]; then
      echo "INFO: checking cinder db consistency - volume attachments of deleted volumes"
      /var/lib/kolla/venv/bin/python /scripts/cinder-consistency.py --config /etc/cinder/cinder.conf --dry-run
    else
      echo "INFO: checking and fixing cinder db consistency"
      /var/lib/kolla/venv/bin/python /scripts/cinder-consistency.py --config /etc/cinder/cinder.conf
    fi
  fi
  if [ "$CINDER_DB_PURGE_ENABLED" = "True" ] || [ "$CINDER_DB_PURGE_ENABLED" = "true" ]; then
    echo -n "INFO: purging deleted cinder entities older than $CINDER_DB_PURGE_OLDER_THAN days from the cinder db - "
    date
    /var/lib/kolla/venv/bin/cinder-manage db purge $CINDER_DB_PURGE_OLDER_THAN
  fi
  echo "INFO: waiting $CINDER_NANNY_INTERVAL minutes before starting the next loop run"
  sleep $(( 60 * $CINDER_NANNY_INTERVAL ))
done
