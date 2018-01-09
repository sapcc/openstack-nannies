#!/bin/bash

set -e

unset http_proxy https_proxy all_proxy no_proxy

echo "INFO: copying cinder config files to /etc/cinder"
cp -v /cinder-etc/* /etc/cinder

# export the env we get from kubernetes - not really required, as we source the corresponding script
export CINDER_DB_PURGE_OLDER_THAN
  
# we run an endless loop to run the script periodically
echo "INFO: starting a loop to periodically run the nany jobs for the cinder db"
while true; do
  if [ "$CINDER_QUOTA_SYNC_ENABLED" = "True" ] || [ "$CINDER_QUOTA_SYNC_ENABLED" = "true" ]; then
    echo "INFO: sync cinder quotas"
    for i in `/var/lib/kolla/venv/bin/python /scripts/cinder-quota-sync.py --config /etc/cinder/cinder.conf --list_projects`; do
      echo project: $i
      /var/lib/kolla/venv/bin/python /scripts/cinder-quota-sync.py --config /etc/cinder/cinder.conf --sync --project_id $i
    done
  fi
  if [ "$CINDER_DB_PURGE_ENABLED" = "True" ] || [ "$CINDER_DB_PURGE_ENABLED" = "true" ]; then
    echo "INFO: purge old deleted entities from the cinder db"
    . /scripts/cinder-db-purge.sh
  fi
  echo "INFO: waiting $CINDER_NANNY_INTERVAL minutes before starting the next loop run"
  sleep $(( 60 * $CINDER_NANNY_INTERVAL ))
done
