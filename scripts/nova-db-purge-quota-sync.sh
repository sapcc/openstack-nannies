#!/bin/bash

set -e

unset http_proxy https_proxy all_proxy no_proxy

echo "INFO: copying nova config files to /etc/nova"
cp -v /nova-etc/* /etc/nova

# export the env we get from kubernetes - not really required, as we source the corresponding script
export NOVA_DB_PURGE_DRY_RUN
export NOVA_DB_PURGE_MAX_NUMBER
export NOVA_DB_PURGE_OLDER_THAN

# we run an endless loop to run the script periodically
echo "INFO: starting a loop to periodically run the nany jobs for the nova db"
while true; do
  if [ "$NOVA_QUOTA_SYNC_ENABLED" = "True" ] || [ "$NOVA_QUOTA_SYNC_ENABLED" = "true" ]; then
    echo "INFO: sync nova quotas"
    /var/lib/kolla/venv/bin/python /scripts/nova-quota-sync.py --all --auto_sync
  fi
  if [ "$NOVA_DB_PURGE_ENABLED" = "True" ] || [ "$NOVA_DB_PURGE_ENABLED" = "true" ]; then
    echo "INFO: purge old deleted instances from the nova db"
    . /scripts/nova-db-purge.sh
    echo "INFO: waiting $NOVA_NANNY_INTERVAL minutes before starting the next loop run"
    sleep $(( 60 * $NOVA_NANNY_INTERVAL ))
  fi
done
