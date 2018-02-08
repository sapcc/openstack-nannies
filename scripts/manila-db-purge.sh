#!/bin/bash

set -e

unset http_proxy https_proxy all_proxy no_proxy

echo "INFO: copying manila config files to /etc/manila"
cp -v /manila-etc/* /etc/manila

# we run an endless loop to run the script periodically
echo "INFO: starting a loop to periodically run the nanny jobs for the manila db"
while true; do
  if [ "$MANILA_DB_PURGE_ENABLED" = "True" ] || [ "$MANILA_DB_PURGE_ENABLED" = "true" ]; then
    echo -n "INFO: purging deleted cinder entities older than $MANILA_DB_PURGE_OLDER_THAN days from the cinder db - "
    date
    /var/lib/kolla/venv/bin/manila-manage db purge $MANILA_DB_PURGE_OLDER_THAN
  fi
  echo "INFO: waiting $MANILA_NANNY_INTERVAL minutes before starting the next loop run"
  sleep $(( 60 * $MANILA_NANNY_INTERVAL ))
done
