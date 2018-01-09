#!/bin/bash

if [ "$NOVA_DB_PURGE_DRY_RUN" = "true" ]; then
  echo -n "INFO: dry run mode only - "
  DRY_RUN="--dry-run"
else
  echo -n "INFO: "
fi
echo -n "purging at max $NOVA_DB_PURGE_MAX_NUMBER deleted instances older than $NOVA_DB_PURGE_OLDER_THAN days from the nova db - "
echo -n `date`
echo -n " - "
/var/lib/kolla/venv/bin/nova-manage db purge_deleted_instances $DRY_RUN --older-than $NOVA_DB_PURGE_OLDER_THAN --max-number $NOVA_DB_PURGE_MAX_NUMBER
