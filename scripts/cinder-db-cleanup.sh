#!/bin/bash

echo -n "INFO: cleaning up cinder entities without a valid project in the cinder db - "
date

export OS_USER_DOMAIN_NAME
export OS_PROJECT_NAME
export OS_PASSWORD
export OS_AUTH_URL
export OS_USERNAME
export OS_PROJECT_DOMAIN_NAME

if [ "$CINDER_DB_CLEANUP_DRY_RUN" = "True" ] || [ "$CINDER_DB_CLEANUP_DRY_RUN" = "true" ]; then
  DRY_RUN="--dry-run"
fi
/var/lib/kolla/venv/bin/python /scripts/db-cleanup.py $DRY_RUN --iterations $CINDER_DB_CLEANUP_ITERATIONS --interval $CINDER_DB_CLEANUP_INTERVAL --cinder
