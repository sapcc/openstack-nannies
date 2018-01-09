#!/bin/bash

echo -n "INFO: cleaning up nova entities without a valid project in the nova db - "
date
if [ "$NOVA_DB_CLEANUP_DRY_RUN" = "True" ] || [ "$NOVA_DB_CLEANUP_DRY_RUN" = "true" ]; then
  DRY_RUN="--dry-run"
fi

export OS_USER_DOMAIN_NAME
export OS_PROJECT_NAME
export OS_PASSWORD
export OS_AUTH_URL
export OS_USERNAME
export OS_PROJECT_DOMAIN_NAME

/var/lib/kolla/venv/bin/python /scripts/db-cleanup.py $DRY_RUN --iterations $NOVA_DB_CLEANUP_ITERATIONS --interval $NOVA_DB_CLEANUP_INTERVAL --nova
