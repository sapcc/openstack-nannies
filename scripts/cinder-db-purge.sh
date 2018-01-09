#!/bin/bash

echo -n "INFO: purging deleted cinder entities older than $CINDER_DB_PURGE_OLDER_THAN days from the cinder db - "
date
/var/lib/kolla/venv/bin/cinder-manage db purge $CINDER_DB_PURGE_OLDER_THAN
