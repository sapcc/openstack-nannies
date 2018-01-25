#!/bin/bash

echo -n "INFO: cleaning up vcenter entities without valid openstack counterparts - "
date
if [ "$VCENTER_CLEANUP_DRY_RUN" = "True" ] || [ "$VCENTER_CLEANUP_DRY_RUN" = "true" ]; then
  DRY_RUN="--dry-run"
fi
if [ "$VCENTER_CLEANUP_POWER_OFF" = "True" ] || [ "$VCENTER_CLEANUP_POWER_OFF" = "true" ]; then
  POWER_OFF="--power-off"
fi
if [ "$VCENTER_CLEANUP_UNREGISTER" = "True" ] || [ "$VCENTER_CLEANUP_UNREGISTER" = "true" ]; then
  UNREGISTER="--unregister"
fi
if [ "$VCENTER_CLEANUP_DELETE" = "True" ] || [ "$VCENTER_CLEANUP_DELETE" = "true" ]; then
  DELETE="--delete"
fi

export OS_USER_DOMAIN_NAME
export OS_PROJECT_NAME
export OS_PASSWORD
export OS_AUTH_URL
export OS_USERNAME
export OS_PROJECT_DOMAIN_NAME

/var/lib/kolla/venv/bin/python /scripts/vcenter-cleanup.py $DRY_RUN $POWER_OFF $UNREGISTER $DELETE --host $VCENTER_CLEANUP_HOST --username $VCENTER_CLEANUP_USER --password $VCENTER_CLEANUP_PASSWORD --iterations $VCENTER_CLEANUP_ITERATIONS --interval $VCENTER_CLEANUP_INTERVAL
