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

if [ "$#" != "1" ]; then
  echo ""
  echo "reconstruct a missing volume_attachment entry in the cinder db from the corresponding block_device_mapping entry in the nova db"
  echo ""
  echo "usage: $0 volume_uuid_to_fix"
  echo ""
  exit 1
fi

if { [ "$NOVA_CELL2_ENABLED" = "True" ] || [ "$NOVA_CELL2_ENABLED" = "true" ]; } && \
  [ "$NOVA_CELL2_VC" = "$VCENTER_CONSISTENCY_HOST" ]; then
    NOVACONFIG=/nova-etc/nova-cell2.conf
else
    NOVACONFIG=/nova-etc/nova.conf
fi

CINDERCONFIG=/cinder-etc/cinder.conf

export OS_USER_DOMAIN_NAME
export OS_PROJECT_NAME
export OS_PASSWORD
export OS_AUTH_URL
export OS_USERNAME
export OS_PROJECT_DOMAIN_NAME

/var/lib/kolla/venv/bin/python /scripts/vcenter_consistency_fix_attachment.py --vchost $VCENTER_CONSISTENCY_HOST --vcusername $VCENTER_CONSISTENCY_USER --vcpassword $VCENTER_CONSISTENCY_PASSWORD --cinderconfig $CINDERCONFIG --novaconfig $NOVACONFIG --fix-uuid $1
