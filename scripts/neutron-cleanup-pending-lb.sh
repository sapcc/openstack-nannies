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

echo "INFO: copying neutron config files to /etc/neutron"
cp -v /neutron-etc/* /etc/neutron

echo -n "INFO: cleaning up lbaas loadbalancer entries with wrong PENDING_* state - "
date
if [ "$NEUTRON_CLEANUP_PENDING_LB_DRY_RUN" = "False" ] || [ "$NEUTRON_CLEANUP_PENDING_LB_DRY_RUN" = "false" ]; then
    DRY_RUN=""
else
    DRY_RUN="--dry-run"
fi

/var/lib/openstack/bin/python2 /scripts/neutron-cleanup-pending-lb.py --config /etc/neutron/neutron.conf $DRY_RUN --interval $NEUTRON_CLEANUP_PENDING_LB_INTERVAL --iterations $NEUTRON_CLEANUP_PENDING_LB_ITERATIONS