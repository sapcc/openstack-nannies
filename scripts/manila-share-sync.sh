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
set -u

unset http_proxy https_proxy all_proxy no_proxy

echo "INFO: copying manila config files to /etc/manila"
cp -v /manila-etc/* /etc/manila

echo "INFO: syncing between manila share and backend"
/var/lib/openstack/bin/python /scripts/manila-share-sync.py \
    --config /etc/manila/manila.conf \
    --netapp-prom-host $PROMETHEUS_HOST \
    --prom-port $MANILA_NANNY_PROMETHEUS_PORT \
    --http-port $MANILA_NANNY_HTTP_PORT \
    --interval $MANILA_NANNY_INTERVAL \
    --task-share-size $TASK_SHARE_SIZE \
    --task-share-size-dry-run $TASK_SHARE_SIZE_DRY_RUN \
    --task-missing-volume $TASK_MISSING_VOLUME \
    --task-missing-volume-dry-run $TASK_MISSING_VOLUME_DRY_RUN \
    --task-offline-volume $TASK_OFFLINE_VOLUME \
    --task-offline-volume-dry-run $TASK_OFFLINE_VOLUME_DRY_RUN \
    --task-orphan-volume $TASK_ORPHAN_VOLUME \
    --task-orphan-volume-dry-run $TASK_ORPHAN_VOLUME_DRY_RUN \
    --task-share-state $TASK_SHARE_STATE \
    --task-share-state-dry-run $TASK_SHARE_STATE_DRY_RUN \

# start a test command
# /var/lib/openstack/bin/python /scripts/manila-share-sync.py --netapp-prom-host http://prometheus-infra-collector.infra-monitoring.svc:9090 --prom-port 9602 --http-port 9003 --task-share-size true  --task-share-size-dry-run true
