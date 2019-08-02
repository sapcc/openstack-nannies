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

#unset http_proxy https_proxy all_proxy no_proxy

#echo "INFO: copying manila config files to /etc/manila"
#cp -v /manila-etc/* /etc/manila

~/ccloud/cc-py27/.venv/bin/python  ./manila-share-size-sync.py \
    --config ~/ccloud/cc-py27/eu-de-1/manila.conf \
    --promhost https://prometheus.eu-de-1.cloud.sap
    # --promhost https://prometheus-infra.scaleout.qa-de-1.cloud.sap
    # --promhost https://prometheus-infra.scaleout.qa-de-1.cloud.sap \
    # --promquery 'netapp_capacity_svm{metric="size_total", job="pods"} + ignoring(metric) netapp_capacity_svm{metric="size_reserved_by_snapshots"}'