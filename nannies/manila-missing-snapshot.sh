#!/bin/bash
#
# Copyright (c) 2022 SAP SE
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

echo "INFO: exporting missing snapshots"
/var/lib/openstack/bin/python /scripts/manila-missing-snapshot.py

# start a test command
# /var/lib/openstack/bin/python /scripts/manila-missing-snapshot.py --prom-port 9605
