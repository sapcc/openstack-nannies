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

echo "INFO: copying manila config files to /etc/manila"
cp -v /manila-etc/* /etc/manila

# we run an endless loop to run the script periodically
echo "INFO: starting a loop to periodically run the nanny job for the manila netapp re-export"
while true; do
    date
    /var/lib/kolla/venv/bin/manila-manage --config-file /etc/manila/backend.conf --config-file /etc/manila/manila.conf shell script --path /scripts/netapp-manila-reexport.py
    echo -n "INFO: waiting $MANILA_NETAPP_NANNY_INTERVAL minutes before starting the next loop run - "
    date
    sleep $(( 60 * $MANILA_NETAPP_NANNY_INTERVAL ))
done
