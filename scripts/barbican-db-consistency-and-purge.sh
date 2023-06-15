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

echo "INFO: copying barbican config files to /etc/barbican"
cp -v /barbican-etc/* /etc/barbican

# barbican is now using proxysql by default in its config - change that back to a normal
# config for the nanny as we do not need it and do not have the proxy around by default
sed -i 's,@/barbican?unix_socket=/run/proxysql/mysql.sock&,@barbican-mariadb/barbican-api?,g' /etc/barbican/barbican-api.conf

# we run an endless loop to run the script periodically
echo "INFO: starting a loop to periodically run the nanny job for the barbican db consistency check and purge"
while true; do

    # there is no consistency check for barbican yet

    if [ "$BARBICAN_DB_PURGE_ENABLED" = "True" ] || [ "$BARBICAN_DB_PURGE_ENABLED" = "true" ]; then
        echo -n "INFO: purging barbican entities older than $BARBICAN_DB_PURGE_OLDER_THAN days from the barbican db - "
        date
        /var/lib/openstack/bin/barbican-manage db clean --min-days $BARBICAN_DB_PURGE_OLDER_THAN
    fi
    echo -n "INFO: waiting $BARBICAN_NANNY_INTERVAL minutes before starting the next loop run - "
    date
    sleep $(( 60 * $BARBICAN_NANNY_INTERVAL ))
done
