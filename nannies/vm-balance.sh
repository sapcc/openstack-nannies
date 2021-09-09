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

echo -n "INFO: vm balancing (dry-run only for now) - "
date
if [ "$VM_BALANCE_AUTO" = "True" ] || [ "$VM_BALANCE_AUTO" = "true" ]; then
    AUTOMATION="--automated"
else
    AUTOMATION=""
fi
if [ "$VM_BALANCE_RECOMMENDER_ENDPOINT" != "" ]; then
    RECOMMENDER_OPTIONS="--migration-recommender-endpoint $VM_BALANCE_RECOMMENDER_ENDPOINT"
    if [ "$VM_BALANCE_RECOMMENDER_MAX_RETRIES" != "" ]; then
        RECOMMENDER_OPTIONS="$RECOMMENDER_OPTIONS --migration-recommender-max-retries $VM_BALANCE_RECOMMENDER_MAX_RETRIES"
    fi
    if [ "$VM_BALANCE_RECOMMENDER_TIMEOUT" != "" ]; then
        RECOMMENDER_OPTIONS="$RECOMMENDER_OPTIONS --migration-recommender-timeout $VM_BALANCE_RECOMMENDER_TIMEOUT"
    fi
else
    RECOMMENDER_OPTIONS=""
fi

python3 /scripts/vm_load_balance.py $AUTOMATION --vc_host $VM_BALANCE_VCHOST --vc_user $VM_BALANCE_VCUSER --vc_password $VM_BALANCE_VCPASSWORD --region $REGION --username $OS_USERNAME --password $OS_PASSWORD --user_domain_name $OS_USER_DOMAIN_NAME --project_name $OS_PROJECT_NAME --project_domain_name $OS_PROJECT_DOMAIN_NAME --interval $VM_BALANCE_INTERVAL --denial_list $DENIAL_BB_LIST $RECOMMENDER_OPTIONS
