#!/usr/bin/env python
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

import vcenter_consistency_module
import logging
import click

log = logging.getLogger('vcenter_consistency_module')
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')

# cmdline handling
@click.command()
# vcenter host, user and password
@click.option('--host', prompt='Host to connect to')
@click.option('--username', prompt='Username to connect with')
@click.option('--password', prompt='Password to connect with')
# dry run option not doing anything harmful
@click.option('--dry-run', is_flag=True)
def get_args_and_run(host, username, password, dry_run):
    print host
    c = vcenter_consistency_module.ConsistencyCheck(host, username, password, dry_run)
    c.run_check()
    
if __name__ == '__main__':

    try:
        get_args_and_run()
    except Exception as e:
        log.error("get_args_and_run() error: %s", e)