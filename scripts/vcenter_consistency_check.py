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
@click.option('--vchost', prompt='vc host to connect to')
@click.option('--vcusername', prompt='vc username to connect with')
@click.option('--vcpassword', prompt='vc password to connect with')
@click.option('--interval', prompt='how long to wait between loop runs')
@click.option('--iterations', prompt='how often a problem can occur before it is creating a warniing')
# dry run option not doing anything harmful
@click.option('--dry-run', is_flag=True)
# port to use for prometheus exporter, otherwise we use 9456 as default
@click.option('--prometheus-port')
def get_args_and_run(vchost, vcusername, vcpassword, dry_run, interval, iterations, prometheus_port):
    # check if the prometheus port is set and if not set it to the default value
    if not prometheus_port:
        prometheus_port = 9456
    c = vcenter_consistency_module.ConsistencyCheck(vchost, vcusername, vcpassword, dry_run, int(prometheus_port))
    c.run_check(interval, iterations)
    
if __name__ == '__main__':

    try:
        get_args_and_run()
    except Exception as e:
        log.error("get_args_and_run() error: %s", e)