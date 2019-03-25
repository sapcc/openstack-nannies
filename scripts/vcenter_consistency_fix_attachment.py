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
@click.option('--novaconfig', prompt='nova config file')
@click.option('--cinderconfig', prompt='cinder config file')
@click.option('--fix-uuid', prompt='the uuid of the volume to fix the attachment for')
def get_args_and_run(vchost, vcusername, vcpassword, novaconfig, cinderconfig, fix_uuid):
    # the "None" below is for the prometheus_port we are not using here
    c = vcenter_consistency_module.ConsistencyCheck(vchost, vcusername, vcpassword, novaconfig, cinderconfig, False, None, None, True)

    log.info("- INFO - connecting to the cinder db")
    c.cinder_db_connect()
    if not c.cinder_db_connection_ok():
        log.error("problems connecting to the cinder db")
        sys.exit(1)
    log.info("- INFO - connecting to the nova db")
    c.nova_db_connect()
    if not c.nova_db_connection_ok():
        log.error("problems connecting to the nova db")
        sys.exit(1)
    attachment_info = dict()
    attachment_info = c.nova_db_get_attachment_info(fix_uuid)
    log.info("- INFO - inserting a volume_attachment entry for volume %s into the cinder db based on the nova block_device_mapping values", fix_uuid)
    c.cinder_db_insert_volume_attachment(fix_uuid, attachment_info)
    log.info("- INFO - disconnecting from the cinder db")
    c.cinder_db_disconnect()
    log.info("- INFO - disconnecting from the nova db")
    c.nova_db_disconnect()
    
if __name__ == '__main__':

    try:
        get_args_and_run()
    except Exception as e:
        log.error("get_args_and_run() error: %s", e)
        raise