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
# this script syncs the nova instance_info_cache table with the info in neutron

import sys
import argparse
import ConfigParser
import json
import logging

from nova import config
from nova.context import get_admin_context
from nova.network.base_api import update_instance_cache_with_nw_info
from nova.network.model import NetworkInfo
from nova.network.neutronv2 import api as neutronapi
from nova.objects.instance import InstanceList
from nova import objects
from nova import exception

from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.ERROR, format='%(asctime)-15s %(message)s')

# cmdline handling
def parse_cmdline_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",
                        default='./nova.conf',
                        help='configuration file')
    parser.add_argument("--dry-run",
                        action="store_true",
                        help='print only what would be done without actually doing it')
    return parser.parse_args()

class NovaInstanceInfoCacheSync:
    def __init__(self, args):

        # initialize variables
        self.instance_info_cache_entries = {}
        self.args = args
        self.thisSession = None
        self.metadata = None
        self.Base = None
        self.db_url = None
        self.context = None
        self.neutron = None
        self.client = None

        self.run_me()

    # establish a database connection and return the handle
    def makeConnection(self):

        engine = create_engine(self.db_url)
        engine.connect()
        Session = sessionmaker(bind=engine)
        self.thisSession = Session()
        self.metadata = MetaData()
        self.metadata.bind = engine
        self.Base = declarative_base()

    # return the database connection string from the config file
    def get_db_url(self):

        parser = ConfigParser.SafeConfigParser()
        try:
            parser.read(self.args.config)
            self.db_url = parser.get('database', 'connection', raw=True)
        except:
            log.error("ERROR: Check Nova configuration file.")
            sys.exit(2)

    # get the neutron instance info cache entry for one instance from the nova db
    def get_instance_info_cache_entry_for_instance(self, instance):

        instance_info_cache_entries_t = Table('instance_info_caches', self.metadata, autoload=True)
        instance_info_cache_entries_q = select(columns=[instance_info_cache_entries_t.c.network_info], whereclause=and_(instance_info_cache_entries_t.c.deleted == 0, instance_info_cache_entries_t.c.instance_uuid == instance))
        cache_info = instance_info_cache_entries_q.execute().fetchone()[0]

        return cache_info

    # build up a connection to neutron
    def buildup_nova_neutron_connection(self):

        self.context = get_admin_context()
        self.neutron = neutronapi.API()
        self.client = neutronapi.get_client(self.context)

    # get the networkinfo from nova for one instance
    def get_neutron_instance_info_for_instance(self, instance):

        try:
            ports = self.neutron.list_ports(self.context, device_id=instance.uuid)["ports"]
            networks = [self.client.show_network(network_uuid).get('network') for network_uuid in set([port["network_id"] for port in ports])]
            port_ids = [port["id"] for port in ports]
            network_info = NetworkInfo(self.neutron._get_instance_nw_info(self.context, instance, port_ids=port_ids, networks=networks))
        except exception.InstanceNotFound:
            log.error("- PLEASE CHECK MANUALLY - instance could not be found: %s - continuing at next loop run", instance.uuid)
            # return None for network_info, so that we can skip this instance in the compare function
            return None

        return network_info

    # compare the neutron an nova view of things
    def compare_instance_info_cache(self):
        for instance in InstanceList.get_all(self.context):
            network_info_raw = self.get_neutron_instance_info_for_instance(instance)
            # do not try to compare if we do not have info from neutron
            if network_info_raw:
                network_info = json.dumps(network_info_raw)
                cache_info = str(self.get_instance_info_cache_entry_for_instance(instance.uuid))
                log.debug("- neutron: %s", network_info)
                log.debug("- nova:    %s", cache_info)

                if cache_info == network_info:
                    log.debug("- nova instance info cache for instance %s is in sync", str(instance.uuid))
                else:
                    log.error("- nova instance info cache for instance %s is out of sync", str(instance.uuid))

    # compare the neutron an nova view of things and fix it by setting the nove cache to the neutron values for one instance
    def fix_instance_info_cache_for_instance(self, instance):
        network_info_raw = self.get_neutron_instance_info_for_instance(instance)
        # do not try to fix if we do not have info from neutron
        if network_info_raw:
            network_info = json.dumps(network_info_raw)
            cache_info = str(self.get_instance_info_cache_entry_for_instance(instance.uuid))
            log.debug("neutron: %s", network_info)
            log.debug("nova:    %s", cache_info)
            if cache_info != network_info:
                log.error("- fixing nova instance info cache for instance: %s", instance.uuid)
                update_instance_cache_with_nw_info(None, self.context, instance, nw_info=network_info_raw)

    # compare the neutron an nova view of things and fix it by setting the nove cache to the neutron values for all instances
    def fix_instance_info_cache(self):
        for instance in InstanceList.get_all(self.context):
            self.fix_instance_info_cache_for_instance(instance)

    def run_me(self):

        # this is to make parse_args happy and still be able to use sys.argv later for argparse
        dummy_argv = []
        dummy_argv.append(sys.argv[0])
        config.parse_args(dummy_argv)

        # looks like this is needed
        objects.register_all()

        self.get_db_url()
        self.makeConnection()

        self.buildup_nova_neutron_connection()
        if self.args.dry_run:
            self.compare_instance_info_cache()
        else:
            self.fix_instance_info_cache()

if __name__ == '__main__':

    try:
        args = parse_cmdline_args()
    except Exception as e:
        log.error("Check command line arguments (%s)", e.strerror)

    n = NovaInstanceInfoCacheSync(args)
