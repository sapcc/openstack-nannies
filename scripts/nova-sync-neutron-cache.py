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
import datetime

from oslo_concurrency import lockutils

from nova import config
from nova.context import get_admin_context
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
        # for debugging
        # print instance
        try:
            cache_info = instance_info_cache_entries_q.execute().fetchone()[0]
        except TypeError:
            log.error("- PLEASE CHECK MANUALLY - instance %s does not have an entry in the instance_info_caches table - ignoring it for now", instance)
            cache_info = None

        return cache_info

    # set the neutron instance info cache entry for one instance in the nova db
    def set_instance_info_cache_entry_for_instance(self, instance, new_cache_info):

        instance_info_cache_entries_t = Table('instance_info_caches', self.metadata, autoload=True)
        now = datetime.datetime.utcnow()
        update_instance_info_cache_entry_q = instance_info_cache_entries_t.update().where(instance_info_cache_entries_t.c.instance_uuid == instance).values(updated_at=now, network_info=new_cache_info)
        update_instance_info_cache_entry_q.execute()

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
            with lockutils.lock('refresh_cache-%s' % instance.uuid):
                network_info = NetworkInfo(self.neutron._get_instance_nw_info(self.context, instance, port_ids=port_ids, networks=networks))
        except exception.InstanceNotFound:
            log.error("- PLEASE CHECK MANUALLY - instance could not be found on neutron side: %s - ignoring this instance for now", instance.uuid)
            # return None for network_info, so that we can skip this instance in the compare function
            return None

        return network_info

    # compare the neutron an nova view of things and fix inconsistencies in case we are not in dry-run mode
    def compare_and_fix_instance_info_cache(self):

        neutron_seen_mac_addresses = dict()
        cache_seen_mac_addresses = dict()

        for instance in InstanceList.get_all(self.context):
            # get network info from neutron via nova-network api
            network_info_source = self.get_neutron_instance_info_for_instance(instance)
            # do not deal with the instance if we do not have infor for it in neutron
            if network_info_source:
                # dicts to collect mac addressed we have already seen
                network_info_entries = dict()
                cache_info_entries = dict()
                # convert network info to json string
                network_info = json.dumps(network_info_source)
                # ... and back to python internal, so that we have the same format as the cache entry from the db
                network_info_raw = json.loads(network_info)
                # get the cache entry from the nova db
                cache_entry = self.get_instance_info_cache_entry_for_instance(instance.uuid)
                if cache_entry is not None:
                    cache_info = str(cache_entry)
                    # convert from json to python internal
                    cache_info_raw = json.loads(cache_info)
                else:
                    # in case we did get None then there is no instance_info_caches entry exists,
                    # so do nothing and got the next instance, an error message has been printed
                    # in get_instance_info_cache_entry_for_instance already in this case
                    continue
                # print "network-info: " + " " + str(type(network_info_source)) + " " + str(network_info_source)
                # print "network-info-raw: " + " " + str(type(network_info_raw)) + " " + str(network_info_raw)
                # print "cache-info: " + " " + str(type(cache_info_raw)) + " " + str(cache_info_raw)
                # go through all the ports in neutron ...
                for i in network_info_raw:
                    # create a dict of the port entries by mac address
                    network_info_entries[i['address']] = i
                    # safety check: we should never see a mac address here twice
                    if not neutron_seen_mac_addresses.get(i['address']):
                        neutron_seen_mac_addresses[i['address']] = instance.uuid
                    else:
                        log.error("- PLEASE CHECK MANUALLY - mac address %s for instance %s already seen in neutron for instance %s", str(i['address']), instance.uuid, neutron_seen_mac_addresses[i['address']])
                # go through all the ports in the nova cache ...
                for i in cache_info_raw:
                    # create a dict of the port entries by mac address
                    cache_info_entries[i['address']] = i
                    # safety check: we should never see a mac address here twice
                    if not cache_seen_mac_addresses.get(i['address']):
                        cache_seen_mac_addresses[i['address']] = instance.uuid
                    else:
                        log.error("- PLEASE CHECK MANUALLY - mac address %s for instance %s already seen in nova cache for instance %s",str(i['address']), instance.uuid, neutron_seen_mac_addresses[i['address']])

                # now check, if we have all the ports from neutron in the nova cache
                for i in network_info_entries:
                    if cache_info_entries.get(i):
                        log.debug("- instance %s - mac address %s is already in the nova cache", instance.uuid, i)
                        # for debugging
                        # log.error("- instance %s - mac address %s is already in the nova cache", instance.uuid, i)
                    else:
                        if self.args.dry_run:
                            log.error("- PLEASE CHECK MANUALLY - instance %s - mac address %s is not yet in the nova cache and needs to be added", instance.uuid, i)
                        else:
                            # get the proper lock for modifying the nova cache
                            with lockutils.lock('refresh_cache-%s' % instance.uuid):
                                log.error("- action: instance %s - adding mac address %s to nova cache", instance.uuid, i)
                                entry_to_append = network_info_entries[i]

                                entry_to_append['preserve_on_delete'] = True
                                # get the current instance info entry, as we might have appened some entries already within this loop
                                fresh_cache_info_raw = json.loads(self.get_instance_info_cache_entry_for_instance(instance.uuid))
                                # if there is already a cache entry, append the missing one
                                if fresh_cache_info_raw:
                                    fresh_cache_info_raw.append(entry_to_append)
                                    new_cache_info_raw = list(fresh_cache_info_raw)
                                # otherwise create one based on the neutron one
                                else:
                                    new_cache_info_raw = [entry_to_append]
                                # for debugging
                                # print "append: " + str(entry_to_append)
                                # print "cache: " + str(cache_info_raw)
                                # print "new cache: " + str(new_cache_info_raw)
                                # print "new json: " + json.dumps(new_cache_info_raw)
                                self.set_instance_info_cache_entry_for_instance(instance.uuid, json.dumps(new_cache_info_raw))

                # safety check: check the other way around too - there should be nothing in the cache we do not have in neutron
                for i in cache_info_entries:
                    if not network_info_entries.get(i):
                        log.error("- PLEASE CHECK MANUALLY - instance %s - mac address %s is in the nova cache, but not in neutron", instance.uuid, i)

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
        self.compare_and_fix_instance_info_cache()

if __name__ == '__main__':

    try:
        args = parse_cmdline_args()
    except Exception as e:
        log.error("Check command line arguments (%s)", e.strerror)

    n = NovaInstanceInfoCacheSync(args)
