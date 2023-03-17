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
# this script cleans up load balancers which are in a pending state for too long in neutron

import sys
import argparse
import ConfigParser
import json
import logging
import datetime
import time

from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy import or_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
# prometheus export functionality
from prometheus_client import start_http_server, Gauge

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')

# cmdline handling
def parse_cmdline_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",
                        default='./neutron.conf',
                        help='configuration file')
    parser.add_argument("--dry-run",
                        action="store_true",
                        help='print only what would be done without actually doing it')
    parser.add_argument("--interval",
                        default=1,
                        help="in which interval the check should run")
    parser.add_argument("--iterations",
                        default=3,
                        help="how many checks to wait before doing anything")
    parser.add_argument("--port",
                        default=9456,
                        help="port to use for prometheus exporter, otherwise we use 9456 as default")
    return parser.parse_args()

class NeutronLbaasCleanupPending:
    def __init__(self, args):

        # initialize variables
        self.args = args
        self.thisSession = None
        self.metadata = None
        self.Base = None
        self.db_url = None

        self.seen_dict = dict()
        self.to_be_dict = dict()

        self.gauge_value_pending_lb = 0

        self.gauge_pending_lb = Gauge('neutron_nanny_pending_lb', 'number of lbs in pending state for a longer time')

        # Start http server for exported data
        if int(args.port):
            prometheus_exporter_port = int(args.port)
        else:
            prometheus_exporter_port = 9456

        try:
            start_http_server(prometheus_exporter_port)
        except Exception as e:
            logging.error("failed to start prometheus exporter http server: " + str(e))

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

        parser = ConfigParser.ConfigParser()
        try:
            parser.read(self.args.config)
            self.db_url = parser.get('database', 'connection', raw=True)
        except:
            log.error("ERROR: Check Neutron configuration file.")
            sys.exit(2)

    # get the lbaas loadbalancers in any pending state
    def get_pending_lbaas_loadbalancers(self):

        pending_lbaas_loadbalancers = []

        lbaas_loadbalancers_t = Table('lbaas_loadbalancers', self.metadata, autoload=True)
        pending_lbaas_loadbalancers_q = select(columns=[lbaas_loadbalancers_t.c.id], whereclause=or_(lbaas_loadbalancers_t.c.provisioning_status == "PENDING_CREATE", lbaas_loadbalancers_t.c.provisioning_status == "PENDING_UPDATE", lbaas_loadbalancers_t.c.provisioning_status == "PENDING_DELETE"))

        # convert the query result into a list
        for i in pending_lbaas_loadbalancers_q.execute():
            pending_lbaas_loadbalancers.append(i[0])

        return pending_lbaas_loadbalancers

    # init dict of all vms or files we have seen already
    def init_seen_dict(self):
        for i in self.seen_dict:
            self.seen_dict[i] = 0

    # decide, if something should be done with a certain lbaas loadbalancer id now or later
    def now_or_later(self, lbaas_loadbalancer_id):
        self.seen_dict[lbaas_loadbalancer_id] = 1
        default = 0
        if self.to_be_dict.get(lbaas_loadbalancer_id, default) <= int(args.iterations):
            if self.to_be_dict.get(lbaas_loadbalancer_id, default) == int(args.iterations):
                if self.args.dry_run:
                    log.info("- PLEASE CHECK MANUALLY - dry-run: setting the provisioning_status for loadbalancer %s from PENDING_* to ERROR", lbaas_loadbalancer_id)
                else:
                    log.info("- PLEASE CHECK MANUALLY - action: setting the provisioning_status for loadbalancer %s from PENDING_* to ERROR", lbaas_loadbalancer_id)
                    # do something here ... we will implement this part if we see this case more often
                # increase the metric counting the lbs in pending state for a longer time
                self.gauge_value_pending_lb += 1
            else:
                # avoid logging it if we have it the first time on out list to reduce log spam
                if self.to_be_dict.get(lbaas_loadbalancer_id, default) > 0:
                    log.info("- PLEASE CHECK MANUALLY - plan: to set the provisioning_status for loadbalancer %s from PENDING_* to ERROR (%s/%s)", lbaas_loadbalancer_id, str(self.to_be_dict.get(lbaas_loadbalancer_id, default) + 1), str(self.args.iterations))
            self.to_be_dict[lbaas_loadbalancer_id] = self.to_be_dict.get(lbaas_loadbalancer_id, default) + 1
        else:
            log.debug("dry-run: ignoring this one - it should only happen in dry-run mode")

    # reset dict of all lbaas loadbalancer id's we plan to do something with
    def reset_to_be_dict(self):
        for i in self.seen_dict:
            # if a machine we planned to delete no longer appears as candidate for delettion, remove it from the list
            if self.seen_dict[i] == 0:
                self.to_be_dict[i] = 0

    def wait_a_moment(self):
        # wait the interval time
        log.info("waiting %s minutes before starting the next loop run", str(self.args.interval))
        time.sleep(60 * int(self.args.interval))

    def send_to_prometheus_exporter(self):
        self.gauge_pending_lb.set(float(self.gauge_value_pending_lb))

    def run_me(self):
        # connect to the DB
        self.get_db_url()
        self.makeConnection()

        while True:
            self.init_seen_dict()
            self.gauge_value_pending_lb = 0
            for i in self.get_pending_lbaas_loadbalancers():
                self.now_or_later(i)
            self.send_to_prometheus_exporter()
            self.reset_to_be_dict()
            self.wait_a_moment()

if __name__ == '__main__':

    # parse command line arguments
    args = parse_cmdline_args()

    c = NeutronLbaasCleanupPending(args)
