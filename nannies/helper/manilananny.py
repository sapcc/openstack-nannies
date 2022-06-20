#!/usr/bin/env python
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

import argparse
import configparser
import os
import sys
import time

from keystoneauth1 import session
from keystoneauth1.identity import v3
from manilaclient import client as manilaclient

from .netapp import NetAppHelper
from .netapp_rest import NetAppRestHelper
from .prometheus_exporter import prometheus_http_start


class Nanny:
    """Nanny Base Class

    Nanny initiates a prometheus exporter and starts a loop in run(). Implement
    the business logic in _run() in subclass.
    """

    def __init__(self, config_file, interval, prom_port, dry_run):
        self.config_file = config_file
        self.interval = interval
        self.dry_run = dry_run

        # start prometheus exporter server
        prometheus_http_start(prom_port)

    def _run(self):
        raise Exception("not implemented")

    def run(self):
        while True:
            self._run()
            time.sleep(self.interval)


class ManilaNanny(Nanny):

    def __init__(self, config_file, interval, prom_port=9500, dry_run=False):
        super(ManilaNanny, self).__init__(config_file, interval, prom_port, dry_run)

    def get_manilaclient(self, version="2.7"):
        """Parse manila config file and create manila client"""
        try:
            parser = configparser.ConfigParser()
            parser.read(self.config_file)
            auth_url = parser.get("keystone_authtoken", "www_authenticate_uri")
            username = parser.get("keystone_authtoken", "username")
            password = parser.get("keystone_authtoken", "password")
            user_domain = parser.get("keystone_authtoken", "user_domain_name")
            prj_domain = parser.get("keystone_authtoken", "project_domain_name")
            prj_name = parser.get("keystone_authtoken", "project_name")
        except Exception as e:
            print(f"ERROR: Parse {self.config_file}: " + str(e))
            sys.exit(2)

        auth = v3.Password(
            username=username,
            password=password,
            user_domain_name=user_domain,
            project_domain_name=prj_domain,
            project_name=prj_name,
            auth_url=auth_url,
        )
        sess = session.Session(auth=auth)
        manila = manilaclient.Client(version, session=sess)
        return manila

    def get_netappclient(self, host):
        # all manila netapp filers get same user and password for api access
        username = os.getenv("MANILA_NANNY_NETAPP_API_USERNAME")
        password = os.getenv("MANILA_NANNY_NETAPP_API_PASSWORD")
        return NetAppHelper(host, username, password)

    def get_netapprestclient(self, host):
        username = os.getenv("MANILA_NANNY_NETAPP_API_USERNAME")
        password = os.getenv("MANILA_NANNY_NETAPP_API_PASSWORD")
        return NetAppRestHelper(host, username, password)


def base_command_parser():
    """Returns a basic command parser, including common args.

    The returned parser can be extended by its add_argument() method.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",
                        default="/manila-etc/manila.conf",
                        help="configuration file")
    parser.add_argument("--interval",
                        type=float,
                        default=600,
                        help="interval in seconds")
    parser.add_argument("--prom-port",
                        type=int,
                        default=9000,
                        help="prometheus exporter port")
    return parser
