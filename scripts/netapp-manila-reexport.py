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
# this script re-exports share instances from manila to netapp

from oslo_config import cfg
from manila import context
from manila import service

CONF = cfg.CONF
service_name = 'netapp-multi'
# host needs to be set in config
host = "%s@%s" % (CONF.host, service_name)

backend = service.Service.create(binary='manila-share', service_name=service_name, host=host)
ctxt = context.get_admin_context()
backend.driver.do_setup(ctxt)
# in contrast to manila-share service enable re-export
backend.init_host(reexport=True)
