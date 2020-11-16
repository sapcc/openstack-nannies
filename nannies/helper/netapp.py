#
# Copyright (c) 2020 SAP SE
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

from netapp_lib.api.zapi.zapi import NaServer, invoke_api
import xmltodict

import ssl
import urllib
import logging

log = logging.getLogger(__name__)

class NetAppHelper:
    api_major = 1
    api_minor = 130

    def __init__(self, host, user, password, use_ssl=True, verify_ssl=False):
        self.host = host
        self.user = user
        self.password = password

        #ssl._create_default_https_context = ssl._create_unverified_context
        self._monkeypatch_netapp_lib()

        self.server = NaServer(self.host, username=self.user, password=self.password, transport_type=NaServer.TRANSPORT_TYPE_HTTPS)
        self.server.set_api_version(major=self.api_major, minor=self.api_minor)
        self.server._verify_ssl = verify_ssl

        if use_ssl:
            self.server.set_transport_type(NaServer.TRANSPORT_TYPE_HTTPS)

    @staticmethod
    def _monkeypatch_netapp_lib():
        # add support for ssl with unverified context
        if not hasattr(NaServer, '_orig_build_opener'):
            def _build_opener(self):
                self._orig_build_opener()
                if not getattr(self, '_verify_ssl'):
                    for handler in self._opener.handlers:
                        # give the HTTPSHandler for this an unverified context
                        if isinstance(handler, urllib.request.HTTPSHandler):
                            handler._context = ssl._create_unverified_context()

            NaServer._orig_build_opener = NaServer._build_opener
            NaServer._build_opener = _build_opener

    def invoke_api(self, *args, **kwargs):
        return invoke_api(self.server, *args, **kwargs)

    def invoke_api_single(self, *args, **kwargs):
        result = list(self.invoke_api(*args, **kwargs))
        if len(result) > 1:
            raise ValueError("Expected exactly one result, got {}".format(len(result)))
        if result:
            return result[0]
        else:
            return None

    def get_single(self, *args, **kwargs):
        # example: na.get_single("system-get-vendor-info")
        result = self.invoke_api_single(*args, **kwargs)
        result = xmltodict.parse(result.to_string())

        return result['results']

    def get_list(self, *args, unpack=True, **kwargs):
        # example: get_list("lun-get-iter", des_result={"lun-info": ["volume"]})
        kwargs['is_iter'] = True

        result = []
        result_iter = self.invoke_api(*args, **kwargs)

        key = None
        for item in result_iter:
            item = xmltodict.parse(item.to_string())
            if int(item['results']['num-records']) == 0:
                continue
            if not key:
                keys = list(item['results']['attributes-list'].keys())
                if len(keys) > 1:
                    raise ValueError("Found more than one possible key for unpack: {}".format(keys))
                key = keys[0]
            result.extend(item['results']['attributes-list'][key])

        return result

    def get_aggregate_usage(self):
        # TODO maybe convert numbers to int or float here already
        desired_attrs = {
            'aggr-attributes': {
                'aggr-raid-attributes': ['is-root-aggregate'],
                'aggregate-name': True,
                'aggr-space-attributes': ['size-total', 'size-used', 'percent-used-capacity']
            }
        }
        return self.get_list('aggr-get-iter', des_result=desired_attrs)

    # netapp volumes = flexvol
    def get_volume_usage(self):
        # TODO maybe think about getting percent used too
        # TODO maybe convert numbers to int or float here already
        desired_attrs = {
            'volume-attributes': {
                'volume-id-attributes': ['name', 'containing-aggregate-name'],
                'volume-space-attributes': ['size-total', 'size-used']
            }
        }

        return self.get_list('volume-get-iter', des_result=desired_attrs)

    def get_luns_for_flexvol(self, flexvol_name):
        # get all luns
        # filtering for volume took ~1s per query, all luns took 6s in qa
        #   --> we went for getting all luns and filtering on the client
        query = {''}
        desired_attrs = {'lun-info': ['volume', 'size-used', 'path', 'comment']}
        luns = self.get_list("lun-get-iter", des_result=desired_attrs)

        # find luns on flexvols
        lun_result = []
        # lun_result = [lun for lun in luns if lun['flexvol'] in flexvol_list]
        for lun in luns:
            try:
                if lun['volume'] == flexvol_name:
                    lun_result.append(lun)
            except TypeError:
                log.info("INFO: we seem to have gotten some garbage from the netapp api, but this should usually not be a problem")

        return lun_result

    def get_luns_for_aggr(self, aggr_name, vol_prefix):
        # find volumes on aggregate
        desired_attrs = {
            'volume-attributes': {
                'volume-id-attributes': ['name', 'containing-aggregate-name']
            }
        }
        query = {'volume-attributes': {'volume-id-attributes': {'containing-aggregate-name': aggr_name}}}
        flexvols = self.get_list('volume-get-iter', des_result=desired_attrs, query=query)
        flexvol_list = [v['volume-id-attributes']['name'] for v in flexvols
                       if v['volume-id-attributes']['name'].lower().startswith(vol_prefix)]

        # get all luns
        # filtering for flexvol took ~1s per query, all luns took 6s in qa
        #   --> we went for getting all luns and filtering on the client
        query = {''}
        desired_attrs = {'lun-info': ['volume', 'size-used', 'path', 'comment']}
        luns = self.get_list("lun-get-iter", des_result=desired_attrs)

        # find luns on flexvols
        lun_result = []
        # lun_result = [lun for lun in luns if lun['volume'] in flexvol_list]
        for lun in luns:
            # there was the case that lun somehow was not a dict, so better check for that
            if isinstance(lun, dict):
                if lun['volume'] in flexvol_list:
                    lun_result.append(lun)
            else:
                log.warning('WARNING: lun is not a dict - lun: %s', str(lun))

        return lun_result
