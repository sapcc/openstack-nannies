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

from openstack import connection, exceptions
import logging
import re

#openstack.enable_logging(debug=False)
log = logging.getLogger('openstack_helper')
class OpenstackHelper:
    def __init__(self, region, user_domain_name, project_domain_name, project_name, username, password, autoconnect=True):
        self.region = region
        self.user_domain_name = user_domain_name
        self.project_domain_name = project_domain_name
        self.project_name = project_name
        self.api = None
        self.username = username
        self.password = password

        self.monkeypatch_openstack()
        self.monkeypatch_keystoneauth1()

        if autoconnect:
            self.connect()

    @staticmethod
    def monkeypatch_openstack():
        """Apply some fixes and backports to the openstacksdk
        Can be called multiple times
        """
        from openstack.compute.v2.server import Server as OpenStackServer
        from openstack import resource

        # add the compute_host attribute to find out in which building block a server runs
        # at the time of this commit this is already in place in the openstacksdk master, but
        # not in any release
        if not hasattr(OpenStackServer, 'compute_host'):
            OpenStackServer.compute_host = resource.Body('OS-EXT-SRV-ATTR:host')

    @staticmethod
    def monkeypatch_keystoneauth1():
        """Apply fixes to keystoneauth1
        Can be called multiple times
        """
        import keystoneauth1.discover
        # patch get_version_data to include the expected values if the endpoint
        # returns only one version like nova's placement-api in queens
        if not getattr(keystoneauth1.discover.get_version_data, 'is_patched', False):
            old_get_version_data = keystoneauth1.discover.get_version_data
            def _get_version_data(session, url, **kwargs):
                data = old_get_version_data(session, url, **kwargs)
                for v in data:
                    if 'status' not in v and len(data) == 1:
                        v['status'] = 'current'
                    if 'links' not in v and len(data) == 1:
                        v['links'] = [{'href': url, 'rel': 'self'}]
                return data
            _get_version_data.is_patched = True
            keystoneauth1.discover.get_version_data = _get_version_data


    def connect(self, test=True):
        auth = dict(
                auth_url='https://identity-3.{}.cloud.sap/v3'.format(self.region),
                username=self.username,
                password=self.password,
                user_domain_name=self.user_domain_name,
                project_domain_name=self.project_domain_name,
                project_name=self.project_name,
        )
        try:
            self.api = connection.Connection(
                    region_name=self.region,
                    auth=auth)
        except Exception as e:
            log.warn("problems connecting to openstack: %s", str(e))

    def get_project_path(self, project_id):
        if not project_id:
            return ""
        project = self.api.identity.get_project(project_id)
        if project.is_domain:
            return project.name
        else:
            return "{}/{}".format(self.get_project_path(project.domain_id), project.name)

    def get_shard_vcenter_all(self, vc_host):
        filter_host = self.get_building_block_all()
        agg = self.api.compute.aggregates()
        hosts = []
        for vc in agg:
            if vc.name in vc_host:
                match = re.search(r"^vc-[a-z]-[0-9]$", vc.name)
                if match:
                    if vc.name not in vc_host:
                        continue
                    hosts = [str(host) for host in vc.hosts if str(host) in filter_host]
        return hosts

    def get_shard_vcenter(self, vc_host):
        filter_host = self.get_building_block_filter()
        agg = self.api.compute.aggregates()
        hosts = []
        for vc in agg:
            if vc.name in vc_host:
                match = re.search(r"^vc-[a-z]-[0-9]$", vc.name)
                if match:
                    if vc.name not in vc_host:
                        continue
                    hosts = [str(host) for host in vc.hosts if str(host) in filter_host]
        return hosts

    def get_avalibity_zone(self):
        pass

    def get_building_block_filter(self):
        services = self.api.compute.services()
        hosts = [str(item.host) for item in services if
                 str(item.host).startswith("nova-compute-bb") and str(item.status) == 'enabled' and str(item.state) == 'up']
        return hosts

    def get_building_block_all(self):
        services = self.api.compute.services()
        hosts = [str(item.host) for item in services if str(item.host).startswith("nova-compute-bb")]
        return hosts

    def get_all_servers(self):
        servers = list(self.api.compute.servers(details=True, all_projects=1))
        return servers

    #function will return list of server in building block #servers must be active
    #task_state?
    def get_all_servers_bb(self,nova_compute):
        servers = self.get_all_servers()
        server_list = { server.id:[server.name,server.flavor["ram"],server.task_state,server.attached_volumes] for server in servers if server.status == u'ACTIVE' and server.compute_host == nova_compute}
        return server_list

    def get_server_detail(self,id):
        return self.api.compute.find_server(id)

    def lock_volume(self, volume_uuid):
        try:
            vol = self.api.block_storage.get_volume(volume_uuid)
            if self.check_volume_metadata(volume_uuid, 'storage_balancing', "in_progress"):
                logging.warning("- PLEASE IGNORE - WARNING - lock_volume: volume {} already has storage_balancing property".format(volume_uuid))
                return False
            else:
                self.set_volume_metadata(volume_uuid, 'storage_balancing', 'in_progress')
                vol._action(self.api.block_storage, {'os-reset_status': {'status': 'maintenance'}})
                return True

        except Exception as e:
            logging.error("- ERROR - failed to lock volume {}".format(str(e)))
            return False

    def lock_volume_vc(self, volume_uuid, vc_host):
        try:
            vol = self.api.block_storage.get_volume(volume_uuid)
            if self.check_volume_metadata_key_exists(volume_uuid, 'storage_balancing'):
                logging.warning("- PLEASE IGNORE - WARNING - lock_volume: volume {} already has storage_balancing property".format(volume_uuid))
                return False
            else:
                self.set_volume_metadata(volume_uuid, 'storage_balancing', vc_host)
                vol._action(self.api.block_storage, {'os-reset_status': {'status': 'maintenance'}})
                return True

        except Exception as e:
            logging.error("- ERROR - failed to lock volume {}".format(str(e)))
            return False

    def unlock_volume(self, volume_uuid):
        try:
            vol = self.api.block_storage.get_volume(volume_uuid)
            if not self.check_volume_metadata(volume_uuid, 'storage_balancing', 'in_progress'):
                logging.warning("- PLEASE IGNORE - WARNING - unlock_volume: volume {} has no storage_balancing property".format(volume_uuid))
                return False
            else:
                status = 'available'
                if vol['attachments'] != []:
                    status = 'in-use'
                vol._action(self.api.block_storage, {'os-reset_status': {'status': status}})
                self.delete_volume_metadata(volume_uuid, 'storage_balancing')
                return True

        except Exception as e:
            logging.error("- ERROR - failed to unlock volume {}".format(str(e)))
            return False

    def unlock_volume_vc(self, volume_uuid, vc_host):
        try:
            vol = self.api.block_storage.get_volume(volume_uuid)
            if not self.check_volume_metadata_key_exists(volume_uuid, 'storage_balancing'):
                logging.warning("- PLEASE IGNORE - WARNING - unlock_volume: volume {} has no storage_balancing property".format(volume_uuid))
                return False
            else:
                status = 'available'
                if vol['attachments'] != []:
                    status = 'in-use'
                vol._action(self.api.block_storage, {'os-reset_status': {'status': status}})
                self.delete_volume_metadata(volume_uuid, 'storage_balancing')
                return True

        except Exception as e:
            logging.error("- ERROR - failed to unlock volume {}".format(str(e)))
            return False

    def set_volume_metadata(self, volume_uuid, key, value):
        # TODO: excpetion handling
        result = self.api.block_storage.post('/volumes/' + volume_uuid + '/metadata', json={ 'metadata': { key: value}})
        if result.status_code == 200:
            return result
        else:
            return False

    def check_volume_metadata(self, volume_uuid, key, value):
        # TODO: excpetion handling
        result = self.api.block_storage.get_volume(volume_uuid).metadata
        if result.get(key) == value:
            return True
        else:
            return False

    def check_volume_metadata_key_exists(self, volume_uuid, key):
        # TODO: excpetion handling
        result = self.api.block_storage.get_volume(volume_uuid).metadata
        if result.get(key, None):
            return True
        else:
            return False

    def delete_volume_metadata(self, volume_uuid, key):
        # TODO: excpetion handling
        result = self.api.block_storage.delete('/volumes/' + volume_uuid + '/metadata/' + key)
        if result.status_code == 200:
            return result
        else:
            return False

    def set_nanny_metadata(self):
        pass

    def delete_nanny_metadata(self,nanny_metadata,avail_zone,shard_vcenter_all):
        for server in self.api.compute.servers(details=True, all_projects=True,availability_zone=avail_zone):
            if server.metadata.get("nanny_metadata") == nanny_metadata:
                if server.compute_host in shard_vcenter_all:
                    if server['is_locked']:
                        self.api.compute.unlock_server(server.id)
                        log.info(f"Server name : {server.name} and UUID {server.id} has orphane nanny server lock cleaning now")
                    self.api.compute.delete_server_metadata(server.id, ["nanny_metadata"])