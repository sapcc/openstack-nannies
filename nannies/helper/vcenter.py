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

import http
import re
import requests

from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim, vmodl
from pyVim.task import WaitForTask
import ssl
import logging
import re
log = logging.getLogger(__name__)

from .exceptions import VCenterObjectNotFound, PyCCloudNotFound


def _get_if_not_instance(obj, objtype, func):
    if isinstance(obj, objtype):
        return obj
    else:
        return func(obj)


class VCenterHelper:
    # import helper for users of this class
    vim = vim

    def __init__(self, host, user, password, verify_ssl=False):
        self.api = None
        self.host = host
        self.user = user
        self.password = password
        self.sslContext = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        #self.sslContext.verify_mode = ssl.CERT_NONE

        #self._connect_class = SmartConnect
        """
        if verify_ssl:
            self._connect_class = pyVim.connect.SmartConnect
        else:
            # NoSSL means no certificate verification in this context
            self._connect_class = pyVim.connect.SmartConnectNoSSL
        """
        self.openstack_re = re.compile('^name')
        self.shadow_vm_uuid_re = re.compile('^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
        self.snapshot_shadow_vm_uuid_re = re.compile('^snapshot-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)

        self.login()

    def get_name(self):
        return self.host.split(".")[0]

    def is_alive(self):
        try:
            self.api.CurrentTime()
            return True
        except (vim.fault.NotAuthenticated, http.client.RemoteDisconnected):
            return False

    def login(self):
        self.sslContext.verify_mode = ssl.CERT_NONE
        self.api = SmartConnect(port=443,
                                host=self.host,
                                user=self.user,
                                pwd=self.password,
                                sslContext=self.sslContext)

    def disconnect(self):
        if self.api:
            Disconnect(self.api)

    def find_server(self, uuid):
        return self.api.content.searchIndex.FindByUuid(None, uuid, True, True)

    def find_all_of_type(self, obj):
        """Find all objects of a type. Creates a ContainerView
        Creates a ContainerView, result can be found in view.
        :param obj: a vim class, e.g. pyVmomi.vim.DistributedVirtualPortgroup
        """

        if not isinstance(obj, list):
            obj = [obj]
        return self.api.content.viewManager.CreateContainerView(self.api.content.rootFolder,
                                                                obj,
                                                                True)

    def get_object_by_name(self, vimtype, name):
        """Really, really slow way of finding an object of a type"""
        for obj in self.find_all_of_type(vimtype).view:
            if obj.name == name:
                return obj

        raise VCenterObjectNotFound(name, vimtype)

    @staticmethod
    def get_nics_of_server(server):
        """Returns all nics of a server objects"""
        return [dev for dev in server.config.hardware.device if hasattr(dev, 'macAddress')]

    # Shamelessly borrowed from:
    # https://github.com/dnaeon/py-vconnector/blob/master/src/vconnector/core.py
    def collect_properties(self, view_ref, obj_type, path_set=None,
                           include_mors=False):
        """
        Collect properties for managed objects from a view ref
        Check the vSphere API documentation for example on retrieving
        object properties:
            - http://goo.gl/erbFDz
        Args:
            view_ref (pyVmomi.vim.view.*): Starting point of inventory navigation
            obj_type      (pyVmomi.vim.*): Type of managed object
            path_set               (list): List of properties to retrieve
            include_mors           (bool): If True include the managed objects
                                           refs in the result
        Returns:
            A list of properties for the managed objects
        """
        collector = self.api.content.propertyCollector

        # Create object specification to define the starting point of
        # inventory navigation
        obj_spec = vmodl.query.PropertyCollector.ObjectSpec()
        obj_spec.obj = view_ref
        obj_spec.skip = True

        # Create a traversal specification to identify the path for collection
        traversal_spec = vmodl.query.PropertyCollector.TraversalSpec()
        traversal_spec.name = 'traverseEntities'
        traversal_spec.path = 'view'
        traversal_spec.skip = False
        traversal_spec.type = view_ref.__class__
        obj_spec.selectSet = [traversal_spec]

        # Identify the properties to the retrieved
        property_spec = vmodl.query.PropertyCollector.PropertySpec()
        property_spec.type = obj_type

        if not path_set:
            property_spec.all = True

        property_spec.pathSet = path_set

        # Add the object and property specification to the
        # property filter specification
        filter_spec = vmodl.query.PropertyCollector.FilterSpec()
        filter_spec.objectSet = [obj_spec]
        filter_spec.propSet = [property_spec]

        # Retrieve properties
        props = collector.RetrieveContents([filter_spec])

        data = []
        for obj in props:
            properties = {}
            for prop in obj.propSet:
                properties[prop.name] = prop.val

            if include_mors:
                properties['obj'] = obj.obj

            data.append(properties)
        return data

    def get_obj(self, ref, obj_type=None):
        """Return the ManagedObject for the ref/moid.
        It will automatically detect the object type, if it can, but you can
        still provide one explicitly if necessary.
        """
        ref_type = None
        if ':' in ref:
            # e.g. vim.HostSystem:host-41960
            ref_type, ref = ref.split(':')

        if obj_type is None:
            if ref_type:
                # e.g. vim.HostSystem
                obj_type = getattr(vim, ref_type.split('.')[-1])
            else:
                try:
                    # e.g. host-41960
                    obj_type = {
                        'datacenter': vim.Datacenter,
                        'datastore': vim.Datastore,
                        'domain': vim.ClusterComputeResource,
                        'host': vim.HostSystem,
                        'vm': vim.VirtualMachine,
                    }[ref.split('-')[0]]
                except KeyError:
                    raise ValueError('Cannot detect object type from ref. Please supply the "obj_type" parameter.')

        prop_spec = vmodl.query.PropertyCollector.PropertySpec(
            type=obj_type,
            pathSet=['name'])
        obj_spec = vmodl.query.PropertyCollector.ObjectSpec(
            obj=obj_type(ref),
            skip=False,
            selectSet=[])
        filter_spec = vmodl.query.PropertyCollector.FilterSpec(
            objectSet=[obj_spec],
            propSet=[prop_spec])

        props = self.api.content.propertyCollector.RetrieveContents([filter_spec])
        data = []
        try:
            for obj in props:
                data.append(obj.obj)
        except vmodl.fault.ManagedObjectNotFound:
            raise VCenterObjectNotFound(ref, obj_type)
        if len(data) > 1:
            msg = 'Multiple objects found for ({}, {}): {}'
            raise PyCCloudNotFound(msg.format(ref, obj_type, data))
        elif not data:
            raise VCenterObjectNotFound(ref, obj_type)
        return data[0]

    def dvs_uuid(self, bb=None):
        """Return a dict(bb, uuid) of all/ a string of one BB-DVS uuid(s)."""
        result = {}

        v = self.find_all_of_type(self.vim.DistributedVirtualSwitch)
        for d in self.collect_properties(v, self.vim.DistributedVirtualSwitch, ['name', 'config.uuid']):
            try:
                dvs_bb_name = d['name'].split('-')[1]
                dvs_uuid = d['config.uuid'].replace(' ', '')
                if bb is not None and dvs_bb_name == bb:
                    return dvs_uuid
                else:
                    result[dvs_bb_name] = dvs_uuid
            except IndexError:
                pass

        if bb is not None:
            raise RuntimeError('No such BB or directory.')

        return result

    def get_dvs(self, switch_uuid):
        """Get a DVS for a switchUuid, e.g. '50 30 1c 5e ...'"""
        return self.api.content.dvSwitchManager.QueryDvsByUuid(switch_uuid)

    def get_portgroup(self, switch_uuid, portgroupKey):
        """Get a portgroup from a switchUuid and a portgroupKey"""
        dvs = _get_if_not_instance(switch_uuid, vim.VmwareDistributedVirtualSwitch, self.get_dvs)
        return dvs.LookupDvPortGroup(portgroupKey)

    def get_port(self, switch_uuid, portKey, portgroupKey=None):
        """Get a (dvs) port by dvs id and portKey
        portgroupKey can be specified to potentially fasten up the search
        """
        dvs = _get_if_not_instance(switch_uuid, vim.VmwareDistributedVirtualSwitch, self.get_dvs)

        pc = vim.dvs.PortCriteria()
        if portgroupKey:
            pc.portgroupKey.append(portgroupKey)
        pc.portKey.append(portKey)
        pc.inside = True

        ports = dvs.FetchDVPorts(pc)

        if len(ports) > 1:
            raise ValueError('get_port() returned more than one port! Found {}'.format(len(ports)))

        if len(ports) == 1:
            return ports[0]
        else:
            return None

    def get_port_for_nic(self, nic):
        """Get the port for a server nic"""
        return self.get_port(nic.backing.port.switchUuid, nic.backing.port.portKey, nic.backing.port.portgroupKey)

    def get_ports_from_server(self, server):
        """Get all (dvs) ports for a server
        server: can be either a vim.VirtualMachine or a server uuid
        """
        server = _get_if_not_instance(server, vim.VirtualMachine, self.find_server)
        ports = [self.get_port_for_nic(nic) for nic in self.get_nics_of_server(server)]

        return ports

    def get_hosts(self):
        return list(self.find_all_of_type(vim.HostSystem).view)

    def get_clusters(self):
        return list(self.find_all_of_type(vim.ClusterComputeResource).view)

    def get_vm(self):
        return list(self.find_all_of_type(vim.VirtualMachine).view)

    def get_available_host_bb(self,building_block):

        pass

    #missing getattr check, if failover not present in production cluster.
    def get_failover_host(self,cluster_view,failover_host=0):
        #by default it will return first set of failover_host
        failoverhosts = []
        clusters = self.collect_properties(cluster_view,vim.ClusterComputeResource,
                            ['name', 'configuration.dasConfig.admissionControlPolicy'], include_mors=True)
        for cluster in clusters:
            if cluster['name'].startswith("production"):
                try:
                    failoverhosts.append(cluster['configuration.dasConfig.admissionControlPolicy'].failoverHosts[failover_host])
                except AttributeError as error:
                    log.info("- INFO - No failoverhosts policy defined with error %s",error)
                except IndexError as error:
                    log.info("- INFO - second failoverhosts not defind for BB %s",cluster['name'])
        #failoverhosts = [cluster['configuration.dasConfig.admissionControlPolicy'].failoverHosts[0] for cluster in clusters if cluster['name'].startswith("production")]
        return failoverhosts

    #list all the host where size of big_VM present consume whole host memory
    def get_source_host(self):
        pass

    def get_host_detail(self,host_view):
        hosts = self.collect_properties(host_view,vim.HostSystem,['name','config','hardware.memorySize','runtime'],include_mors=True)
        for host in hosts:
            print(host['name'])

    def get_production_node(self, cluster_view):
        host = []
        clusters = self.collect_properties(cluster_view, vim.ClusterComputeResource,
                                           ['name'],
                                           include_mors=True)
    #free host we have to
    def get_destination_host(self):
        pass

    def get_big_vm_host(self,cluster_view):
        host = []
        clusters = self.collect_properties(cluster_view, vim.ClusterComputeResource,
                                         ['name', 'configurationEx'],
                                         include_mors=True)

        group_ret = [getattr(cluster['configurationEx'], 'group', None) for cluster in
                     clusters if cluster['name'].startswith("production")]
        hg_name = "bigvm_free_host_antiaffinity_hostroup"

        for group in group_ret:
            for subgroup in group:
                try:
                    if not hasattr(subgroup, 'host'):
                        continue
                    if subgroup.name == hg_name:
                        host.append(subgroup.host[0])
                except IndexError as error:
                    log.info("- INFO - No big_vm host defined with error %s", error)
        return host

    # check if a vw is a shadow vm for a volume - those have 128mb ram, 1 cpu, no network, are powered off
    # and the instance name should be the volume uuid (i.e. a valid openstack uuid) and nothing more
    def is_shadow_vm(self, server):
        if server.get('config.hardware.memoryMB') == 128 and server.get('config.hardware.numCPU') == 1 and \
                    server.get('runtime.powerState') == 'poweredOff' and self.shadow_vm_uuid_re.match(str(server.get('name'))) \
                    and not any(isinstance(d, vim.vm.device.VirtualEthernetCard) for d in server.get('config.hardware.device')):
            return True
        else:
            return False

    # check if a vw is a shadow vm for a snapshot - those have 128mb ram, 1 cpu, no network, are powered off
    # and the instance name should be snapshot-<volume-uuid>
    def is_snapshot_shadow_vm(self, server):
        if server.get('config.hardware.memoryMB') == 128 and server.get('config.hardware.numCPU') == 1 and \
                    server.get('runtime.powerState') == 'poweredOff' and self.snapshot_shadow_vm_uuid_re.match(str(server.get('name'))) \
                    and not any(isinstance(d, vim.vm.device.VirtualEthernetCard) for d in server.get('config.hardware.device')):
            return True
        else:
            return False

    # openstack vms habe a name field in their annotations
    def is_openstack_vm(self, server):
        if self.openstack_re.match(str(server.get('config.annotation'))):
            return True
        else:
            return False

    def vmotion_inside_bb(self,openstack_obj,big_vm_name_uuid,free_node_name,data):

        # details about vm and  free node
        vm = self.find_server(big_vm_name_uuid)
        # if vm not found on vcenter return
        vhost = self.get_object_by_name(vim.HostSystem, free_node_name)
        log.info("INFO:  vmotion of instance uuid %s started to target node %s", big_vm_name_uuid, free_node_name)

        # capture the status of server
        # check metadata and lock if exist
        loc_check = openstack_obj.api.compute.get_server(big_vm_name_uuid)
        # if vm not found on vcenter return
        log.info("INFO: instance uuid %s lock status %s", big_vm_name_uuid, loc_check['is_locked'])

        # setting metadata and lock for nanny
        openstack_obj.api.compute.set_server_metadata(big_vm_name_uuid, nanny_metadata=data)
        openstack_obj.api.compute.lock_server(big_vm_name_uuid)
        loc_check = openstack_obj.api.compute.get_server(big_vm_name_uuid)
        log.info("INFO: instance uuid %s lock status set by nanny %s", big_vm_name_uuid, loc_check['is_locked'])

        # actual vmotion step
        spec = vim.VirtualMachineRelocateSpec()
        spec.host = vhost
        task = vm.RelocateVM_Task(spec)
        try:
            state = WaitForTask(task, si=self.api)
        except Exception as e:
            log.error("ERROR: failed to relocate big vm %s to target node %s with error message =>%s",
                          str(big_vm_name_uuid), str(free_node_name), str(e.msg))
            state = "Vmotion_failed"
        else:
            log.info("INFO: vmotion done big vm %s to target node %s and state %s", str(big_vm_name_uuid),
                     str(free_node_name), str(state))

        # if result failed through alert
        # unlock the server and unset nanny metadata
        openstack_obj.api.compute.unlock_server(big_vm_name_uuid)
        openstack_obj.api.compute.delete_server_metadata(big_vm_name_uuid, ['nanny_metadata'])

        # check unlock succesfully done
        unloc_check = openstack_obj.api.compute.get_server(big_vm_name_uuid)
        log.info("INFO: instance uuid %s unlock status %s done", big_vm_name_uuid, unloc_check['is_locked'])

        return state


class VCenterRESTHelper:
    def __init__(self, host, user, password, verify_ssl=False):
        self.api = None
        self.host = host
        self.user = user
        self.password = password
        self.verify_ssl = verify_ssl

        self._URL = 'https://{}/rest{{}}'.format(self.host)

        self.login()

    @staticmethod
    def validate_obj_id(obj_id):
        """Validates the format of an obj_i required by the REST API
        e.g. {"id": "datastore-6561", "type": "Datastore"}
        Raises ValueError on failure.
        """
        if not isinstance(obj_id, dict):
            raise ValueError('obj_id needs to be a dict')

        for k in ('id', 'type'):
            if k not in obj_id:
                raise ValueError('obj_id needs a {}'.format(k))
            if not isinstance(obj_id[k], str):
                raise ValueError('obj_id[{}] needs to of type str'.format(k))

    def login(self):
        s = requests.Session()
        if not self.verify_ssl:
            s.verify = False

        r = s.post(self._URL.format('/com/vmware/cis/session'), auth=(self.user, self.password))
        if r.status_code != 200:
            raise RuntimeError('{}: {}'.format(r, r.content))

        self.api = s

    def disconnect(self):
        if self.api:
            self.api.delete(self._URL.format('/com/vmware/cis/session'))

    def raw_request(self, method, url, unpack=True, **kwargs):
        if not url.startswith('/'):
            url = '/' + url

        r = getattr(self.api, method)(self._URL.format(url), **kwargs)
        if r.status_code != 200:
            raise RuntimeError('{}: {}'.format(r, r.content))

        if r.content:
            data = r.json()
            if unpack and 'value' in data:
                return data['value']
            return data

    def get(self, url, **kwargs):
        return self.raw_request('get', url, **kwargs)

    def post(self, url, **kwargs):
        return self.raw_request('post', url, **kwargs)

    def get_vm_by_name(self, name):
        """Retrieve a VM object by exact name match"""
        data = self.get('/vcenter/vm', params={'filter.names': name})
        if len(data) > 1:
            raise PyCCloudNotFound('More than one VM found with that name.')
        elif not len(data):
            raise PyCCloudNotFound('No VM found with that name.')

        return data[0]

    def get_tagging_categories(self, details=False):
        """Retrieve all categories
        Optionally retrieves the details like the name instead of only the IDs.
        """
        category_ids = self.get('/com/vmware/cis/tagging/category')
        if not details:
            return {i: {} for i in category_ids}

        return {i: self.get_tagging_category(i) for i in category_ids}

    def get_tagging_category(self, category_id):
        """Get a category by id"""
        return self.get('/com/vmware/cis/tagging/category/id:{}'.format(category_id))

    def get_tagging_tags(self, details=False):
        """Retrieve all tags
        Optionally retrieves not only the tag IDS, but also the details of a
        tag like the name.
        """
        tag_ids = self.get('/com/vmware/cis/tagging/tag')
        if not details:
            return {i: {} for i in tag_ids}

        return {i: self.get_tagging_tag(i) for i in tag_ids}

    def get_tagging_tag(self, tag_id):
        """Retrieve a tag by ID"""
        return self.get('/com/vmware/cis/tagging/tag/id:{}'.format(tag_id))

    def find_tag(self, name, category, ignore_case=False):
        """Find a tag by name in a certain category (also identified by name)
        We enrich the returned tag dictionary with the category in the key
        "_category".
        Raises PyCCloudNotFound if something cannot be found.
        """
        categories = self.get_tagging_categories(details=True)
        for c in categories.values():
            if c['name'] == category or \
                    ignore_case and c['name'].lower() == category.lower():
                category = c
                break
        else:
            raise PyCCloudNotFound('Could not find category named "{}".'
                                   .format(category))

        params = {'~action': 'list-tags-for-category'}
        url = '/com/vmware/cis/tagging/tag/id:{}'.format(category['id'])
        for tag_id in self.post(url, params=params):
            tag = self.get_tagging_tag(tag_id)
            if tag['name'] == name or \
                    ignore_case and tag['name'].lower() == name.lower():
                tag['_category'] = category
                break
        else:
            raise PyCCloudNotFound('Could not find tag named "{}" in category "{}".'
                                   .format(name, category))

        return tag

    def list_attached_tags(self, obj_id):
        """List tags attached to an object"""
        self.validate_obj_id(obj_id)
        params = {'~action': 'list-attached-tags'}
        data = {'object_id': obj_id}
        return self.post('/com/vmware/cis/tagging/tag-association', params=params, json=data)

    def detach_tag_from_obj(self, tag_id, obj_id):
        """Detach a tag from an object"""
        self.validate_obj_id(obj_id)
        params = {'~action': 'detach'}
        data = {'tag_id': tag_id,
                'object_id': obj_id}
        return self.post('/com/vmware/cis/tagging/tag-association/id:{}'.format(tag_id), params=params, json=data)

    def attach_tag_to_obj(self, tag_id, obj_id):
        """Attached a tag to an object"""
        self.validate_obj_id(obj_id)
        params = {'~action': 'attach'}
        data = {'tag_id': tag_id,
                'object_id': obj_id}
        return self.post('/com/vmware/cis/tagging/tag-association/id:{}'.format(tag_id), params=params, json=data)
