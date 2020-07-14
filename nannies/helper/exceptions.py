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

class PyCCloudException(Exception):
    pass


class PyCCloudNotFound(PyCCloudException):
    pass


class ASRError(PyCCloudException):
    pass


class NoActiveASRFound(PyCCloudNotFound, ASRError):
    def __init__(self, agent, tried_devices):
        self.agent = agent
        self.tried_devices = tried_devices
        msg = 'Could not find active ASR for agent {}, tried: {}'.format(agent, ', '.join(tried_devices))

        super(NoActiveASRFound, self).__init__(msg)


class NoASRFound(PyCCloudNotFound, ASRError):
    def __init__(self, agent):
        self.agent = agent
        msg = 'No ASR found for agent {}'.format(agent)

        super(NoASRFound, self).__init__(msg)


class ASRVRFNotFound(PyCCloudNotFound, ASRError):
    def __init__(self, host, vrf):
        self.host = host
        self.vrf = vrf
        msg = 'VRF {} not found on ASR {}'.format(vrf, host)

        super(ASRVRFNotFound, self).__init__(msg)


class NotFoundInSecrets(PyCCloudNotFound):
    def __init__(self, name, resource):
        msg = 'Resource {} for "{}" not found in secrets'.format(resource, name)
        super(NotFoundInSecrets, self).__init__(msg)


class PyCCloudUnconfigured(PyCCloudException):
    def __init__(self, _env_vars=None, **kwargs):
        self.env_vars = _env_vars
        self.missing_params = [k for k, v in kwargs.items() if v is None]
        self.missing_msg = 'Missing parameter for pyccloud configuration: {}'.format(', '.join(self.missing_params))
        msg = self.missing_msg

        self.env_msg = ''
        if self.env_vars:
            self.env_msg = 'The following env-vars are available: {}'.format(', '.join(self.env_vars))
            msg = '{} ({})'.format(self.missing_msg, self.env_msg)

        super(PyCCloudUnconfigured, self).__init__(msg)


class VCenterObjectNotFound(PyCCloudNotFound):
    def __init__(self, name, objtype):
        self.name = name
        self.objtype = objtype

        msg = 'Could not find "{}" of type "{}" in VCenter'.format(name, objtype)
        super(VCenterObjectNotFound, self).__init__(msg)


class AristaSwitchNotFound(PyCCloudNotFound):
    def __init__(self, name):
        self.name = name
        msg = 'Switch with attribute "{}" not found'.format(self.name)
        super(AristaSwitchNotFound, self).__init__(msg)


class SentryException(PyCCloudException):
    pass