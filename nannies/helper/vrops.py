from urllib3 import disable_warnings
from urllib3 import exceptions
from threading import Thread
import requests
import json
import os
import queue
import logging

logger = logging.getLogger('vrops-exporter')


class Vrops:
    def get_token(self,target,user,password):
        url = "https://" + target + "/suite-api/api/auth/token/acquire"
        headers = {
            'Content-Type': "application/json",
            'Accept': "application/json"
        }
        payload = {
            "username": user,
            "authSource": "Local",
            "password": password
        }
        disable_warnings(exceptions.InsecureRequestWarning)
        try:
            response = requests.post(url,
                                     data=json.dumps(payload),
                                     verify=False,
                                     headers=headers,
                                     timeout=10)
        except Exception as e:
            logger.error(f'Problem connecting to {target}. Error: {e}')
            return False, 503

        if response.status_code == 200:
            return response.json()["token"], response.status_code
        else:
            logger.error(f'Problem getting token from {target} : {response.text}')
            return False, response.status_code

    def get_adapter(self, target: str, token: str) -> (str, str):
        url = f'https://{target}/suite-api/api/adapters'
        querystring = {
            "adapterKindKey": "VMWARE"
        }
        headers = {
            'Content-Type': "application/json",
            'Accept': "application/json",
            'Authorization': f"vRealizeOpsToken {token}"
        }
        name = uuid = None
        disable_warnings(exceptions.InsecureRequestWarning)
        try:
            response = requests.get(url,
                                    params=querystring,
                                    verify=False,
                                    headers=headers)
        except Exception as e:
            logger.error(f'Problem connecting to {target} - Error: {e}')
            return name, uuid

        if response.status_code == 200:
            for resource in response.json()["adapterInstancesInfoDto"]:
                name = resource["resourceKey"]["name"]
                uuid = resource["id"]

        else:
            logger.error(f'Problem getting adapter {target} : {response.text}')
            return name, uuid
        return name, uuid

