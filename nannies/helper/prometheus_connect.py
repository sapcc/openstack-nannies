import requests
import json
import os
import queue
import logging
from prometheus_api_client import PrometheusConnect

logger = logging.getLogger('vrops-exporter')


class PrometheusInfraConnect:

    def __init__(self, region, verify_ssl=False):
        self.api = None
        self.region = region
        self.prometheus_infra = "https://prometheus-infra-collector." + self.region + ".cloud.sap"

        self.login()

    def login(self):
        self.api = PrometheusConnect(url=self.prometheus_infra, disable_ssl=False)
        self.api._session.cert = ('certs/client.cert', 'certs/client.key')

    def find_vm_readiness(self,avail_zone,vm):

        vm_label_config = {"datacenter": avail_zone, "virtualmachine": vm}
        if float(self.api.get_current_metric_value(metric_name='vrops_virtualmachine_cpu_ready_ratio',
                                                        label_config=vm_label_config)[0]['value'][1]) < 1 \
                and float(self.api.get_current_metric_value(metric_name='vrops_virtualmachine_memory_activewrite_kilobytes',
                                                         label_config=vm_label_config)[0]['value'][1]) < 21000:
            return True
        return False


    def find_host_contention(self,avail_zone,host):

        host_label_config = {"datacenter": avail_zone, "hostsystem": host}
        if float(self.api.get_current_metric_value(metric_name='vrops_hostsystem_cpu_contention_percentage',
                                                     label_config=host_label_config)[0]['value'][1]) < 3 \
                and float(self.api.get_current_metric_value(metric_name='vrops_hostsystem_memory_contention_percentage',
                                                     label_config=host_label_config)[0]['value'][1]) == 0:
            return True
        return False


