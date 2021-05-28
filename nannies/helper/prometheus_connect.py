import requests
import json
import os
import queue
import logging
from prometheus_api_client import PrometheusConnect

log = logging.getLogger('prometheus_helper')


class PrometheusInfraConnect:

    def __init__(self, region, verify_ssl=False):
        self.api = None
        self.region = region
        self.prometheus_infra = "https://prometheus-infra-collector." + self.region + ".cloud.sap"

        self.login()

    def login(self):
            self.api = PrometheusConnect(url=self.prometheus_infra, disable_ssl=False,retry = None)
            self.api._session.cert = ('/etc/secret-volume/client_cert', '/etc/secret-volume/client_key')

    def find_vm_readiness(self,vcenter,vm):

        avail_zone = self.region + vcenter.split("-",2)[1]
        vm_label_config = {"datacenter": avail_zone, "virtualmachine": vm}
        try:
            if float(self.api.get_current_metric_value(metric_name='vrops_virtualmachine_cpu_ready_ratio',
                                                            label_config=vm_label_config)[0]['value'][1]) < 1 \
                    and float(self.api.get_current_metric_value(metric_name='vrops_virtualmachine_memory_activewrite_kilobytes',
                                                             label_config=vm_label_config)[0]['value'][1]) < 21000:
                return "vm_readiness"
            return "no_vm_readiness"
        except Exception as e:
            log.warn("problems connecting to prometheus infra: %s", str(e))
            return "prom_issue"

    def find_host_contention(self,vcenter,host):

        avail_zone = self.region + vcenter.split("-",2)[1]
        host_label_config = {"datacenter": avail_zone, "hostsystem": host}
        try:
            if float(self.api.get_current_metric_value(metric_name='vrops_hostsystem_cpu_contention_percentage',
                                                         label_config=host_label_config)[0]['value'][1]) < 3 \
                    and float(self.api.get_current_metric_value(metric_name='vrops_hostsystem_memory_contention_percentage',
                                                         label_config=host_label_config)[0]['value'][1]) == 0:
                return "host_contention"
            return "no_host_contention"
        except Exception as e:
            log.warn("problems connecting to prometheus infra: %s", str(e))
            return "prom_issue"
