import requests
import json
import os
import queue
import logging
from prometheus_api_client import PrometheusConnect

log = logging.getLogger('prometheus_helper')


class PrometheusInfraConnect:

    def __init__(self, region, prometheus_infra=None):
        self.api = None
        self.region = region
        self.prometheus_infra = prometheus_infra or f'http://prometheus-infra-collector.infra-monitoring.svc.kubernetes.{region}.cloud.sap:9090'

        self.login()

    def login(self):
            self.api = PrometheusConnect(url=self.prometheus_infra)

    def find_vm_readiness(self,vcenter,vm):

        avail_zone = self.region + vcenter.split("-",2)[1]
        vm_label_config = {"datacenter": avail_zone, "virtualmachine": vm}
        try:
            vm_cpu_ready_ratio = float(self.api.get_current_metric_value(metric_name='vrops_virtualmachine_cpu_ready_ratio',
                                                            label_config=vm_label_config)[0]['value'][1])
            vm__memory_activewrite_kb = float(self.api.get_current_metric_value(metric_name='vrops_virtualmachine_memory_activewrite_kilobytes',
                                                             label_config=vm_label_config)[0]['value'][1])
            if vm_cpu_ready_ratio < 1 and vm__memory_activewrite_kb < 21000000:
                return "vm_readiness"
            log.info("- INFO - vm name %s has cpu_ready_ratio %s and memory_activewrite_kb %s ",str(vm), str(vm_cpu_ready_ratio),str(vm__memory_activewrite_kb))
            return "no_vm_readiness"
        except Exception as e:
            log.warn("problems connecting vm %s in prometheus infra: %s", str(vm),str(e))
            return "prom_issue"

    def find_host_contention(self,vcenter,host):

        avail_zone = self.region + vcenter.split("-",2)[1]
        host_label_config = {"datacenter": avail_zone, "hostsystem": host}
        try:
            host_cpu_contention = float(self.api.get_current_metric_value(metric_name='vrops_hostsystem_cpu_contention_percentage',
                                                         label_config=host_label_config)[0]['value'][1])
            host_memory_contention = float(self.api.get_current_metric_value(metric_name='vrops_hostsystem_memory_contention_percentage',
                                                         label_config=host_label_config)[0]['value'][1])
            if host_cpu_contention < 3 and host_memory_contention == 0:
                return "host_contention"
            log.info("- INFO - host name %s has host_cpu_contention %s and  host_memory_contention %s",host,host_cpu_contention,host_memory_contention)
            return "no_host_contention"
        except Exception as e:
            log.warn("problems connecting host %s in prometheus infra: %s", str(host),str(e))
            return "prom_issue"
