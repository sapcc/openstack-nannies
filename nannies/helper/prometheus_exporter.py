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

from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, REGISTRY

import logging
import time

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')

class CustomCollector():
    def __init__(self, metricsobject, dataobject):
        self.metricsobject = metricsobject
        self.dataobject = dataobject

    def describe(self):
        for metric in self.metricsobject.get_metrics():
            # metric = metricname, metric[0] = description
            yield GaugeMetricFamily(metric, metric[0])

    def collect(self):
        metric = self.metricsobject.get_metrics()
        gauge = dict()
        metrics_data = self.dataobject.get_data()
        if len(metrics_data)>0:
            # metric = metricname, metric[0] = description, metric[1] = labelnames
            for data in metrics_data:
                gauge[data[0]] = GaugeMetricFamily(data[0], metric[data[0]][0], labels=metric[data[0]][1])
                gauge[data[0]].add_metric(labels=metrics_data[data]['labels'], value = metrics_data[data]['values'])
                yield gauge[data[0]]

class PromMetricsClass:
    def __init__(self):
        self.metrics = dict()

    # metricname = string, metricdescription = string, labelnames = list of strings
    def set_metrics(self, metricname, metricdescription, labelnames):
        self.metrics[metricname] = (metricdescription, labelnames)

    def get_metrics(self):
        return self.metrics

class PromDataClass:
    def __init__(self):
        self.values_in = dict()
        self.values_out = dict()

    def set_data(self, metricname, datavalue, labelvalues):
        self.values_in[(metricname,tuple(labelvalues))] = { 'values': datavalue, 'labels': labelvalues }

    def sync_data(self):
        self.values_out = self.values_in.copy()
        self.values_in.clear()

    def get_data(self):
        return self.values_out

# start prometheus exporter if needed
def prometheus_http_start(prometheus_port):
    # start http server for exported data
    try:
        start_http_server(prometheus_port)
        log.info("INFO: prometheus metrics exporter started")
    except Exception as e:
        log.error("- ERROR - failed to start prometheus exporter http server: %s", str(e))
