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

import logging

from prometheus_client import start_http_server
from prometheus_client.core import Gauge, GaugeMetricFamily

log = logging.getLogger(__name__)


class CustomCollector:
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


class LabelGaugeError(Exception):
    pass


class LabelGauge:
    """
    Parse input data as labels and export them as promethues Gauge

    Usage: Initiate as the normal Gauge, with name, helper string and label list.
    Call the method export() to set gauge labels.
    """

    def __init__(self, name, helper, labels):
        self._gauge = Gauge(name, helper, labels)
        self._labelkey_cache = {}

    def export(self, data):
        """
        This method expects a list of gauge lables as input data:
            [
                { "id": "xxx", "server": "xxx" },
                { "id": "yyy", "server": "yyy" },
            ]
        and export them as gauge labels. Gauge with labels that are not in the
        input data are removed.
        """
        _labelkey_cache = {}

        # process labels and set gauge
        for labels_input in data:
            # remove invalid gauge labels from input
            labels = {}
            for labelname in self._gauge._labelnames:
                if labelname not in labels_input:
                    raise LabelGaugeError(f'label "{labelname}" not found while exporting')
                labels[labelname] = labels_input[labelname]

            # generate gauge label key and cache them
            # the key is built from concatenated {label_name} and {label_value}
            _labelkey_cache[self.serialize_labels(labels)] = labels
            # set gauge
            self._gauge.labels(**labels).set(1)

        # remove gauge with unfound labels
        for labelkey, labels in self._labelkey_cache.items():
            if labelkey not in _labelkey_cache.keys():
                self._gauge.remove(*labels.values())

        # cache labels
        self._labelkey_cache = _labelkey_cache

    def serialize_labels(self, labels):
        result = ""
        for lname in self._gauge._labelnames:
            result = f"{result}__{lname}_{labels[lname]}"
        return result + "__"


# start prometheus exporter if needed
def prometheus_http_start(prometheus_port):
    # start http server for exported data
    try:
        start_http_server(prometheus_port)
        log.info("INFO: prometheus metrics exporter started")
    except Exception as e:
        log.error("- ERROR - failed to start prometheus exporter http server: %s", str(e))
