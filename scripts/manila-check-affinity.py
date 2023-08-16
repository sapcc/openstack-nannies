#!/usr/bin/env python3
#
# Copyright (c) 2023 SAP SE
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
import argparse
import logging
import os

from helper.prometheus_exporter import LabelGauge
from manilananny import ManilaNanny

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(message)s')


class ManilaCheckAffinity(ManilaNanny):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.checks = None
        self.affinity_groups = []
        self.anti_affinity_groups = []
        self.affinityGauge = LabelGauge("manila_nanny_affinity_rule_violation",
                                        "Violated Affinity Rules", ['share_id', 'rule'])
        self.antiAffinityGauge = LabelGauge("manila_nanny_anti_affinity_rule_violation",
                                            "Violated Anti Affinity Rules", ['share_id', 'rule'])

    def _run(self):
        LOG.info("Checking Anti-Affinity Rules")

        violated_anti_affinity_rules = []
        share_rules = self.query_shares_with_anti_affinity_rules()
        for share, rule in share_rules:
            if not self._check_rule(share, rule, attracting=False):
                violated_anti_affinity_rules.append({'share_id': share, 'rule': rule})
        self.antiAffinityGauge.export(violated_anti_affinity_rules)

        LOG.info("Checking Affinity Rules")

        violated_affinity_rules = []
        share_rules = self.query_shares_with_affinity_rules()
        for share, rule in share_rules:
            if not self._check_rule(share, rule, attracting=True):
                violated_affinity_rules.append({'share_id': share, 'rule': rule})
        self.affinityGauge.export(violated_affinity_rules)

        LOG.info("Finished Checking")

    def _update_affinity_groups(self):
        self.affinity_groups = []
        share_rules = self.query_shares_with_affinity_rules()
        for share, rule in share_rules:
            # the rule is a comma separated list of share ids, e.g.
            # 5c16a438-0879-4f2b-bfab-6b170c70f509,eda52baa-ec53-49c4-a5f2-5ca61e65d6b9
            shares = [share, *rule.split(',')]
            self.affinity_groups.append(shares)

    def _update_anti_affinity_groups(self):
        self.anti_affinity_groups = []
        share_rules = self.query_shares_with_anti_affinity_rules()
        for share, rule in share_rules:
            shares = [share, *rule.split(',')]
            self.anti_affinity_groups.append(shares)

    def _check_rule(self, share, affinity_shares, attracting=True):
        # affinity rule is a comma separated list of share ids, e.g.
        # 5c16a438-0879-4f2b-bfab-6b170c70f509,eda52baa-ec53-49c4-a5f2-5ca61e65d6b9
        affinity_shares = [*affinity_shares.split(',')]

        host = self.query_share_host(share).get('host')
        if host is None:
            LOG.warning("Host for Share %s is unknown", share)

        affinity_hosts = []
        for s in affinity_shares:
            h = self.query_share_host(s).get('host')
            if h is None:
                LOG.warning("Host for Share %s is unknown", s)
            affinity_hosts.append(h)

        host = host.split('#')[0]
        affinity_hosts = [h.split('#')[0] for h in affinity_hosts]

        if attracting:
            # attracting affinity rules require first share to be on the same host as one of the others
            check = host in affinity_hosts
            if not check:
                LOG.error("Affinity rule violated for Share %s", share)
                LOG.error("Share %s on host %s", share, host)
                for s in zip(affinity_shares, affinity_hosts):
                    LOG.error("Share %s on host %s", s[0], s[1])
        else:
            # anti-affinity rules require share to be on a different host than all affinity shares
            check = host not in affinity_hosts
            if not check:
                LOG.error("Anti-Affinity rule violated for Share %s", share)
                LOG.error("Share %s on host %s", share, host)
                for s in zip(affinity_shares, affinity_hosts):
                    LOG.error("Share %s on host %s", s[0], s[1])

        return check

if __name__ == "__main__":

    default_config_file = os.environ.get('MANILA_NANNY_CONFIG') or '/etc/manila/manila.conf'
    default_interval = os.environ.get('MANILA_NANNY_INTERVAL') or 3600
    default_prom_port = os.environ.get('MANILA_NANNY_PROMETHEUS_PORT') or 9000

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", dest="config_file", default=default_config_file, help="configuration file")
    parser.add_argument("--interval", type=int, default=default_interval, help="interval")
    parser.add_argument("--pdb-port", type=int, default=50000, help="port for pdb_attach server")
    parser.add_argument("--prom-port", type=int, default=default_prom_port, help="port for prometheus exporter")

    ManilaCheckAffinity(**vars(parser.parse_args())).run()
