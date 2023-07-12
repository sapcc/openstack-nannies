#!/usr/bin/env python3
import argparse
import logging

from helper.prometheus_exporter import LabelGauge, LabelGaugeError
from manilananny import ManilaNanny
from prometheus_client import Info

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

        self._update_anti_affinity_groups()

        violated_rules = []
        for grp in self.anti_affinity_groups:
            check, share_hosts = self._check_group(grp, attracting=False)
            if not check:
                LOG.error("Anti-Affinity rule violated for Shares")
                for s in share_hosts:
                    LOG.error("Share %s on host %s", s['share_id'], s['host'])

        self.antiAffinityGauge.export(violated_rules)

        LOG.info("Checking Affinity Rules")

        self._update_affinity_groups()

        violated_rules = []
        for grp in self.affinity_groups:
            check, share_hosts = self._check_group(grp)
            if not check:
                LOG.error("Affinity rule violated for Shares")
                for s in share_hosts:
                    LOG.error("Share %s on host %s", s['share_id'], s['host'])
                violated_rules.append({'share_id': grp[0], 'rule': ','.join(grp[1:])})

        self.affinityGauge.export(violated_rules)

        LOG.info("Finished Checking")

    def _update_affinity_groups(self):
        self.affinity_groups = []
        share_rules = self.query_shares_with_affinity_rules()
        for share, rule in share_rules:
            shares = [share, *rule.split(',')]
            self.affinity_groups.append(shares)

    def _update_anti_affinity_groups(self):
        self.anti_affinity_groups = []
        share_rules = self.query_shares_with_anti_affinity_rules()
        for share, rule in share_rules:
            shares = [share, *rule.split(',')]
            self.anti_affinity_groups.append(shares)

    def _check_group(self, shares, attracting=True):
        if len(shares) == 1:
            return True

        hosts = []
        share_hosts = self.query_share_hosts(shares)
        for share, host in map(lambda x: (x['share_id'], x['host']), share_hosts):
            if host == '':
                LOG.error("Share %s does not exist on filer", share)
            else:
                hosts.append(host.split('#')[0])

        if attracting:
            # atrracting affinity rules require all shares to be on the same host
            return len(set(hosts)) == 1, share_hosts
        else:
            # anti-affinity rules require all shares to be on different hosts
            return len(set(hosts)) == len(hosts), share_hosts


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", dest="config_file", default='/etc/manila/manila.conf',
                        help='configuration file')
    parser.add_argument("--interval", type=float, default=86400, help="interval")
    parser.add_argument("--pdb-port", type=int, default=50000, help="port for pdb_attach server")
    parser.add_argument("--prom-port", type=int, default=9000, help="port for prometheus exporter")

    ManilaCheckAffinity(**vars(parser.parse_args())).run()
