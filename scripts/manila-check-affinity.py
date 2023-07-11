#!/usr/bin/env python3
import argparse
import logging

from manilananny import ManilaNanny
from prometheus_client import Info

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(message)s')


class ManilaCheckAffinity(ManilaNanny):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.checks = None
        self.affinityInfo = Info('manila_nanny_affinity_check', 'Check Affinity Rules')

    def _run(self):
        LOG.info("Checking Affinity Rules")
        self.checks = self._check_affinity()

    def _check_affinity(self):
        # get all shares with metadata key '__affinity_same_host'
        shares = self.manilaclient.shares.list(search_opts={'metadata': {'__affinity_same_host': 'true'}})
        print(shares)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", dest="config_file", default='/etc/manila/manila.conf',
                        help='configuration file')
    parser.add_argument("--dry-run", action="store_true", help="dry run")
    parser.add_argument("--interval", type=float, default=3600, help="interval")
    parser.add_argument("--pdb-port", type=int, default=50000, help="port for pdb_attach")
    parser.add_argument("--prom-port", type=int, default=9000, help="prometheus port")
    parser.add_argument("--debug", action="store_true", help="debug")

    ManilaCheckAffinity(**vars(parser.parse_args())).run()
