#!/usr/bin/env python
#
# Copyright (c) 2022 SAP SE
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
import configparser
import logging
import sys

import manilaclient.common.apiclient.exceptions as manilaexceptions
import yaml
from helper.manilananny import ManilaNanny, base_command_parser
from helper.netapp_rest import NetAppRestHelper
from helper.prometheus_connect import PrometheusInfraConnect
from helper.prometheus_exporter import LabelGauge
from netapp_ontap.error import NetAppRestError

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(message)s')
logging.getLogger('helper').setLevel(logging.DEBUG)


class MissingSnapshotNanny(ManilaNanny):

    def __init__(self, config_file, netapp_filers, interval, prom_port) -> None:
        super(MissingSnapshotNanny, self).__init__(config_file, interval, prom_port)
        with open(netapp_filers, "r") as f:
            self.netapp_filers = yaml.safe_load(f)["filers"]

        # parse manila config for region value
        try:
            parser = configparser.ConfigParser()
            parser.read(self.config_file)
            region = parser.get("keystone_authtoken", "region_name")
        except Exception as e:
            print(f"ERROR: Parse {self.config_file}: " + str(e))
            sys.exit(2)

        # initialize prom client
        self.prom_client = PrometheusInfraConnect(region=region)
        # initialize gauge
        self.missing_snapshot_gauge = LabelGauge(
            "manila_nanny_missing_snapshot_instance",
            "Missing Snapshot Instances on Netapp filers",
            [
                'netapp_host', 'netapp_volume', 'netapp_provider_location', 'share_id',
                'share_instance_id', 'snapshot_id', 'snapshot_instance_id', 'status', 'created_at'
            ],
        )

    def _run(self):
        log.info(f'start {self.__class__.__name__}.{self._run.__name__}()')
        self.missing_snapshot_gauge.export(self._get_missing_snapshots())

    def _get_missing_snapshots(self):
        """get list of snapshots that are not found on netapp filer"""
        manila = self.get_manilaclient("2.19")
        manila_snaps = {}
        missing_snaps = []
        missing_snapshots = []

        manila_host_prefix = 'manila-share-netapp-'
        # mapping of filer name and host
        # 'manila-share-netapp-ma01-st051': 'stnpca1-st051.cc.qa-de-1.cloud.sap'
        manila_filers = {
            f'{manila_host_prefix}{filer["name"]}': filer['host']
            for filer in self.netapp_filers
        }

        logging.info("fetching Manila Snapshots...")
        _, count = manila.share_snapshots.list(
            search_opts={'all_tenants': True, 'with_count': True})
        count = (count / 1000) + 1

        snapshots = []
        offset = 0
        for i in range(int(count)):
            snapshots.extend(manila.share_snapshots.list(
                search_opts={'all_tenants': True, 'limit': 1000, 'offset': offset}))
            offset = offset + 1000

        for _snapshot in snapshots:
            try:
                # with detailed=True, next call may abort with an internal error
                _snap_instances = manila.share_snapshot_instances.list(
                    detailed=True, snapshot=_snapshot.id)
                if not isinstance(_snap_instances, list):
                    raise Exception("not a list")
            except manilaexceptions.InternalServerError as e:
                logging.error(e)
                continue

            for _snap_instance in _snap_instances:
                if _snap_instance.status == 'available':
                    try:
                        _share_instance = manila.share_instances.get(_snap_instance.share_instance_id)
                    except manilaexceptions.NotFound:
                        logging.warning(f'share instance {_snap_instance.share_instance_id} '
                                        f'of snapshot instance {_snap_instance.id} not found')
                        continue
                    # map Share Instance Host to NetApp Filer's fqdn, like
                    # 'manila-share-netapp-ma01-md004@ma01-md004#aggr_ssd_stnpa1_01_md004_1'
                    #   -> 'stnpca1-md004.cc.qa-de-1.cloud.sap'
                    fname = _share_instance.host.split('@')[0]
                    fhost = manila_filers.get(fname)
                    if not fhost:
                        logging.warning(f'{fname} not in {manila_filers.keys()}')
                        continue
                    # map Share Instance Id to NetApp Volume name
                    volume = 'share_' + _share_instance.id.replace('-', '_')

                    manila_snaps[fhost] = manila_snaps.get(fhost, {})
                    manila_snaps[fhost][volume] = manila_snaps[fhost].get(volume, [])
                    manila_snaps[fhost][volume].append({
                        'netapp_host': fhost,
                        'netapp_volume': volume,
                        'netapp_provider_location': _snap_instance.provider_location,
                        'share_id': _snap_instance.share_id,
                        'share_instance_id': _snap_instance.share_instance_id,
                        'snapshot_id': _snap_instance.snapshot_id,
                        'snapshot_instance_id': _snap_instance.id,
                        'status': _snap_instance.status,
                        'created_at': _snap_instance.created_at,
                    })

        for host, volume_snaps in manila_snaps.items():
            netapp = self.get_netapprestclient(host)
            snap_count = sum([len(v) for v in volume_snaps.values()])
            logging.info(f"checking {snap_count} snapshots on filer {host}...")

            for vol_name, snapshots in volume_snaps.items():
                _check_provider_location_on_filer(host, netapp, vol_name, missing_snaps, snapshots)

        # double check the status of snapshot and share, because they can be changed in the mean while
        for snap in missing_snaps:
            try:
                snap_instance = manila.share_snapshot_instances.get(snap['snapshot_instance_id'])
            except manilaexceptions.NotFound:
                continue
            # skip if share instance is not available
            share_instance = manila.share_instances.get(snap_instance.share_instance_id)
            if share_instance.status != 'available':
                continue
            # skip if snapshot instance is not available
            if snap_instance.status != 'available':
                continue

            # double checking for SVM DR move to another filer
            manila_filer_name = share_instance.host.split('@')[0]
            manila_host = manila_filers.get(manila_filer_name)
            prom_filer_name = self.prom_client.get_share_filer_name(share_instance.share_id)
            if prom_filer_name is None:
                logging.error(f'no snapshot metrics found on any filer for share {share_instance.share_id}')
                continue
            prom_host = manila_filers.get(f'{manila_host_prefix}{prom_filer_name}')
            if manila_host != prom_host:
                prom_netapp = self.get_netapprestclient(prom_host)
                # map Share Instance Id to NetApp Volume name
                volume_name = 'share_' + share_instance.id.replace('-', '_')
                if _check_provider_location_on_filer(prom_host, prom_netapp, volume_name, missing_snapshots, [snap]):
                    logging.warning(f'Parent volume {volume_name} of snapshot {snap} on wrong filer - '
                                    f'expected on {manila_host}, found on {prom_host}')
                    # wrong location, but not missing - moving on ..
                    continue

            logging.error(f'Snapshot not found: {snap}')
            missing_snapshots.append(snap)

        logging.info(f'{len(missing_snapshots)} missing Snapshot Instances found')
        return missing_snapshots

def _check_provider_location_on_filer(host, netapp_cli, vol_name,
                                      missing_on_filer=[], snaps_to_check=[]):
    snapshots_exist = True
    vols = netapp_cli.get_volumes(name=vol_name)
    if len(vols) > 0:
        _snaps = netapp_cli.get_snapshots(vols[0].uuid, name="share_snapshot_*")
        _snap_names = [_.name for _ in _snaps]
        for s in snaps_to_check:
            if s['netapp_provider_location'] not in _snap_names:
                missing_on_filer.append(s)
                snapshots_exist = False
    else:
        logging.warning(f'Volume {vol_name} not found on {host}')
        missing_on_filer.extend(snaps_to_check)
        snapshots_exist = False

    return snapshots_exist

def list_share_snapshots(netappclient: NetAppRestHelper):
    """ Get snapshots of all volumes on Netapp filer """
    snapshots = []
    for vol in netappclient.get_volumes(name="share_*"):
        try:
            volsnapshots = netappclient.get_snapshots(vol.uuid, name="share_snapshot_*")
            snapshots.extend(volsnapshots)
        except NetAppRestError as e:
            # Normally the exception can be ignored, for example volume can be
            # deleted in the mean time.
            log.warn(f'failed to get snapshot: {e}')
    return snapshots


def main():
    parser = base_command_parser()
    parser.add_argument("--netapp-filers", default="/manila-etc/netapp-filers.yaml",
                        help="Netapp filers list")
    args = parser.parse_args()

    log.info(f'start MissingSnapshotNanny...')
    log.info(f'parameter: config file: {args.config}')
    log.info(f'parameter: refresh interval: {args.interval}s')
    log.info(f'parameter: Netapp filers file: {args.netapp_filers}')
    log.info(f'parameter: Prometheus exporter port: {args.prom_port}')

    MissingSnapshotNanny(args.config, args.netapp_filers, args.interval, args.prom_port).run()


if __name__ == "__main__":
    main()
