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
import logging

import manilaclient.common.apiclient.exceptions as manilaexceptions
import yaml
from helper.manilananny import ManilaNanny, base_command_parser
from helper.netapp_rest import NetAppRestHelper
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

        # intialize gauge
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

        # mapping of filer name and host
        # 'manila-share-netapp-ma01-st051': 'stnpca1-st051.cc.qa-de-1.cloud.sap'
        manila_filers = {
            f'manila-share-netapp-{filer["name"]}': filer['host']
            for filer in self.netapp_filers
        }

        logging.info("fetching Manila Snapshots...")

        for _snapshot in manila.share_snapshots.list(search_opts={'all_tenants': True}):
            try:
                # with detailed=True, next call may abort with an internal error
                _snap_instances = manila.share_snapshot_instances.list(
                    detailed=True, snapshot=_snapshot)
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
                vols = netapp.get_volumes(name=vol_name)
                if len(vols) > 0:
                    _snaps = netapp.get_snapshots(vols[0].uuid, name="share_snapshot_*")
                    _snap_names = [_.name for _ in _snaps]
                    for s in snapshots:
                        if s['netapp_provider_location'] not in _snap_names:
                            missing_snaps.append(s)
                else:
                    logging.warning(f'Volume {vol_name} not found on {host}')
                    missing_snaps.extend(snapshots)

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
            # check if snapshot is available
            if snap_instance.status == 'available':
                logging.warning(f'Snapshot not found: {snap}')
                missing_snapshots.append(snap)

        logging.info(f'{len(missing_snapshots)} missing Snapshot Instances found')
        return missing_snapshots


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
