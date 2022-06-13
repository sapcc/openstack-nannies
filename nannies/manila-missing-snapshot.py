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
import yaml
from helper.manilananny import ManilaNanny, base_command_parser
from helper.prometheus_exporter import LabelGauge


class MissingSnapshotNanny(ManilaNanny):

    def __init__(self, config_file, netapp_filers, interval, prom_port) -> None:
        super(MissingSnapshotNanny, self).__init__(config_file, interval, prom_port)
        with open(netapp_filers, "r") as f:
            self.netapp_filers = yaml.safe_load(f)["filers"]

        # intialize gauge
        self.missing_snapshot_gauge = LabelGauge(
            "manila_nanny_missing_snapshot_instance",
            "list snapshot instances that are not found on Netapp filers",
            ["share_id", "snapshot_id", "provider_location", "status"],
        )

    def _run(self):
        print("export missing snapshots")
        self.missing_snapshot_gauge.export(self._get_missing_snapshots())

    def _get_missing_snapshots(self):
        """get list of snapshots that are not found on netapp filer"""
        # List Manila's snapshots with details, e.g. a snapshot response
        # {
        #     "id": "57c9b9a2-14fe-47fa-bb47-9c3b80a088bd",
        #     "snapshot_id": "fbc23c16-c10f-4d96-a456-f4ab62454bf8",
        #     "created_at": "2022-06-13T14:05:40.190032",
        #     "updated_at": "2022-06-13T14:05:40.627164",
        #     "status": "available",
        #     "share_id": "0fa59dbe-b1bd-4699-9a3b-6d86996a019f",
        #     "share_instance_id": "b8fa6b49-941f-44ae-a485-9c77c0ca314a",
        #     "progress": "100%",
        #     "provider_location": "share_snapshot_57c9b9a2_14fe_47fa_bb47_9c3b80a088bd",
        # }
        manila = self.get_manilaclient("2.19")
        response = manila.share_snapshot_instances.list(search_opts={"all_tenants": True},
                                                        detailed=True)
        snapshot_instance_table = {
            snapshot_instance.id: snapshot_instance.to_dict()
            for snapshot_instance in response
        }

        # 1. Find snapshots from Netapp Filer and retrieve snapshot instance id
        # by format "share_snapshot_<id>"
        # 2. Remove _id from snapshot_instance_table if corresponding snapshot
        # is found on Netapp filers, the rest are the missing ones
        for filer in self.netapp_filers:
            netapp = self.get_netappclient(filer)
            resp = netapp.get_list("snapshot-get-iter",
                                   des_result={"snapshot-info": ["name", "volume"]})
            for snapshot in resp:
                if snapshot["name"].startswith("share_snapshot_"):
                    _id = snapshot["name"][len("share_snapshot_"):].replace("_", "-")
                    if _id in snapshot_instance_table:
                        snapshot_instance_table.pop(_id)
        return snapshot_instance_table.values()


def main():
    parser = base_command_parser()
    parser.add_argument("--netapp-filers",
                        default="/manila-etc/netapp-filers.yaml",
                        help="Netapp filers list")
    args = parser.parse_args()

    MissingSnapshotNanny(
        args.config,
        args.netapp_filers,
        args.interval,
        args.prom_port,
    ).run()


if __name__ == "__main__":
    main()
