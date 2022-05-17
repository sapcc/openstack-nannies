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
            "snapshot not found on netapp filer",
            ["id", "snapshot_id", "status"],
        )

    def _run(self):
        print("export missing snapshots")
        self.missing_snapshot_gauge.export(self._get_missing_snapshots())

    def _get_missing_snapshots(self):
        """get list of snapshots that are not found on netapp filer"""
        # list snapshots from Manila; and build a hash table with snapshot id as key
        manilaclient = self.get_manilaclient("2.19")
        search_opts = {"all_tenants": True}
        response = manilaclient.share_snapshot_instances.list(search_opts=search_opts)
        snapshot_instance_table = {
            snapshot_instance.id: snapshot_instance for snapshot_instance in response
        }

        # find snapshots from Netapp Filer with snapshot name in the format
        # "share_snapshot_<id>", and build a list of their ids
        snapshots_on_netapp = []
        for filer in self.netapp_filers:
            netappclient = self.get_netappclient(filer)
            resp = netappclient.get_list(
                "snapshot-get-iter", des_result={"snapshot-info": ["name", "volume"]}
            )
            for snapshot in resp:
                if snapshot["name"].startswith("share_snapshot_"):
                    id = snapshot["name"][len("share_snapshot_") :].replace("_", "-")
                    snapshots_on_netapp.append(id)

        # return missing snapshot list
        for sid in snapshots_on_netapp:
            if sid in snapshot_instance_table:
                snapshot_instance_table.pop(sid)
        return [
            {
                "id": snapshot_instance.id,
                "status": snapshot_instance.status,
                "snapshot_id": snapshot_instance.snapshot_id,
            }
            for snapshot_instance in snapshot_instance_table.values()
        ]


def main():
    parser = base_command_parser()
    parser.add_argument(
        "--netapp-filers", default="/manila-etc/netapp-filers.yaml", help="Netapp filers list"
    )
    args = parser.parse_args()

    MissingSnapshotNanny(
        args.config,
        args.netapp_filers,
        args.interval,
        args.prom_port,
    ).run()


if __name__ == "__main__":
    main()
