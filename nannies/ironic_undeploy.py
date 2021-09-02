#!/usr/bin/env python

import argparse
import logging
import os
import sys
import time

from helper.openstack import OpenstackHelper


def parse_commandline():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="dry run option not doing anything harmful",
    )

    args = parser.parse_args()

    return args


def start_run(openstack_obj, logger, dry_run=True):
    node_filter = {
        # "maintenance": True, # leave for now for testing purposes
        "provision_state": "error",
        "associated": False,
    }

    nodes = [x for x in openstack_obj.api.baremetal.nodes(**node_filter)]

    if nodes:
        for node in nodes:
            logger.info(
                "Node {nodename} with instance UUID {instance_uuid} found (state: {provision_state}, maintenance: {maintenance})".format(
                    nodename=node.name,
                    # uuid field is different depending on the python-openstack / python-ironicclient version
                    # uuid=node.uuid,
                    instance_uuid=node.instance_id,
                    provision_state=node.provision_state,
                    maintenance=node.is_maintenance,
                )
            )
            if not dry_run and not node.instance_id:
                logger.info("Cleaning up node {}.".format(node.name))
                # setting deleted is the equivalent of undeploy
                # https://github.com/openstack/python-ironicclient/blob/master/ironicclient/osc/v1/baremetal_node.py#L1439-L1444
                openstack_obj.api.baremetal.set_node_provision_state(node.id, "deleted")
                logger.info("Node cleaned up, waiting 10 seconds before the next one.")
                time.sleep(10)

    else:
        logger.info("No nodes in provision_state error found.")


def main():
    INTERVAL = 60  # run interval in minutes

    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
    )

    logger = logging.getLogger(__name__)

    # does this need to be done in the loop? (can the openstack session time out while the script is still running?)
    openstack_obj = OpenstackHelper(
        os.environ["OS_REGION_NAME"],
        os.environ["OS_USER_DOMAIN_NAME"],
        os.environ["OS_PROJECT_DOMAIN_NAME"],
        os.environ["OS_PROJECT_NAME"],
        os.environ["OS_USERNAME"],
        os.environ["OS_PASSWORD"],
    )

    args = parse_commandline()

    try:
        while True:
            logger.info("Starting new run ===========================================")

            if args.dry_run:
                logger.info("dry-run mode: not doing anything harmful")

            start_run(openstack_obj, logger, args.dry_run)

            logger.info(
                "waiting {} minutes before starting the next loop run".format(INTERVAL)
            )
            time.sleep(60 * INTERVAL)
    except KeyboardInterrupt:
        sys.exit()


if __name__ == "__main__":
    main()
