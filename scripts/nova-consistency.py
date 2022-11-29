#!/usr/bin/env python
#
# Copyright (c) 2018 SAP SE
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
# this script checks for block_device_mappings in the nova db for already deleted volumes in cinder

import argparse
import sys
import ConfigParser
import logging
import os
import datetime

from openstack import connection, exceptions, utils

from sqlalchemy import and_
from sqlalchemy import func
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import join
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import false
from sqlalchemy.ext.declarative import declarative_base

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')


# get all volumes from cinder
def get_cinder_volumes(conn):

    cinder_volumes = dict()

    # get all volumes from cinder
    try:
        for cinder_volume in conn.block_store.volumes(details=False, all_projects=1):
            cinder_volumes[cinder_volume.id] = cinder_volume
        if not cinder_volumes:
            raise RuntimeError("- PLEASE CHECK MANUALLY - did not get any cinder volumes back from "
                               "the cinder api - this should in theory never happen ...")

    except exceptions.HttpException as e:
        log.warn("- PLEASE CHECK MANUALLY - got an http exception connecting to openstack: %s", str(e))
        sys.exit(1)

    except exceptions.SDKException as e:
        log.warn("- PLEASE CHECK MANUALLY - got an sdk exception connecting to openstack: %s", str(e))
        sys.exit(1)

    # for i in cinder_volumes:
    #     print cinder_volumes[i].id

    return cinder_volumes


# get all block device mappings for volumes
def get_block_device_mappings(meta):

    block_device_mappings = {}
    block_device_mapping_t = Table('block_device_mapping', meta, autoload=True)
    block_device_mapping_q = select(columns=[block_device_mapping_t.c.id, block_device_mapping_t.c.volume_id],
                                    whereclause=and_(block_device_mapping_t.c.deleted == 0,
                                                     block_device_mapping_t.c.volume_id.isnot(None),
                                                     block_device_mapping_t.c.destination_type == "volume"))

    # return a dict indexed by block_device_mapping_id and with the value
    # cinder_volume_id for non deleted block_device_mappings
    for (block_device_mapping_id, cinder_volume_id) in block_device_mapping_q.execute():
        block_device_mappings[block_device_mapping_id] = cinder_volume_id

    return block_device_mappings


# get all the block_device_mappings in the nova db for already deleted volumes in cinder
def get_wrong_block_device_mappings(cinder_volumes, block_device_mappings):

    wrong_block_device_mappings = {}

    for block_device_mapping_id in block_device_mappings:
        if cinder_volumes.get(block_device_mappings[block_device_mapping_id]) is None:
            wrong_block_device_mappings[block_device_mapping_id] = block_device_mappings[block_device_mapping_id]

    return wrong_block_device_mappings


# delete block_device_mappings in the nova db for already deleted volumes in cinder
def fix_wrong_block_device_mappings(meta, wrong_block_device_mappings, fix_limit):

    if len(wrong_block_device_mappings) <= int(fix_limit):
        block_device_mapping_t = Table('block_device_mapping', meta, autoload=True)

        for block_device_mapping_id in wrong_block_device_mappings:
            log.info("-- action: deleting block device mapping id: %s", block_device_mapping_id)
            now = datetime.datetime.utcnow()
            delete_block_device_mapping_q = block_device_mapping_t.update().\
                where(block_device_mapping_t.c.id == block_device_mapping_id).\
                values(updated_at=now, deleted_at=now,
                       deleted=block_device_mapping_id)
            delete_block_device_mapping_q.execute()
    else:
        log.warn("- PLEASE CHECK MANUALLY - too many (more than %s) wrong block device mappings - "
                 "denying to fix them automatically", str(fix_limit))


# delete block_device_mappings in the nova db with the deleted flag set and older than a certain time
# looks like the nova db purge does not clean those up properly
def purge_block_device_mappings(meta, older_than):

    block_device_mapping_t = Table('block_device_mapping', meta, autoload=True)

    log.info("- action: purging deleted block device mappings older than %s days", older_than)
    older_than_date = datetime.datetime.utcnow() - datetime.timedelta(days=older_than)
    purge_block_device_mapping_q = block_device_mapping_t.delete().\
        where(and_(block_device_mapping_t.c.deleted != 0,
                   block_device_mapping_t.c.deleted_at < older_than_date))
    purge_block_device_mapping_q.execute()


# delete reservations in the nova db with the deleted flag set and older than a certain time
# looks like the nova db purge does not clean those up properly
def purge_reservations(meta, older_than):

    reservations_t = Table('reservations', meta, autoload=True)

    log.info("- action: purging deleted reservations older than %s days", older_than)
    older_than_date = datetime.datetime.utcnow() - datetime.timedelta(days=older_than)
    purge_reservations_q = reservations_t.delete().\
        where(and_(reservations_t.c.deleted != 0, reservations_t.c.deleted_at < older_than_date))
    purge_reservations_q.execute()


# delete instance_id_mappings in the nova db with the deleted flag set and older than a certain time
# looks like the nova db purge does not clean those up properly
def purge_instance_id_mappings(meta, older_than):

    instance_id_mappings_t = Table('instance_id_mappings', meta, autoload=True)

    log.info("- action: purging deleted instance_id_mappings older than %s days", older_than)
    older_than_date = datetime.datetime.utcnow() - datetime.timedelta(days=older_than)
    purge_instance_id_mappings_q = instance_id_mappings_t.delete().\
        where(and_(instance_id_mappings_t.c.deleted != 0, instance_id_mappings_t.c.deleted_at < older_than_date))
    purge_instance_id_mappings_q.execute()


# it looks like this is not required as the purging is already done properly via the instance purging
# # delete instance_system_metadata in the nova db with the deleted flag set and older than a certain time
# # looks like the nova db purge does not clean those up properly
# def purge_instance_system_metadata(meta, older_than):
#
#     instance_system_metadata_t = Table('instance_system_metadata', meta, autoload=True)
#
#     log.info("- action: purging deleted instance_system_metadata older than %s days", older_than)
#     older_than_date = datetime.datetime.utcnow() - datetime.timedelta(days=older_than)
#     purge_instance_system_metadata_q = instance_system_metadata_t.delete().\
#         where(and_(instance_system_metadata_t.c.
#                    deleted != 0, instance_system_metadata_t.c.deleted_at < older_than_date))
#     purge_instance_system_metadata_q.execute()


# delete old instance fault entries in the nova db
def purge_instance_faults(session, meta, max_instance_faults):

    instance_faults_t = Table('instance_faults', meta, autoload=True)

    log.info("- purging instance faults to at maximum %s per instance", max_instance_faults)
    # get the max_instance_faults latest oinstance fault entries per instance and delete all others
    subquery = session.query(
        instance_faults_t,
        func.dense_rank().over(
            order_by=instance_faults_t.c.created_at.desc(),
            partition_by=instance_faults_t.c.instance_uuid
        ).label('rank')
    ).subquery()
    for i in session.query(subquery).filter(subquery.c.rank > max_instance_faults).all():
        log.info("- action: deleting instance fault entry for instance %s from %s", i.instance_uuid, str(i.created_at))
        purge_instance_faults_q = instance_faults_t.delete().where(instance_faults_t.c.id == i.id)
        purge_instance_faults_q.execute()


# establish an openstack connection
def makeOsConnection():
    try:
        conn = connection.Connection(auth_url=os.getenv('OS_AUTH_URL'),
                                     project_name=os.getenv('OS_PROJECT_NAME'),
                                     project_domain_name=os.getenv('OS_PROJECT_DOMAIN_NAME'),
                                     username=os.getenv('OS_USERNAME'),
                                     user_domain_name=os.getenv('OS_USER_DOMAIN_NAME'),
                                     password=os.getenv('OS_PASSWORD'),
                                     identity_api_version="3")
    except Exception as e:
        log.warn("- PLEASE CHECK MANUALLY - problems connecting to openstack: %s",
                 str(e))
        sys.exit(1)

    return conn


# establish a database connection and return the handle
def makeConnection(db_url):

    engine = create_engine(db_url)
    engine.connect()
    Session = sessionmaker(bind=engine)
    thisSession = Session()
    metadata = MetaData()
    metadata.bind = engine
    Base = declarative_base()
    return thisSession, metadata, Base


# return the database connection string from the config file
def get_db_url(config_file):

    parser = ConfigParser.SafeConfigParser()
    try:
        parser.read(config_file)
        db_url = parser.get('database', 'connection', raw=True)
    except Exception:
        log.info("ERROR: Check Nova configuration file.")
        sys.exit(2)
    return db_url


# cmdline handling
def parse_cmdline_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",
                        default='./nova.conf',
                        help='configuration file')
    parser.add_argument("--dry-run",
                        action="store_true",
                        help="print only what would be done without actually doing it")
    parser.add_argument("--older-than",
                        type=int,
                        help="how many days of marked as deleted entries to keep")
    parser.add_argument("--max-instance-faults",
                        type=int,
                        help="how many instance faults entries to keep")
    parser.add_argument("--fix-limit",
                        default=25,
                        help="maximum number of inconsistencies to fix automatically - if there are more, "
                        "automatic fixing is denied")
    return parser.parse_args()


def main():
    try:
        args = parse_cmdline_args()
    except Exception as e:
        log.error("Check command line arguments (%s)", e.strerror)

    # connect to openstack
    conn = makeOsConnection()

    # connect to the DB
    db_url = get_db_url(args.config)
    nova_session, nova_metadata, nova_Base = makeConnection(db_url)

    if args.older_than and not args.dry_run:
        purge_block_device_mappings(nova_metadata, args.older_than)
        purge_reservations(nova_metadata, args.older_than)
        purge_instance_id_mappings(nova_metadata, args.older_than)
        # purge_instance_system_metadata(nova_metadata, args.older_than)
    if args.max_instance_faults and not args.dry_run:
        purge_instance_faults(nova_session, nova_metadata, args.max_instance_faults)
    block_device_mappings = get_block_device_mappings(nova_metadata)
    cinder_volumes = get_cinder_volumes(conn)
    wrong_block_device_mappings = get_wrong_block_device_mappings(cinder_volumes, block_device_mappings)
    if len(wrong_block_device_mappings) != 0:
        log.info("- block device mapping inconsistencies found:")
        # print out what we would delete
        for block_device_mapping_id in wrong_block_device_mappings:
            log.info("-- block device mapping (id in nova db: %s) for non existent volume in cinder: %s",
                     block_device_mapping_id,
                     block_device_mappings[block_device_mapping_id])
        if not args.dry_run:
            log.info("- deleting block device mapping inconsistencies found")
            fix_wrong_block_device_mappings(nova_metadata, wrong_block_device_mappings, args.fix_limit)
    else:
        log.info("- block device mappings are consistent")


if __name__ == "__main__":
    main()
