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
# this script checks for volume attachments of already deleted volumes in the cinder db

import argparse
import configparser
import datetime
import logging
import os
import sys

from openstack import connection, exceptions

from sqlalchemy import and_, MetaData, select, Table, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')


# get all instances from nova
def get_nova_instances(conn):

    nova_instances = dict()

    # get all instance from nova
    try:
        for nova_instance in conn.compute.servers(details=False, all_projects=1):
            nova_instances[nova_instance.id] = nova_instance
        if not nova_instances:
            raise RuntimeError('- PLEASE CHECK MANUALLY - did not get any nova instances back from the nova api - this should in theory never happen ...')

    except exceptions.HttpException as e:
        log.warn("- PLEASE CHECK MANUALLY - got an http exception connecting to openstack: %s", str(e))
        sys.exit(1)

    except exceptions.SDKException as e:
        log.warn("- PLEASE CHECK MANUALLY - got an sdk exception connecting to openstack: %s", str(e))
        sys.exit(1)

    # for i in nova_instances:
    #     print nova_instances[i].id

    if not nova_instances:
        raise RuntimeError('Did not get any nova instances back.')

    return nova_instances


# get all volume attachments for volumes
def get_orphan_volume_attachments(meta):

    orphan_volume_attachments = {}
    orphan_volume_attachment_t = Table('volume_attachment', meta, autoload=True)
    columns = [orphan_volume_attachment_t.c.id, orphan_volume_attachment_t.c.instance_uuid]
    orphan_volume_attachment_q = select(columns=columns, whereclause=and_(orphan_volume_attachment_t.c.deleted == 0))

    # return a dict indexed by orphan_volume_attachment_id and with the value nova_instance_uuid for non deleted orphan_volume_attachments
    for (orphan_volume_attachment_id, nova_instance_uuid) in orphan_volume_attachment_q.execute():
        orphan_volume_attachments[orphan_volume_attachment_id] = nova_instance_uuid

    return orphan_volume_attachments


# get all the volume attachments in the cinder db for already deleted instances in nova
def get_wrong_orphan_volume_attachments(nova_instances, orphan_volume_attachments):

    wrong_orphan_volume_attachments = {}

    for orphan_volume_attachment_id in orphan_volume_attachments:
        if nova_instances.get(orphan_volume_attachments[orphan_volume_attachment_id]) is None:
            wrong_orphan_volume_attachments[orphan_volume_attachment_id] = orphan_volume_attachments[orphan_volume_attachment_id]

    return wrong_orphan_volume_attachments


# delete volume attachments in the cinder db for already deleted instances in nova
def fix_wrong_orphan_volume_attachments(meta, wrong_orphan_volume_attachments, fix_limit):

    if len(wrong_orphan_volume_attachments) <= int(fix_limit):

        orphan_volume_attachment_t = Table('volume_attachment', meta, autoload=True)

        for orphan_volume_attachment_id in wrong_orphan_volume_attachments:
            log.info("-- action: deleting orphan volume attachment id: %s", orphan_volume_attachment_id)
            now = datetime.datetime.utcnow()
            delete_orphan_volume_attachment_q = orphan_volume_attachment_t.update().\
                where(orphan_volume_attachment_t.c.id == orphan_volume_attachment_id).values(updated_at=now, deleted_at=now, deleted=1)
            delete_orphan_volume_attachment_q.execute()

    else:
        log.warn("- PLEASE CHECK MANUALLY - too many (more than %s) wrong orphan volume attachments - denying to fix them automatically", str(fix_limit))


# get all the volumes in state "error_deleting"
def get_error_deleting_volumes(meta):

    error_deleting_volumes = []

    volumes_t = Table('volumes', meta, autoload=True)
    error_deleting_volumes_q = select(columns=[volumes_t.c.id]).where(and_(volumes_t.c.status == "error_deleting", volumes_t.c.deleted == 0))

    # convert the query result into a list
    for i in error_deleting_volumes_q.execute():
        error_deleting_volumes.append(i[0])

    return error_deleting_volumes


# delete all the volumes in state "error_deleting"
def fix_error_deleting_volumes(meta, error_deleting_volumes):

    volumes_t = Table('volumes', meta, autoload=True)
    volume_attachment_t = Table('volume_attachment', meta, autoload=True)
    volume_metadata_t = Table('volume_metadata', meta, autoload=True)
    volume_admin_metadata_t = Table('volume_admin_metadata', meta, autoload=True)

    for error_deleting_volumes_id in error_deleting_volumes:
        now = datetime.datetime.utcnow()
        log.info("-- action: deleting possible volume admin metadata for volume id: %s", error_deleting_volumes_id)
        delete_volume_admin_metadata_q = volume_admin_metadata_t.update().\
            where(volume_admin_metadata_t.c.volume_id == error_deleting_volumes_id).values(updated_at=now, deleted_at=now, deleted=1)
        delete_volume_admin_metadata_q.execute()
        log.info("-- action: deleting possible volume metadata for volume id: %s", error_deleting_volumes_id)
        delete_volume_metadata_q = volume_metadata_t.update().\
            where(volume_metadata_t.c.volume_id == error_deleting_volumes_id).values(updated_at=now, deleted_at=now, deleted=1)
        delete_volume_metadata_q.execute()
        log.info("-- action: deleting possible volume attachments for volume id: %s", error_deleting_volumes_id)
        delete_volume_attachment_q = volume_attachment_t.update().\
            where(volume_attachment_t.c.volume_id == error_deleting_volumes_id).values(updated_at=now, deleted_at=now, deleted=1)
        delete_volume_attachment_q.execute()
        log.info("-- action: deleting volume id: %s", error_deleting_volumes_id)
        delete_volume_q = volumes_t.update().\
            where(volumes_t.c.id == error_deleting_volumes_id).values(updated_at=now, deleted_at=now, deleted=1)
        delete_volume_q.execute()


# get all the snapshots in state "error_deleting"
def get_error_deleting_snapshots(meta):

    error_deleting_snapshots = []

    snapshots_t = Table('snapshots', meta, autoload=True)
    error_deleting_snapshots_q = select(columns=[snapshots_t.c.id]).where(and_(snapshots_t.c.status == "error_deleting", snapshots_t.c.deleted == 0))

    # convert the query result into a list
    for i in error_deleting_snapshots_q.execute():
        error_deleting_snapshots.append(i[0])

    return error_deleting_snapshots


# delete all the snapshots in state "error_deleting"
def fix_error_deleting_snapshots(meta, error_deleting_snapshots):

    snapshots_t = Table('snapshots', meta, autoload=True)

    for error_deleting_snapshots_id in error_deleting_snapshots:
        log.info("-- action: deleting snapshot id: %s", error_deleting_snapshots_id)
        now = datetime.datetime.utcnow()
        delete_snapshot_q = snapshots_t.update().\
            where(snapshots_t.c.id == error_deleting_snapshots_id).values(updated_at=now, deleted_at=now, deleted=1)
        delete_snapshot_q.execute()


# get all the rows with a volume_admin_metadata still defined where the corresponding volume is already deleted
def get_wrong_volume_admin_metadata(meta):

    wrong_admin_metadata = {}
    volume_admin_metadata_t = Table('volume_admin_metadata', meta, autoload=True)
    volumes_t = Table('volumes', meta, autoload=True)
    admin_metadata_join = volume_admin_metadata_t.join(volumes_t, volume_admin_metadata_t.c.volume_id == volumes_t.c.id)
    columns = [volumes_t.c.id, volumes_t.c.deleted, volume_admin_metadata_t.c.id, volume_admin_metadata_t.c.deleted]
    wrong_volume_admin_metadata_q = select(columns=columns).select_from(admin_metadata_join).\
        where(and_(volumes_t.c.deleted == 1, volume_admin_metadata_t.c.deleted == 0))

    # return a dict indexed by volume_admin_metadata_id and with the value volume_id for non deleted volume_admin_metadata
    for (volume_id, volume_deleted, volume_admin_metadata_id, volume_admin_metadata_deleted) in wrong_volume_admin_metadata_q.execute():
        wrong_admin_metadata[volume_admin_metadata_id] = volume_id
    return wrong_admin_metadata


# delete volume_admin_metadata still defined where the corresponding volume is already deleted
def fix_wrong_volume_admin_metadata(meta, wrong_admin_metadata):

    volume_admin_metadata_t = Table('volume_admin_metadata', meta, autoload=True)

    for volume_admin_metadata_id in wrong_admin_metadata:
        log.info("-- action: deleting volume_admin_metadata id: %s", volume_admin_metadata_id)
        now = datetime.datetime.utcnow()
        delete_volume_admin_metadata_q = volume_admin_metadata_t.update().\
            where(volume_admin_metadata_t.c.id == volume_admin_metadata_id).values(updated_at=now, deleted_at=now, deleted=1)
        delete_volume_admin_metadata_q.execute()


# get all the rows with a volume_glance_metadata still defined where the corresponding volume is already deleted
def get_wrong_volume_glance_metadata_volumes(meta):

    wrong_glance_metadata = {}
    volume_glance_metadata_t = Table('volume_glance_metadata', meta, autoload=True)
    volumes_t = Table('volumes', meta, autoload=True)
    glance_metadata_join = volume_glance_metadata_t.join(volumes_t, volume_glance_metadata_t.c.volume_id == volumes_t.c.id)
    columns = [volumes_t.c.id, volumes_t.c.deleted, volume_glance_metadata_t.c.id, volume_glance_metadata_t.c.deleted]
    wrong_volume_glance_metadata_q = select(columns=columns).select_from(glance_metadata_join).\
        where(and_(volumes_t.c.deleted == 1, volume_glance_metadata_t.c.deleted == 0))

    # return a dict indexed by volume_glance_metadata_id and with the value volume_id for non deleted volume_glance_metadata
    for (volume_id, volume_deleted, volume_glance_metadata_id, volume_glance_metadata_deleted) in wrong_volume_glance_metadata_q.execute():
        wrong_glance_metadata[volume_glance_metadata_id] = volume_id
    return wrong_glance_metadata


# delete volume_glance_metadata still defined where the corresponding volume is already deleted
def fix_wrong_volume_glance_metadata_volumes(meta, wrong_glance_metadata):

    volume_glance_metadata_t = Table('volume_glance_metadata', meta, autoload=True)

    for volume_glance_metadata_id in wrong_glance_metadata:
        log.info("-- action: deleting volume_glance_metadata id (volume): %s", volume_glance_metadata_id)
        now = datetime.datetime.utcnow()
        delete_volume_glance_metadata_q = volume_glance_metadata_t.update().\
            where(volume_glance_metadata_t.c.id == volume_glance_metadata_id).values(updated_at=now, deleted_at=now, deleted=1)
        delete_volume_glance_metadata_q.execute()


# get all the rows with a volume_glance_metadata still defined where the corresponding snapshot is already deleted
def get_wrong_volume_glance_metadata_snapshots(meta):

    wrong_glance_metadata = {}
    volume_glance_metadata_t = Table('volume_glance_metadata', meta, autoload=True)
    snapshots_t = Table('snapshots', meta, autoload=True)
    glance_metadata_join = volume_glance_metadata_t.join(snapshots_t, volume_glance_metadata_t.c.snapshot_id == snapshots_t.c.id)
    columns = [snapshots_t.c.id, snapshots_t.c.deleted, volume_glance_metadata_t.c.id, volume_glance_metadata_t.c.deleted]
    wrong_volume_glance_metadata_q = select(columns=columns).select_from(glance_metadata_join).\
        where(and_(snapshots_t.c.deleted == 1, volume_glance_metadata_t.c.deleted == 0))

    # return a dict indexed by volume_glance_metadata_id and with the value volume_id for non deleted volume_glance_metadata
    for (snapshot_id, snapshot_deleted, volume_glance_metadata_id, volume_glance_metadata_deleted) in wrong_volume_glance_metadata_q.execute():
        wrong_glance_metadata[volume_glance_metadata_id] = snapshot_id
    return wrong_glance_metadata


# delete volume_glance_metadata still defined where the corresponding volume is snapshot deleted
def fix_wrong_volume_glance_metadata_snapshots(meta, wrong_glance_metadata):

    volume_glance_metadata_t = Table('volume_glance_metadata', meta, autoload=True)

    for volume_glance_metadata_id in wrong_glance_metadata:
        log.info("-- action: deleting volume_glance_metadata id (snapshot): %s", volume_glance_metadata_id)
        now = datetime.datetime.utcnow()
        delete_volume_glance_metadata_q = volume_glance_metadata_t.update().\
            where(volume_glance_metadata_t.c.id == volume_glance_metadata_id).values(updated_at=now, deleted_at=now, deleted=1)
        delete_volume_glance_metadata_q.execute()


# get all the rows with a volume_metadata still defined where the corresponding volume is already deleted
def get_wrong_volume_metadata(meta):

    wrong_metadata = {}
    volume_metadata_t = Table('volume_metadata', meta, autoload=True)
    volumes_t = Table('volumes', meta, autoload=True)
    metadata_join = volume_metadata_t.join(volumes_t, volume_metadata_t.c.volume_id == volumes_t.c.id)
    columns = [volumes_t.c.id, volumes_t.c.deleted, volume_metadata_t.c.id, volume_metadata_t.c.deleted]
    wrong_volume_metadata_q = select(columns=columns).select_from(metadata_join).\
        where(and_(volumes_t.c.deleted == 1, volume_metadata_t.c.deleted == 0))

    # return a dict indexed by volume_metadata_id and with the value volume_id for non deleted volume_metadata
    for (volume_id, volume_deleted, volume_metadata_id, volume_metadata_deleted) in wrong_volume_metadata_q.execute():
        wrong_metadata[volume_metadata_id] = volume_id
    return wrong_metadata


# delete volume_metadata still defined where the corresponding volume is already deleted
def fix_wrong_volume_metadata(meta, wrong_metadata):

    volume_metadata_t = Table('volume_metadata', meta, autoload=True)

    for volume_metadata_id in wrong_metadata:
        log.info("-- action: deleting volume_metadata id: %s", volume_metadata_id)
        now = datetime.datetime.utcnow()
        delete_volume_metadata_q = volume_metadata_t.update().\
            where(volume_metadata_t.c.id == volume_metadata_id).values(updated_at=now, deleted_at=now, deleted=1)
        delete_volume_metadata_q.execute()


# get all the rows with a volume attachment still defined where the corresponding volume is already deleted
def get_wrong_volume_attachments(meta):

    wrong_attachments = {}
    volume_attachment_t = Table('volume_attachment', meta, autoload=True)
    volumes_t = Table('volumes', meta, autoload=True)
    attachment_join = volume_attachment_t.join(volumes_t, volume_attachment_t.c.volume_id == volumes_t.c.id)
    columns = [volumes_t.c.id, volumes_t.c.deleted, volume_attachment_t.c.id, volume_attachment_t.c.deleted]
    wrong_volume_attachment_q = select(columns=columns).select_from(attachment_join).\
        where(and_(volumes_t.c.deleted == 1, volume_attachment_t.c.deleted == 0))

    # return a dict indexed by volume_attachment_id and with the value volume_id for non deleted volume_attachments
    for (volume_id, volume_deleted, volume_attachment_id, volume_attachment_deleted) in wrong_volume_attachment_q.execute():
        wrong_attachments[volume_attachment_id] = volume_id
    return wrong_attachments


# delete volume attachment still defined where the corresponding volume is already deleted
def fix_wrong_volume_attachments(meta, wrong_attachments, fix_limit):

    if len(wrong_attachments) <= int(fix_limit):

        volume_attachment_t = Table('volume_attachment', meta, autoload=True)

        for volume_attachment_id in wrong_attachments:
            log.info("-- action: deleting volume attachment id: %s", volume_attachment_id)
            now = datetime.datetime.utcnow()
            delete_volume_attachment_q = volume_attachment_t.update().\
                where(volume_attachment_t.c.id == volume_attachment_id).values(updated_at=now, deleted_at=now, deleted=1)
            delete_volume_attachment_q.execute()

    else:
        log.warn("- PLEASE CHECK MANUALLY - too many (more than %s) wrong volume attachments - denying to fix them automatically", str(fix_limit))


# get all the rows with a snapshot_metadata still defined where the corresponding snapshot is already deleted
def get_wrong_snapshot_metadata(meta):

    wrong_metadata = {}
    snapshot_metadata_t = Table('snapshot_metadata', meta, autoload=True)
    snapshots_t = Table('snapshots', meta, autoload=True)
    metadata_join = snapshot_metadata_t.join(snapshots_t, snapshot_metadata_t.c.snapshot_id == snapshots_t.c.id)
    columns = [snapshots_t.c.id, snapshots_t.c.deleted, snapshot_metadata_t.c.id, snapshot_metadata_t.c.deleted]
    wrong_snapshot_metadata_q = select(columns=columns).select_from(metadata_join).\
        where(and_(snapshots_t.c.deleted == 1, snapshot_metadata_t.c.deleted == 0))

    # return a dict indexed by snapshot_metadata_id and with the value snapshot_id for non deleted snapshot_metadata
    for (snapshot_id, snapshot_deleted, snapshot_metadata_id, snapshot_metadata_deleted) in wrong_snapshot_metadata_q.execute():
        wrong_metadata[snapshot_metadata_id] = snapshot_id
    return wrong_metadata


# delete snapshot_metadata still defined where the corresponding snapshot is already deleted
def fix_wrong_snapshot_metadata(meta, wrong_metadata):

    snapshot_metadata_t = Table('snapshot_metadata', meta, autoload=True)

    for snapshot_metadata_id in wrong_metadata:
        log.info("-- action: deleting snapshot_metadata id: %s", snapshot_metadata_id)
        now = datetime.datetime.utcnow()
        delete_snapshot_metadata_q = snapshot_metadata_t.update().\
            where(snapshot_metadata_t.c.id == snapshot_metadata_id).values(updated_at=now, deleted_at=now, deleted=1)
        delete_snapshot_metadata_q.execute()


# get all the rows with a group_volume_type_mapping still defined where the corresponding group_id is already deleted
def get_wrong_group_volume_type_mappings(meta):

    wrong_group_volume_type_mappings = {}
    group_volume_type_mapping_t = Table('group_volume_type_mapping', meta, autoload=True)
    groups_t = Table('groups', meta, autoload=True)
    group_volume_type_mapping_join = group_volume_type_mapping_t.join(groups_t, group_volume_type_mapping_t.c.group_id == groups_t.c.id)
    columns = [groups_t.c.id, groups_t.c.deleted, group_volume_type_mapping_t.c.id, group_volume_type_mapping_t.c.deleted]
    wrong_group_volume_type_mapping_q = select(columns=columns).select_from(group_volume_type_mapping_join).\
        where(and_(groups_t.c.deleted == 1, group_volume_type_mapping_t.c.deleted == 0))

    # return a dict indexed by volume_attachment_id and with the value volume_id for non deleted volume_attachments
    for (group_id, group_deleted, group_volume_type_mapping_id, group_volume_type_mapping_deleted) in wrong_group_volume_type_mapping_q.execute():
        wrong_group_volume_type_mappings[group_volume_type_mapping_id] = group_id
    return wrong_group_volume_type_mappings


# delete group_volume_type_mapping still defined where the corresponding groupid is already deleted
def fix_wrong_group_volume_type_mappings(meta, wrong_group_volume_type_mappings, fix_limit):

    if len(wrong_group_volume_type_mappings) <= int(fix_limit):

        group_volume_type_mapping_t = Table('group_volume_type_mapping', meta, autoload=True)

        for group_volume_type_mapping_id in wrong_group_volume_type_mappings:
            log.info("-- action: deleting group_volume_type_mapping id: %s", group_volume_type_mapping_id)
            now = datetime.datetime.utcnow()
            delete_group_volume_type_mapping_q = group_volume_type_mapping_t.update().\
                where(group_volume_type_mapping_t.c.id == group_volume_type_mapping_id).values(updated_at=now, deleted_at=now, deleted=1)
            delete_group_volume_type_mapping_q.execute()

    else:
        log.warn("- PLEASE CHECK MANUALLY - too many (more than %s) wrong group_volume_type_mappings - denying to fix them automatically", str(fix_limit))


# get all the rows, which have the deleted flag set, but not the delete_at column
def get_missing_deleted_at(meta, table_names):

    missing_deleted_at = {}
    for t in table_names:
        a_table_t = Table(t, meta, autoload=True)
        a_table_select_deleted_at_q = a_table_t.select().where(
            and_(a_table_t.c.deleted == 1, a_table_t.c.deleted_at is None))

        for row in a_table_select_deleted_at_q.execute():
            missing_deleted_at[row.id] = t
    return missing_deleted_at


# set deleted_at to updated_at value if not set for marked as deleted rows
def fix_missing_deleted_at(meta, table_names):
    now = datetime.datetime.utcnow()
    for t in table_names:
        a_table_t = Table(t, meta, autoload=True)

        log.info("- action: fixing columns with missing deleted_at times in the %s table", t)
        a_table_set_deleted_at_q = a_table_t.update().where(
            and_(a_table_t.c.deleted == 1, a_table_t.c.deleted_at is None)).values(
            deleted_at=now)
        a_table_set_deleted_at_q.execute()


# get all the rows with a service still defined where the corresponding volume is already deleted
def get_deleted_services_still_used_in_volumes(meta):

    deleted_services_still_used_in_volumes = {}
    services_t = Table('services', meta, autoload=True)
    volumes_t = Table('volumes', meta, autoload=True)
    services_volumes_join = services_t.join(volumes_t, services_t.c.uuid == volumes_t.c.service_uuid)
    columns = [services_t.c.uuid, services_t.c.deleted, volumes_t.c.id, volumes_t.c.deleted]
    deleted_services_still_used_in_volumes_q = select(columns=columns).select_from(services_volumes_join).\
        where(and_(volumes_t.c.deleted == 0, services_t.c.deleted == 1))

    # return a dict indexed by service_uuid and with the value volume_id for deleted but still referenced services
    for (service_uuid, service_deleted, volume_id, volume_deleted) in deleted_services_still_used_in_volumes_q.execute():
        deleted_services_still_used_in_volumes[service_uuid] = volume_id
    return deleted_services_still_used_in_volumes


# delete services still defined where the corresponding volume is already deleted
def fix_deleted_services_still_used_in_volumes(meta, deleted_services_still_used_in_volumes):

    services_t = Table('services', meta, autoload=True)

    for deleted_services_still_used_in_volumes_id in deleted_services_still_used_in_volumes:
        log.info("-- action: undeleting service uuid: %s", deleted_services_still_used_in_volumes_id)
        undelete_services_q = services_t.update().where(services_t.c.uuid == deleted_services_still_used_in_volumes_id).values(deleted=0,deleted_at=None)
        undelete_services_q.execute()


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
        log.warn("- PLEASE CHECK MANUALLY - problems connecting to openstack: %s", str(e))
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
    parser = configparser.ConfigParser()
    try:
        parser.read(config_file)
        db_url = parser.get('database', 'connection', raw=True)
    except Exception as e:
        log.info("ERROR: Check Cinder configuration file - error %s", str(e))
        sys.exit(2)
    return db_url


# cmdline handling
def parse_cmdline_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",
                        default='./cinder.conf',
                        help='configuration file')
    parser.add_argument("--dry-run", action="store_true", help='print only what would be done without actually doing it')
    parser.add_argument("--fix-limit", default=25, help='maximum number of inconsistencies to fix automatically - if there are more, automatic fixing is denied')
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
    cinder_session, cinder_metadata, cinder_Base = makeConnection(db_url)

    # fixing volume attachments at no longer existing instances
    orphan_volume_attachments = get_orphan_volume_attachments(cinder_metadata)
    nova_instances = get_nova_instances(conn)
    wrong_orphan_volume_attachments = get_wrong_orphan_volume_attachments(nova_instances, orphan_volume_attachments)
    if len(wrong_orphan_volume_attachments) != 0:
        log.info("- orphan volume attachments found:")
        # print out what we would delete
        for orphan_volume_attachment_id in wrong_orphan_volume_attachments:
            log.info("-- orphan volume attachment (id in cinder db: %s) for non existent instance in nova: %s", orphan_volume_attachment_id,
                     orphan_volume_attachments[orphan_volume_attachment_id])
        if not args.dry_run:
            log.info("- deleting orphan volume attachment inconsistencies found")
            fix_wrong_orphan_volume_attachments(cinder_metadata, wrong_orphan_volume_attachments, args.fix_limit)
    else:
        log.info("- no orphan volume attachments found")

    # fixing possible volumes in state "error-deleting"
    error_deleting_volumes = get_error_deleting_volumes(cinder_metadata)
    if len(error_deleting_volumes) != 0:
        log.info("- volumes in state error_deleting found")
        # print out what we would delete
        for error_deleting_volumes_id in error_deleting_volumes:
            log.info("-- volume id: %s", error_deleting_volumes_id)
        if not args.dry_run:
            log.info("- deleting volumes in state error_deleting")
            fix_error_deleting_volumes(cinder_metadata, error_deleting_volumes)
    else:
        log.info("- no volumes in state error_deleting found")

    # fixing possible snapshots in state "error-deleting"
    error_deleting_snapshots = get_error_deleting_snapshots(cinder_metadata)
    if len(error_deleting_snapshots) != 0:
        log.info("- snapshots in state error_deleting found")
        # print out what we would delete
        for error_deleting_snapshots_id in error_deleting_snapshots:
            log.info("-- snapshot id: %s", error_deleting_snapshots_id)
        if not args.dry_run:
            log.info("- deleting snapshots in state error_deleting")
            fix_error_deleting_snapshots(cinder_metadata, error_deleting_snapshots)
    else:
        log.info("- no snapshots in state error_deleting found")

    # fixing possible wrong admin_metadata entries
    wrong_admin_metadata = get_wrong_volume_admin_metadata(cinder_metadata)
    if len(wrong_admin_metadata) != 0:
        log.info("- volume_admin_metadata inconsistencies found")
        # print out what we would delete
        for volume_admin_metadata_id in wrong_admin_metadata:
            log.info("-- volume_admin_metadata id: %s - deleted volume id: %s", volume_admin_metadata_id, wrong_admin_metadata[volume_admin_metadata_id])
        if not args.dry_run:
            log.info("- removing volume_admin_metadata inconsistencies found")
            fix_wrong_volume_admin_metadata(cinder_metadata, wrong_admin_metadata)
    else:
        log.info("- volume_admin_metadata entries are consistent")

    # fixing possible wrong glance_metadata entries for volumes
    wrong_glance_metadata = get_wrong_volume_glance_metadata_volumes(cinder_metadata)
    if len(wrong_glance_metadata) != 0:
        log.info("- volume_glance_metadata inconsistencies for volumes found")
        # print out what we would delete
        for volume_glance_metadata_id in wrong_glance_metadata:
            log.info("-- volume_glance_metadata id: %s - deleted volume id: %s", volume_glance_metadata_id, wrong_glance_metadata[volume_glance_metadata_id])
        if not args.dry_run:
            log.info("- removing volume_glance_metadata inconsistencies found")
            fix_wrong_volume_glance_metadata_volumes(cinder_metadata, wrong_glance_metadata)
    else:
        log.info("- volume_glance_metadata entries for volumes are consistent")

    # fixing possible wrong glance_metadata entries for snapshots
    wrong_glance_metadata = get_wrong_volume_glance_metadata_snapshots(cinder_metadata)
    if len(wrong_glance_metadata) != 0:
        log.info("- volume_glance_metadata inconsistencies for snapshots found")
        # print out what we would delete
        for volume_glance_metadata_id in wrong_glance_metadata:
            log.info("-- volume_glance_metadata id: %s - deleted snapshot id: %s", volume_glance_metadata_id, wrong_glance_metadata[volume_glance_metadata_id])
        if not args.dry_run:
            log.info("- removing volume_glance_metadata inconsistencies found")
            fix_wrong_volume_glance_metadata_snapshots(cinder_metadata, wrong_glance_metadata)
    else:
        log.info("- volume_glance_metadata entries for snapshots are consistent")

    # fixing possible wrong volume metadata entries
    wrong_metadata = get_wrong_volume_metadata(cinder_metadata)
    if len(wrong_metadata) != 0:
        log.info("- volume_metadata inconsistencies found")
        # print out what we would delete
        for volume_metadata_id in wrong_metadata:
            log.info("-- volume_metadata id: %s - deleted volume id: %s", volume_metadata_id, wrong_metadata[volume_metadata_id])
        if not args.dry_run:
            log.info("- removing volume_metadata inconsistencies found")
            fix_wrong_volume_metadata(cinder_metadata, wrong_metadata)
    else:
        log.info("- volume_metadata entries are consistent")

    # fixing possible wrong attachment entries
    wrong_attachments = get_wrong_volume_attachments(cinder_metadata)
    if len(wrong_attachments) != 0:
        log.info("- volume attachment inconsistencies found")
        # print out what we would delete
        for volume_attachment_id in wrong_attachments:
            log.info("-- volume attachment id: %s - deleted volume id: %s", volume_attachment_id, wrong_attachments[volume_attachment_id])
        if not args.dry_run:
            log.info("- removing volume attachment inconsistencies found")
            fix_wrong_volume_attachments(cinder_metadata, wrong_attachments, args.fix_limit)
    else:
        log.info("- volume attachments are consistent")

    # fixing possible wrong snapshot metadata entries
    wrong_metadata = get_wrong_snapshot_metadata(cinder_metadata)
    if len(wrong_metadata) != 0:
        log.info("- snapshot_metadata inconsistencies found")
        # print out what we would delete
        for snapshot_metadata_id in wrong_metadata:
            log.info("-- snapshot_metadata id: %s - deleted snapshot id: %s", snapshot_metadata_id, wrong_metadata[snapshot_metadata_id])
        if not args.dry_run:
            log.info("- removing snapshot_metadata inconsistencies found")
            fix_wrong_snapshot_metadata(cinder_metadata, wrong_metadata)
    else:
        log.info("- snapshot_metadata entries are consistent")

    # fixing possible wrong group_volume_type_mappings entries
    wrong_group_volume_type_mappings = get_wrong_group_volume_type_mappings(cinder_metadata)
    if len(wrong_group_volume_type_mappings) != 0:
        log.info("- group_volume_type_mappings inconsistencies found")
        # print out what we would delete
        for group_volume_type_mapping_id in wrong_group_volume_type_mappings:
            log.info("-- group_volume_type_mapping id: %s - deleted group id: %s", group_volume_type_mapping_id, wrong_group_volume_type_mappings[group_volume_type_mapping_id])
        if not args.dry_run:
            log.info("- removing group_volume_type_mapping inconsistencies found")
            fix_wrong_group_volume_type_mappings(cinder_metadata, wrong_group_volume_type_mappings, args.fix_limit)
    else:
        log.info("- group_volume_type_mappings are consistent")

    # fixing possible missing deleted_at timestamps in some tables
    # tables which sometimes have missing deleted_at values
    table_names = ['snapshots', 'volume_attachment']

    missing_deleted_at = get_missing_deleted_at(cinder_metadata, table_names)
    if len(missing_deleted_at) != 0:
        log.info("- missing deleted_at values found:")
        # print out what we would delete
        for missing_deleted_at_id in missing_deleted_at:
            log.info("--- id %s of the %s table is missing deleted_at time", missing_deleted_at_id, missing_deleted_at[missing_deleted_at_id])
        if not args.dry_run:
            log.info("- setting missing deleted_at values")
            fix_missing_deleted_at(cinder_metadata, table_names)
    else:
        log.info("- no missing deleted_at values")

    deleted_services_still_used_in_volumes = get_deleted_services_still_used_in_volumes(cinder_metadata)
    if len(deleted_services_still_used_in_volumes) != 0:
        log.info("- deleted services still used in volumes found:")
        # print out what we would delete
        for deleted_services_still_used_in_volumes_id in deleted_services_still_used_in_volumes:
            log.info("--- deleted service uuid %s still used in volumes table entry %s", deleted_services_still_used_in_volumes_id, deleted_services_still_used_in_volumes[deleted_services_still_used_in_volumes_id])
        if not args.dry_run:
            log.info("- undeleting service uuid still used in volumes table")
            fix_deleted_services_still_used_in_volumes(cinder_metadata, deleted_services_still_used_in_volumes)
    else:
        log.info("- deleted services still used in volumes")


if __name__ == "__main__":
    main()
