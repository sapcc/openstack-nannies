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
# this script checks for inconsistencies in the manila db

import argparse
import sys
import ConfigParser
import logging
import datetime

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

# get all the rows with a share_network_security_service_association still defined where the corresponding share_network is already deleted
def get_wrong_share_network_ssas(meta):

    wrong_share_network_ssas = {}
    share_network_ssa_t = Table('share_network_security_service_association', meta, autoload=True)
    share_networks_t = Table('share_networks', meta, autoload=True)
    share_network_ssa_join = share_network_ssa_t.join(share_networks_t,share_network_ssa_t.c.share_network_id == share_networks_t.c.id)
    wrong_share_network_ssa_q = select(columns=[share_networks_t.c.id,share_networks_t.c.deleted,share_network_ssa_t.c.id,share_network_ssa_t.c.deleted]).select_from(share_network_ssa_join).where(and_(share_networks_t.c.deleted != "False",share_network_ssa_t.c.deleted == 0))

    # return a dict indexed by share_network_security_service_association id and with the value share_network_id for non deleted ssas
    for (share_network_id, share_network_deleted, share_network_ssa_id, share_network_ssa_deleted) in wrong_share_network_ssa_q.execute():
        wrong_share_network_ssas[share_network_ssa_id] = share_network_id
    return wrong_share_network_ssas

# delete share_network_security_service_association still defined where the corresponding share_network is already deleted
def fix_wrong_share_network_ssas(meta, wrong_share_network_ssas):

    share_network_ssa_t = Table('share_network_security_service_association', meta, autoload=True)

    now = datetime.datetime.utcnow()
    for share_network_ssa_id in wrong_share_network_ssas:
        log.info ("-- action: deleting share network security service association id: %s", share_network_ssa_id)
        delete_share_network_ssa_q = share_network_ssa_t.update().\
            where(share_network_ssa_t.c.id == share_network_ssa_id).\
                values(updated_at=now, deleted_at=now, deleted=share_network_ssa_id)
        delete_share_network_ssa_q.execute()

# get all the rows with a network_allocations still defined where the corresponding share_server is already deleted
def get_wrong_network_allocations(meta):

    wrong_network_allocations = {}
    network_allocations_t = Table('network_allocations', meta, autoload=True)
    share_servers_t = Table('share_servers', meta, autoload=True)
    network_allocations_join = network_allocations_t.join(share_servers_t,network_allocations_t.c.share_server_id == share_servers_t.c.id)
    wrong_network_allocations_q = select(columns=[share_servers_t.c.id,share_servers_t.c.deleted,network_allocations_t.c.id,network_allocations_t.c.deleted]).select_from(network_allocations_join).where(and_(share_servers_t.c.deleted != "False",network_allocations_t.c.deleted == "False"))

    # return a dict indexed by share_network_security_service_association id and with the value share_server_id for non deleted ssas
    for (share_server_id, share_network_deleted, network_allocations_id, network_allocations_deleted) in wrong_network_allocations_q.execute():
        wrong_network_allocations[network_allocations_id] = share_server_id
    return wrong_network_allocations

# delete network_allocations still defined where the corresponding share_server is already deleted
def fix_wrong_network_allocations(meta, wrong_network_allocations):
    network_allocations_t = Table('network_allocations', meta, autoload=True)

    now = datetime.datetime.utcnow()
    for network_allocations_id in wrong_network_allocations:
        log.info ("-- action: deleting network allocation id: %s", network_allocations_id)
        delete_network_allocations_q = network_allocations_t.update().\
            where(network_allocations_t.c.id == network_allocations_id).\
                values(updated_at=now, deleted_at=now, deleted=network_allocations_id)
        delete_network_allocations_q.execute()

# get all the rows with a share_metadata still defined where the corresponding share is already deleted
def get_wrong_share_metadata(meta):

    wrong_share_metadata = {}
    share_metadata_t = Table('share_metadata', meta, autoload=True)
    shares_t = Table('shares', meta, autoload=True)
    share_metadata_join = share_metadata_t.join(shares_t,share_metadata_t.c.share_id == shares_t.c.id)
    wrong_share_metadata_q = select(columns=[shares_t.c.id,shares_t.c.deleted,share_metadata_t.c.id,share_metadata_t.c.deleted]).select_from(share_metadata_join).where(and_(shares_t.c.deleted != "False",share_metadata_t.c.deleted == 0))

    # return a dict indexed by share_network_security_service_association id and with the value share_id for non deleted ssas
    for (share_id, share_deleted, share_metadata_id, share_metadata_deleted) in wrong_share_metadata_q.execute():
        wrong_share_metadata[share_metadata_id] = share_id
    return wrong_share_metadata

# delete share_metadata still defined where the corresponding share is already deleted
def fix_wrong_share_metadata(meta, wrong_share_metadata):
    share_metadata_t = Table('share_metadata', meta, autoload=True)

    now = datetime.datetime.utcnow()
    for share_metadata_id in wrong_share_metadata:
        log.info ("-- action: deleting share metadata id: %s", share_metadata_id)
        delete_share_metadata_q = share_metadata_t.update().\
            where(share_metadata_t.c.id == share_metadata_id).\
                values(updated_at=now, deleted_at=now, deleted=share_metadata_id)
        delete_share_metadata_q.execute()

# get all the rows with a share_group_type_share_type_mapping still defined where the corresponding share_group_type is already deleted
def get_wrong_share_gtstm(meta):

    wrong_share_gtstm = {}
    share_gtstm_t = Table('share_group_type_share_type_mappings', meta, autoload=True)
    share_group_types_t = Table('share_group_types', meta, autoload=True)
    share_gtstm_join = share_gtstm_t.join(share_group_types_t,share_gtstm_t.c.share_group_type_id == share_group_types_t.c.id)
    wrong_share_gtstm_q = select(columns=[share_group_types_t.c.id,share_group_types_t.c.deleted,share_gtstm_t.c.id,share_gtstm_t.c.deleted]).select_from(share_gtstm_join).where(and_(share_group_types_t.c.deleted != "False",share_gtstm_t.c.deleted == "False"))

    # return a dict indexed by share_network_security_service_association id and with the value share_id for non deleted ssas
    for (share_group_type_id, share_group_type_deleted, share_gtstm_id, share_gtstm_deleted) in wrong_share_gtstm_q.execute():
        wrong_share_gtstm[share_gtstm_id] = share_group_type_id
    return wrong_share_gtstm

# delete share_group_type_share_type_mapping still defined where the corresponding share_group_type is already deleted
def fix_wrong_share_gtstm(meta, wrong_share_gtstm):
    share_gtstm_t = Table('share_group_type_share_type_mappings', meta, autoload=True)

    now = datetime.datetime.utcnow()
    for share_gtstm_id in wrong_share_gtstm:
        log.info ("-- action: deleting share group type id: %s", share_gtstm_id)
        delete_share_gtstm_q = share_gtstm_t.update().\
            where(share_gtstm_t.c.id == share_gtstm_id).\
                values(updated_at=now, deleted_at=now, deleted=share_gtstm_id)
        delete_share_gtstm_q.execute()

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
    except:
        log.info("ERROR: Check Manila configuration file.")
        sys.exit(2)
    return db_url

# cmdline handling
def parse_cmdline_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",
                        default='./manila.conf',
                        help='configuration file')
    parser.add_argument("--dry-run",
                       action="store_true",
                       help='print only what would be done without actually doing it')
    return parser.parse_args()

def main():
    try:
        args = parse_cmdline_args()
    except Exception as e:
        log.error("Check command line arguments (%s)", e.strerror)

    # connect to the DB
    db_url = get_db_url(args.config)
    manila_session, manila_metadata, manila_Base = makeConnection(db_url)

    wrong_share_network_ssas = get_wrong_share_network_ssas(manila_metadata)
    if len(wrong_share_network_ssas) != 0:
        log.info("- share network security service association inconsistencies found")
        # print out what we would delete
        for share_network_ssa_id in wrong_share_network_ssas:
            log.info("-- share network security service association id: %s - deleted share network id: %s", share_network_ssa_id, wrong_share_network_ssas[share_network_ssa_id])
        if not args.dry_run:
            log.info("- deleting share network security service association inconsistencies found")
            fix_wrong_share_network_ssas(manila_metadata, wrong_share_network_ssas)
    else:
        log.info("- share network security service associations are consistent")


    wrong_network_allocations = get_wrong_network_allocations(manila_metadata)
    if len(wrong_network_allocations) != 0:
        log.info("- network allocation inconsistencies found")
        # print out what we would delete
        for network_allocation_id in wrong_network_allocations:
            log.info("-- network allocation id: %s - deleted share server id: %s", network_allocation_id, wrong_network_allocations[network_allocation_id])
        if not args.dry_run:
            log.info("- deleting network allocation inconsistencies found")
            fix_wrong_network_allocations(manila_metadata, wrong_network_allocations)
    else:
        log.info("- network allocations are consistent")


    wrong_share_metadata = get_wrong_share_metadata(manila_metadata)
    if len(wrong_share_metadata) != 0:
        log.info("- share metadata inconsistencies found")
        # print out what we would delete
        for share_metadata_id in wrong_share_metadata:
            log.info("-- share metadata id: %s - deleted share id: %s", share_metadata_id, wrong_share_metadata[share_metadata_id])
        if not args.dry_run:
            log.info("- deleting share metadata inconsistencies found")
            fix_wrong_share_metadata(manila_metadata, wrong_share_metadata)
    else:
        log.info("- share metadata is consistent")


    wrong_share_gtstm = get_wrong_share_gtstm(manila_metadata)
    if len(wrong_share_gtstm) != 0:
        log.info("- share group type share type mapping inconsistencies found")
        # print out what we would delete
        for share_gtstm_id in wrong_share_gtstm:
            log.info("-- share group type share type mapping id: %s - deleted share group type id: %s", share_gtstm_id, wrong_share_gtstm[share_gtstm_id])
        if not args.dry_run:
            log.info("- deleting share group type share type mapping inconsistencies found")
            fix_wrong_share_gtstm(manila_metadata, wrong_share_gtstm)
    else:
        log.info("- share group type share type mapping is consistent")


if __name__ == "__main__":
    main()
