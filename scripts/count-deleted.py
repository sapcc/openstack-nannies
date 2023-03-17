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
# this script counts the number of elements per table with the deleted flag set

import argparse
import sys
import ConfigParser
import logging

from prettytable import PrettyTable
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

# return the number of elements with the deleted flag set, i.e. not 0 ... -1 means no deleted flag
def get_deleted(meta, table_name):

    deleted_t = Table(table_name, meta, autoload=True)
    try:
        deleted_q = select(columns=[deleted_t.c.deleted])
        deleted_count = 0
        for i in deleted_q.execute():
            if i[0] != 0:
                deleted_count += 1
    except AttributeError:
        deleted_count = -1

    return deleted_count

# Establish a database connection and return the handle
def makeConnection(db_url):

    engine = create_engine(db_url)
    engine.connect()
    Session = sessionmaker(bind=engine)
    thisSession = Session()
    metadata = MetaData()
    metadata.bind = engine
    Base = declarative_base()
    # reflect db schema to MetaData
    metadata.reflect(bind=engine)
    return thisSession, metadata, Base

# return the database connection string from the config file
def get_db_url(config_file):

    parser = ConfigParser.ConfigParser()
    try:
        parser.read(config_file)
        db_url = parser.get('database', 'connection', raw=True)
    except:
        print "ERROR: Check configuration file."
        sys.exit(2)
    return db_url

# cmdline handling
def parse_cmdline_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",
                        default='./config.conf',
                        help='configuration file')
    return parser.parse_args()

def main():
    try:
        args = parse_cmdline_args()
    except Exception as e:
        log.error("Check command line arguments (%s)", e.strerror)

    # connect to the DB
    db_url = get_db_url(args.config)
    session, metadata, Base = makeConnection(db_url)

    # for all tables print the number of entries marked as deleted - -1 means: no deleted flag in that table
    ptable = PrettyTable(["table name", "# deleted"])
    for i in metadata.tables.keys():
        ptable.add_row([i,get_deleted(metadata,i)])
    print ptable

if __name__ == "__main__":
    main()
